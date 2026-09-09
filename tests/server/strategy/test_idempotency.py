import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from bullet_trade.server.strategy.idempotency import (
    IdempotencyConflictError,
    OperationState,
    SQLiteOperationRepository,
)
from bullet_trade.server.strategy.repository import (
    RepositoryError,
    SQLiteStrategyRepository,
)
from bullet_trade.server.strategy.schema import connect_database


@pytest.fixture
def operation_repository(tmp_path):
    database_path = tmp_path / "operations.db"
    ledger = SQLiteStrategyRepository(database_path)
    ledger.initialize()
    ledger.create_physical_account(
        "qmt-main",
        "QMT",
        "account-1",
        unallocated_cash_units=500_000_000,
    )
    ledger.create_strategy_account(
        "good-etf",
        "good_etf",
        "qmt-main",
        100_000_000,
    )
    return SQLiteOperationRepository(database_path)


def _create(repository, key="rebalance-1", amount=100):
    return repository.create_operation(
        "good-etf",
        "portfolio.submit",
        key,
        {"targets": {"510300.XSHG": amount}},
    )


def _counts(repository):
    connection = connect_database(repository.database_path)
    try:
        operations = connection.execute(
            "SELECT COUNT(*) FROM strategy_operations"
        ).fetchone()[0]
        outbox = connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        return operations, outbox
    finally:
        connection.close()


def test_same_key_and_payload_replays_one_operation_and_outbox(operation_repository):
    first = _create(operation_repository)
    second = _create(operation_repository)

    assert first.replayed is False
    assert second.replayed is True
    assert second.operation == first.operation
    assert len(first.operation.client_tag) <= 24
    assert _counts(operation_repository) == (1, 1)


def test_sellable_filter_keeps_blocked_order_pending_and_claims_other_security(operation_repository):
    repository = operation_repository
    first = repository.create_operation("good-etf", "broker.submit", "sell-a", {
        "side": "SELL", "security": "510050.XSHG", "amount": 500,
    })
    second = repository.create_operation("good-etf", "broker.submit", "sell-b", {
        "side": "SELL", "security": "510300.XSHG", "amount": 200,
    })
    claim = repository.claim_next("good-etf", sellable_limits={"510300.XSHG": 200})
    assert claim.operation_id == second.operation.operation_id
    assert repository.claim_next("good-etf", sellable_limits={"510050.XSHG": 499}) is None
    db = connect_database(repository.database_path)
    try:
        row = db.execute("SELECT state, attempt_count FROM outbox WHERE operation_id=?",
                         (first.operation.operation_id,)).fetchone()
        assert tuple(row) == ("PENDING", 0)
    finally:
        db.close()
    recovered = repository.claim_next("good-etf", sellable_limits={"510050.XSHG": 500})
    assert recovered.operation_id == first.operation.operation_id


def test_same_key_with_different_payload_conflicts(operation_repository):
    _create(operation_repository, amount=100)
    with pytest.raises(IdempotencyConflictError, match="different payload"):
        _create(operation_repository, amount=200)
    assert _counts(operation_repository) == (1, 1)


def test_concurrent_same_key_creates_one_operation_and_outbox(operation_repository):
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(lambda _: _create(operation_repository), range(100)))

    assert sum(not result.replayed for result in results) == 1
    assert len({result.operation.operation_id for result in results}) == 1
    assert _counts(operation_repository) == (1, 1)


def test_operation_and_outbox_insert_are_one_transaction(operation_repository):
    connection = connect_database(operation_repository.database_path)
    try:
        connection.execute(
            """
            CREATE TRIGGER test_abort_outbox_insert
            BEFORE INSERT ON outbox
            BEGIN
                SELECT RAISE(ABORT, 'injected outbox failure');
            END
            """
        )
    finally:
        connection.close()

    with pytest.raises(RepositoryError, match="create idempotent operation"):
        _create(operation_repository)
    assert _counts(operation_repository) == (0, 0)


def test_operation_hash_and_outbox_share_one_request_snapshot(operation_repository):
    class MutatingPayload(dict):
        def __init__(self):
            super().__init__({"targets": {"510300.XSHG": 100}})
            self.read_count = 0

        def items(self):
            snapshot = list(super().items())
            self.read_count += 1
            if self.read_count == 1:
                self["targets"] = {"510300.XSHG": 200}
            return snapshot

    payload = MutatingPayload()
    created = operation_repository.create_operation(
        "good-etf",
        "portfolio.submit",
        "snapshot-1",
        payload,
    )

    connection = connect_database(operation_repository.database_path)
    try:
        request_json = connection.execute(
            "SELECT request_json FROM strategy_operations WHERE operation_id = ?",
            (created.operation.operation_id,),
        ).fetchone()[0]
        outbox_json = connection.execute(
            "SELECT payload_json FROM outbox WHERE operation_id = ?",
            (created.operation.operation_id,),
        ).fetchone()[0]
    finally:
        connection.close()
    assert payload.read_count == 1
    assert json.loads(request_json)["targets"]["510300.XSHG"] == 100
    assert json.loads(outbox_json)["payload"] == json.loads(request_json)


def test_claim_begin_and_finish_persist_response_for_replay(operation_repository):
    created = _create(operation_repository)
    claim = operation_repository.claim_next()
    assert claim is not None
    payload = json.loads(claim.payload_json)
    assert payload["operation_id"] == created.operation.operation_id
    assert payload["client_tag"] == created.operation.client_tag
    assert claim.attempt_count == 1

    submitting = operation_repository.begin_submission(claim.outbox_id)
    assert submitting.state is OperationState.SUBMITTING
    completed = operation_repository.finish_submission(
        claim.outbox_id,
        {"order_id": "broker-order-1", "status": "submitted"},
    )
    assert completed.state is OperationState.COMPLETED
    assert json.loads(completed.response_json) == {
        "order_id": "broker-order-1",
        "status": "submitted",
    }
    replay = _create(operation_repository)
    assert replay.replayed is True
    assert replay.operation == completed
    assert operation_repository.claim_next() is None


def test_concurrent_claims_have_a_single_winner(operation_repository):
    _create(operation_repository)
    barrier = threading.Barrier(2)

    def claim(_):
        barrier.wait()
        return operation_repository.claim_next()

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, range(2)))

    assert sum(item is not None for item in claims) == 1


def test_claim_can_be_scoped_to_one_strategy_account(operation_repository):
    ledger = SQLiteStrategyRepository(operation_repository.database_path)
    ledger.create_strategy_account(
        "other-strategy",
        "other_strategy",
        "qmt-main",
        100_000_000,
    )
    first = _create(operation_repository, key="first")
    other = operation_repository.create_operation(
        "other-strategy",
        "portfolio.submit",
        "second",
        {"targets": {"510300.XSHG": 200}},
    )

    claimed = operation_repository.claim_next("other-strategy")

    assert claimed is not None
    assert claimed.operation_id == other.operation.operation_id
    remaining = operation_repository.claim_next("good-etf")
    assert remaining is not None
    assert remaining.operation_id == first.operation.operation_id


def test_unknown_submission_is_not_requeued(operation_repository):
    _create(operation_repository)
    claim = operation_repository.claim_next()
    operation_repository.begin_submission(claim.outbox_id)
    unknown = operation_repository.finish_submission(
        claim.outbox_id,
        {"error": "response lost"},
        unknown=True,
    )

    assert unknown.state is OperationState.SUBMIT_UNKNOWN
    assert operation_repository.claim_next() is None
    assert _create(operation_repository).operation.state is OperationState.SUBMIT_UNKNOWN


def test_restart_quarantines_submission_that_crossed_effect_boundary(operation_repository):
    created = _create(operation_repository)
    claim = operation_repository.claim_next()
    operation_repository.begin_submission(claim.outbox_id)

    assert operation_repository.quarantine_inflight() == 1
    recovered = operation_repository.get_operation(created.operation.operation_id)
    assert recovered.state is OperationState.SUBMIT_UNKNOWN
    assert operation_repository.claim_next() is None


def test_restart_resets_unsubmitted_claim_to_pending(operation_repository):
    created = _create(operation_repository)
    first = operation_repository.claim_next()
    assert first is not None
    with pytest.raises(RepositoryError, match="not claimed"):
        operation_repository.begin_submission(first.outbox_id + 999)

    # Simulated restart between claim and begin_submission: the message must
    # not get stuck in CLAIMED, it returns to PENDING and can be claimed again.
    assert operation_repository.quarantine_inflight() == 1
    recovered = operation_repository.get_operation(created.operation.operation_id)
    assert recovered.state is OperationState.PENDING
    second = operation_repository.claim_next()
    assert second is not None
    assert second.outbox_id == first.outbox_id
    assert second.attempt_count == 2
