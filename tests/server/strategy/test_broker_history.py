import json
from datetime import datetime

from bullet_trade.server.strategy.broker_history import (
    SQLiteBrokerHistoryStore,
    merge_broker_rows,
)
from bullet_trade.server.strategy.domain import SHANGHAI_TZ
from bullet_trade.server.strategy.schema import connect_database, migrate_database


def test_broker_history_survives_reopen_and_updates_order_state(tmp_path):
    database = tmp_path / "ledger.db"
    first = SQLiteBrokerHistoryStore(database)
    observed = datetime(2026, 8, 24, 9, 35, tzinfo=SHANGHAI_TZ)
    assert first.record_order(
        "default",
        {
            "order_id": "O-1",
            "security": "510300.XSHG",
            "status": "open",
            "order_remark": "good_etf|one",
        },
        observed,
    )
    assert first.record_order(
        "default", {"order_id": "O-1", "status": "filled"}, observed
    )

    reopened = SQLiteBrokerHistoryStore(database)
    rows = reopened.list_orders("default")
    assert len(rows) == 1
    assert rows[0]["security"] == "510300.XSHG"
    assert rows[0]["status"] == "filled"


def test_same_day_reused_order_id_never_mix_fields_and_survive_restart(tmp_path):
    database = tmp_path / "ledger.db"
    store = SQLiteBrokerHistoryStore(database)
    first = {"order_id": "REUSED", "security": "159031.XSHE", "side": "SELL",
             "order_remark": "bt:old", "order_time": "2026-09-07 09:30:00",
             "status": "filled", "price": 1.05, "filled": 1000, "amount": 1000}
    second = {"order_id": "REUSED", "security": "159322.XSHE", "side": "BUY",
              "order_remark": "bt:new", "order_time": "2026-09-07 21:26:00",
              "status": "rejected", "price": 0, "filled": 0, "amount": 1000}
    for row in (first, second, first, second):
        store.record_order("default", row)
    history = SQLiteBrokerHistoryStore(database).list_orders("default")
    assert len(history) == 2
    rows = {row["order_remark"]: row for row in history}
    for original in (first, second):
        assert all(rows[original["order_remark"]][k] == v for k, v in original.items())
    merged = merge_broker_rows([first], history, "order_id")
    assert len(merged) == 2
    assert next(row for row in merged if row["order_remark"] == "bt:new")["price"] == 0
    assert next(row for row in merged if row["order_remark"] == "bt:new")["_broker_history_only"] is True
    # A partial callback without identifying fields cannot update either order.
    partial = {"order_id": "REUSED", "status": "canceled", "_broker_trading_day": "2026-09-07"}
    for _ in range(2):
        store.record_order("default", partial)
    assert len(store.list_orders("default")) == 3
    for row in store.list_orders("default"):
        if row.get("order_remark") == "bt:old":
            assert row["status"] == "filled"
        if row.get("order_remark") == "bt:new":
            assert row["status"] == "rejected" and row["price"] == 0


def test_same_symbol_same_side_reused_id_keeps_distinct_tags(tmp_path):
    store = SQLiteBrokerHistoryStore(tmp_path / "ledger.db")
    base = {"order_id": "REUSED", "security": "510050.XSHG", "side": "BUY",
            "amount": 100, "_broker_trading_day": "2026-09-07"}
    old = dict(base, order_remark="bt:old", status="filled", price=2.5)
    new = dict(base, order_remark="bt:new", status="open", price=0)
    for row in (old, new):
        store.record_order("default", row)
    merged = merge_broker_rows([dict(new, status="canceled")], store.list_orders("default"), "order_id")
    assert len(merged) == 2
    assert {r["order_remark"]: (r["status"], r["price"]) for r in merged} == {
        "bt:old": ("filled", 2.5), "bt:new": ("canceled", 0),
    }


def test_trade_history_preserves_known_fee_when_later_callback_omits_it(tmp_path):
    store = SQLiteBrokerHistoryStore(tmp_path / "ledger.db")
    store.record_trade(
        "default",
        {
            "trade_id": "T-1",
            "order_id": "O-1",
            "commission_known": True,
            "commission_fee": 1.25,
            "tax_known": False,
            "tax": 0.0,
        },
    )
    store.record_trade(
        "default",
        {
            "trade_id": "T-1",
            "order_id": "O-1",
            "commission_known": False,
            "commission_fee": 0.0,
            "tax_known": False,
            "tax": 0.0,
        },
    )

    trade = store.list_trades("default")[0]
    assert trade["commission_known"] is True
    assert trade["commission_fee"] == 1.25
    assert trade["tax_known"] is False


def test_trade_history_does_not_replace_valid_link_with_zero(tmp_path):
    store = SQLiteBrokerHistoryStore(tmp_path / "ledger.db")
    store.record_trade(
        "default",
        {
            "trade_id": "T-1",
            "order_id": "O-1",
            "order_sysid": "SYS-1",
        },
    )
    store.record_trade(
        "default",
        {
            "trade_id": "T-1",
            "order_id": 0,
            "order_sysid": "0",
        },
    )

    trade = store.list_trades("default")[0]
    assert trade["order_id"] == "O-1"
    assert trade["order_sysid"] == "SYS-1"


def test_trade_history_does_not_replace_valid_prices_with_zero(tmp_path):
    store = SQLiteBrokerHistoryStore(tmp_path / "ledger.db")
    store.record_trade(
        "default",
        {
            "trade_id": "T-1",
            "order_id": "O-1",
            "price": 2.5,
            "traded_price": 2.5,
            "deal_balance": 250.0,
        },
    )
    store.record_trade(
        "default",
        {
            "trade_id": "T-1",
            "order_id": "O-1",
            "price": 0,
            "traded_price": 0.0,
            "deal_balance": 0,
        },
    )

    trade = store.list_trades("default")[0]
    assert trade["price"] == 2.5
    assert trade["traded_price"] == 2.5
    assert trade["deal_balance"] == 250.0


def test_history_filters_and_current_rows_win_merge(tmp_path):
    store = SQLiteBrokerHistoryStore(tmp_path / "ledger.db")
    store.record_order(
        "default",
        {"order_id": "O-1", "security": "510300.XSHG", "status": "open"},
    )
    store.record_order(
        "default",
        {"order_id": "O-2", "security": "159915.XSHE", "status": "filled"},
    )

    assert [row["order_id"] for row in store.list_orders("default", status="open")] == [
        "O-1"
    ]
    merged = merge_broker_rows(
        [{"order_id": "O-1", "status": "filled"}],
        store.list_orders("default"),
        "order_id",
    )
    by_id = {row["order_id"]: row for row in merged}
    assert by_id["O-1"]["status"] == "filled"
    assert by_id["O-1"]["_broker_history_only"] is False
    assert by_id["O-2"]["status"] == "filled"
    assert by_id["O-2"]["_broker_history_only"] is True


def test_reused_qmt_ids_are_kept_separately_by_trading_day(tmp_path):
    store = SQLiteBrokerHistoryStore(tmp_path / "ledger.db")
    store.record_order(
        "default",
        {
            "order_id": "REUSED",
            "security": "510050.XSHG",
            "order_time": "2026-08-27 09:30:00",
        },
    )
    store.record_order(
        "default",
        {
            "order_id": "REUSED",
            "security": "560490.XSHG",
            "order_time": "2026-09-03 09:30:00",
        },
    )
    store.record_trade(
        "default",
        {
            "trade_id": "REUSED-TRADE",
            "order_id": "REUSED",
            "security": "510050.XSHG",
            "time": "2026-08-27 09:30:01",
        },
    )
    store.record_trade(
        "default",
        {
            "trade_id": "REUSED-TRADE",
            "order_id": "REUSED",
            "security": "560490.XSHG",
            "time": "2026-09-03 09:30:01",
        },
    )

    orders = store.list_orders("default")
    trades = store.list_trades("default")

    assert [(row["_broker_trading_day"], row["security"]) for row in orders] == [
        ("2026-08-27", "510050.XSHG"),
        ("2026-09-03", "560490.XSHG"),
    ]
    assert [(row["_broker_trading_day"], row["security"]) for row in trades] == [
        ("2026-08-27", "510050.XSHG"),
        ("2026-09-03", "560490.XSHG"),
    ]


def test_migration_hides_v5_row_overwritten_by_a_later_day(tmp_path):
    database = tmp_path / "ledger.db"
    migrate_database(database, target_version=8)
    connection = connect_database(database)
    try:
        connection.execute(
            """
            INSERT INTO broker_order_history(
                account_key, broker_order_id, payload_json,
                first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "default",
                "REUSED",
                json.dumps(
                    {
                        "order_id": "REUSED",
                        "security": "560490.XSHG",
                        "order_time": "2026-09-03 09:30:00",
                    }
                ),
                "2026-08-27T09:30:00+08:00",
                "2026-09-03T09:30:00+08:00",
            ),
        )
        connection.commit()
    finally:
        connection.close()
    migrate_database(database)

    store = SQLiteBrokerHistoryStore(database)

    assert store.list_orders("default") == ()
