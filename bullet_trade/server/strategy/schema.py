"""Versioned SQLite schema for StrategyLedger.

Each migration is a separate transaction. Production upgrades must back up the
database file before calling :func:`migrate_database`; rollback restores that
backup rather than attempting destructive down-migrations.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple, Union


DatabasePath = Union[str, Path]


class SchemaMigrationError(RuntimeError):
    """Raised when schema history is invalid or a migration cannot commit."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: Tuple[str, ...]


_ACCOUNT_STATES = "'ACTIVE','TRADING_BLOCKED','RECONCILIATION_BLOCKED','CLOSED'"
_INTENT_STATES = "'CREATED','PLANNED','EXECUTING','RECONCILING','COMPLETED','CANCELED','FAILED'"
_ORDER_STATES = (
    "'PENDING_SUBMIT','SUBMIT_UNKNOWN','SUBMITTED','PARTIALLY_FILLED',"
    "'FILLED','CANCELED','REJECTED'"
)


MIGRATIONS: Tuple[Migration, ...] = (
    Migration(
        1,
        "core_accounts_positions",
        (
            """
            CREATE TABLE physical_accounts (
                physical_account_id TEXT PRIMARY KEY,
                broker_kind TEXT NOT NULL,
                broker_account_ref TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('READY','BLOCKED','CLOSED')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE cash_pools (
                physical_account_id TEXT PRIMARY KEY
                    REFERENCES physical_accounts(physical_account_id),
                unallocated_cash_units INTEGER NOT NULL DEFAULT 0
                    CHECK (
                        typeof(unallocated_cash_units) = 'integer'
                        AND unallocated_cash_units >= 0
                    ),
                reserved_cash_units INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(reserved_cash_units) = 'integer' AND reserved_cash_units >= 0),
                version INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(version) = 'integer' AND version >= 0),
                CHECK (reserved_cash_units <= unallocated_cash_units)
            )
            """,
            """
            CREATE TABLE strategy_accounts (
                strategy_account_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL UNIQUE,
                physical_account_id TEXT NOT NULL REFERENCES physical_accounts(physical_account_id),
                initial_capital_units INTEGER NOT NULL
                    CHECK (
                        typeof(initial_capital_units) = 'integer'
                        AND initial_capital_units >= 0
                    ),
                cash_units INTEGER NOT NULL
                    CHECK (typeof(cash_units) = 'integer' AND cash_units >= 0),
                reserved_cash_units INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(reserved_cash_units) = 'integer' AND reserved_cash_units >= 0),
                ledger_version INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(ledger_version) = 'integer' AND ledger_version >= 0),
                event_seq INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(event_seq) = 'integer' AND event_seq >= 0),
                status TEXT NOT NULL CHECK (status IN (""" + _ACCOUNT_STATES + """)),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK (reserved_cash_units <= cash_units)
            )
            """,
            """
            CREATE TABLE ledger_entries (
                ledger_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                event_seq INTEGER NOT NULL
                    CHECK (typeof(event_seq) = 'integer' AND event_seq > 0),
                entry_type TEXT NOT NULL,
                amount_units INTEGER NOT NULL CHECK (typeof(amount_units) = 'integer'),
                cash_after_units INTEGER NOT NULL
                    CHECK (typeof(cash_after_units) = 'integer' AND cash_after_units >= 0),
                reserved_after_units INTEGER NOT NULL
                    CHECK (typeof(reserved_after_units) = 'integer' AND reserved_after_units >= 0),
                reference_type TEXT,
                reference_id TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                UNIQUE (strategy_account_id, event_seq),
                CHECK (reserved_after_units <= cash_after_units)
            )
            """,
            """
            CREATE TABLE positions (
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                security TEXT NOT NULL,
                total_qty INTEGER NOT NULL CHECK (typeof(total_qty) = 'integer' AND total_qty >= 0),
                sellable_qty INTEGER NOT NULL
                    CHECK (typeof(sellable_qty) = 'integer' AND sellable_qty >= 0),
                avg_cost_price_units INTEGER NOT NULL
                    CHECK (typeof(avg_cost_price_units) = 'integer' AND avg_cost_price_units >= 0),
                version INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(version) = 'integer' AND version >= 0),
                updated_at TEXT NOT NULL,
                PRIMARY KEY (strategy_account_id, security),
                CHECK (sellable_qty <= total_qty)
            )
            """,
            """
            CREATE TABLE position_lots (
                lot_id TEXT PRIMARY KEY,
                strategy_account_id TEXT NOT NULL,
                security TEXT NOT NULL,
                acquired_trade_date TEXT NOT NULL,
                sellable_from_trade_date TEXT NOT NULL,
                original_qty INTEGER NOT NULL
                    CHECK (typeof(original_qty) = 'integer' AND original_qty > 0),
                remaining_qty INTEGER NOT NULL
                    CHECK (typeof(remaining_qty) = 'integer' AND remaining_qty >= 0),
                cost_price_units INTEGER NOT NULL
                    CHECK (typeof(cost_price_units) = 'integer' AND cost_price_units >= 0),
                source_fill_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (strategy_account_id, security)
                    REFERENCES positions(strategy_account_id, security),
                CHECK (remaining_qty <= original_qty)
            )
            """,
            (
                "CREATE INDEX idx_ledger_entries_account_seq "
                "ON ledger_entries(strategy_account_id, event_seq)"
            ),
            (
                "CREATE INDEX idx_position_lots_account_security "
                "ON position_lots(strategy_account_id, security)"
            ),
        ),
    ),
    Migration(
        2,
        "execution_events_reconciliation",
        (
            """
            CREATE TABLE portfolio_intents (
                intent_id TEXT PRIMARY KEY,
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                idempotency_key TEXT NOT NULL,
                expected_ledger_version INTEGER NOT NULL
                    CHECK (
                        typeof(expected_ledger_version) = 'integer'
                        AND expected_ledger_version >= 0
                    ),
                state TEXT NOT NULL CHECK (state IN (""" + _INTENT_STATES + """)),
                targets_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (strategy_account_id, idempotency_key)
            )
            """,
            """
            CREATE TABLE strategy_orders (
                order_id TEXT PRIMARY KEY,
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                intent_id TEXT REFERENCES portfolio_intents(intent_id),
                client_tag TEXT NOT NULL UNIQUE,
                broker_order_id TEXT,
                security TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
                requested_qty INTEGER NOT NULL
                    CHECK (typeof(requested_qty) = 'integer' AND requested_qty > 0),
                filled_qty INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(filled_qty) = 'integer' AND filled_qty >= 0),
                limit_price_units INTEGER
                    CHECK (
                        limit_price_units IS NULL
                        OR (typeof(limit_price_units) = 'integer' AND limit_price_units > 0)
                    ),
                state TEXT NOT NULL CHECK (state IN (""" + _ORDER_STATES + """)),
                trading_day TEXT NOT NULL,
                submitted_at TEXT,
                terminal_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK (filled_qty <= requested_qty)
            )
            """,
            (
                "CREATE UNIQUE INDEX idx_strategy_orders_broker_id "
                "ON strategy_orders(broker_order_id) "
                "WHERE broker_order_id IS NOT NULL"
            ),
            """
            CREATE TABLE fills (
                fill_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL REFERENCES strategy_orders(order_id),
                broker_trade_id TEXT,
                fill_fingerprint TEXT NOT NULL UNIQUE,
                security TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
                quantity INTEGER NOT NULL CHECK (typeof(quantity) = 'integer' AND quantity > 0),
                price_units INTEGER NOT NULL
                    CHECK (typeof(price_units) = 'integer' AND price_units > 0),
                commission_units INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(commission_units) = 'integer' AND commission_units >= 0),
                tax_units INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(tax_units) = 'integer' AND tax_units >= 0),
                traded_at TEXT NOT NULL,
                booked_at TEXT NOT NULL
            )
            """,
            (
                "CREATE UNIQUE INDEX idx_fills_broker_trade_id "
                "ON fills(broker_trade_id) WHERE broker_trade_id IS NOT NULL"
            ),
            """
            CREATE TABLE strategy_events (
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                event_seq INTEGER NOT NULL CHECK (typeof(event_seq) = 'integer' AND event_seq > 0),
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                PRIMARY KEY (strategy_account_id, event_seq)
            )
            """,
            """
            CREATE TABLE outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                topic TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('PENDING','CLAIMED','DONE','FAILED')),
                attempt_count INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(attempt_count) = 'integer' AND attempt_count >= 0),
                available_at TEXT NOT NULL,
                lease_owner TEXT,
                lease_until TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX idx_outbox_ready ON outbox(state, available_at)",
            """
            CREATE TABLE reconciliation_runs (
                reconciliation_id TEXT PRIMARY KEY,
                physical_account_id TEXT NOT NULL REFERENCES physical_accounts(physical_account_id),
                state TEXT NOT NULL CHECK (state IN ('UNKNOWN','READY','BLOCKED')),
                broker_as_of TEXT,
                details_json TEXT NOT NULL DEFAULT '{}',
                started_at TEXT NOT NULL,
                completed_at TEXT
            )
            """,
            """
            CREATE TABLE capital_flows (
                capital_flow_id TEXT PRIMARY KEY,
                strategy_account_id TEXT NOT NULL REFERENCES strategy_accounts(strategy_account_id),
                flow_type TEXT NOT NULL CHECK (flow_type IN ('ALLOCATE','WITHDRAW','ADJUSTMENT')),
                amount_units INTEGER NOT NULL
                    CHECK (typeof(amount_units) = 'integer' AND amount_units > 0),
                external_ref TEXT UNIQUE,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE corporate_actions (
                corporate_action_id TEXT PRIMARY KEY,
                security TEXT NOT NULL,
                action_type TEXT NOT NULL,
                ex_date TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                applied_at TEXT
            )
            """,
        ),
    ),
    Migration(
        3,
        "append_only_guards",
        (
            """
            CREATE TRIGGER ledger_entries_no_update
            BEFORE UPDATE ON ledger_entries
            BEGIN
                SELECT RAISE(ABORT, 'ledger_entries is append-only');
            END
            """,
            """
            CREATE TRIGGER ledger_entries_no_delete
            BEFORE DELETE ON ledger_entries
            BEGIN
                SELECT RAISE(ABORT, 'ledger_entries is append-only');
            END
            """,
            """
            CREATE TRIGGER strategy_events_no_update
            BEFORE UPDATE ON strategy_events
            BEGIN
                SELECT RAISE(ABORT, 'strategy_events is append-only');
            END
            """,
            """
            CREATE TRIGGER strategy_events_no_delete
            BEFORE DELETE ON strategy_events
            BEGIN
                SELECT RAISE(ABORT, 'strategy_events is append-only');
            END
            """,
        ),
    ),
    Migration(
        4,
        "persistent_operations",
        (
            """
            CREATE TABLE strategy_operations (
                operation_id TEXT PRIMARY KEY,
                strategy_account_id TEXT NOT NULL
                    REFERENCES strategy_accounts(strategy_account_id),
                strategy_id TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                request_json TEXT NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN (
                        'PENDING','SUBMITTING','SUBMIT_UNKNOWN',
                        'COMPLETED','FAILED'
                    )
                ),
                client_tag TEXT NOT NULL UNIQUE,
                response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (strategy_id, endpoint, idempotency_key)
            )
            """,
            (
                "ALTER TABLE outbox ADD COLUMN operation_id TEXT "
                "REFERENCES strategy_operations(operation_id)"
            ),
            (
                "CREATE UNIQUE INDEX idx_outbox_operation_id "
                "ON outbox(operation_id) WHERE operation_id IS NOT NULL"
            ),
        ),
    ),
    Migration(
        5,
        "broker_history_cache",
        (
            """
            CREATE TABLE broker_order_history (
                account_key TEXT NOT NULL,
                broker_order_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (account_key, broker_order_id)
            )
            """,
            """
            CREATE TABLE broker_trade_history (
                account_key TEXT NOT NULL,
                broker_trade_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (account_key, broker_trade_id)
            )
            """,
            (
                "CREATE INDEX idx_broker_order_history_last_seen "
                "ON broker_order_history(account_key, last_seen_at)"
            ),
            (
                "CREATE INDEX idx_broker_trade_history_last_seen "
                "ON broker_trade_history(account_key, last_seen_at)"
            ),
        ),
    ),
    Migration(
        6,
        "explicit_unknown_fill_fees",
        (
            (
                "ALTER TABLE fills ADD COLUMN commission_known INTEGER NOT NULL "
                "DEFAULT 1 CHECK (commission_known IN (0, 1))"
            ),
            (
                "ALTER TABLE fills ADD COLUMN tax_known INTEGER NOT NULL "
                "DEFAULT 1 CHECK (tax_known IN (0, 1))"
            ),
        ),
    ),
    Migration(
        7,
        "explicit_estimated_fill_prices",
        (
            (
                "ALTER TABLE fills ADD COLUMN price_source TEXT NOT NULL "
                "DEFAULT 'BROKER_TRADE' CHECK (price_source IN "
                "('BROKER_TRADE','ORDER_PRICE_FALLBACK'))"
            ),
            (
                "ALTER TABLE fills ADD COLUMN price_known INTEGER NOT NULL "
                "DEFAULT 1 CHECK (price_known IN (0, 1))"
            ),
        ),
    ),
    Migration(
        8,
        "dashboard_performance_snapshots",
        (
            """
            CREATE TABLE dashboard_snapshots (
                strategy_account_id TEXT NOT NULL
                    REFERENCES strategy_accounts(strategy_account_id),
                snapshot_minute TEXT NOT NULL,
                as_of TEXT NOT NULL,
                total_value_units INTEGER NOT NULL
                    CHECK (typeof(total_value_units) = 'integer' AND total_value_units >= 0),
                cash_units INTEGER NOT NULL
                    CHECK (typeof(cash_units) = 'integer' AND cash_units >= 0),
                positions_value_units INTEGER NOT NULL
                    CHECK (typeof(positions_value_units) = 'integer' AND positions_value_units >= 0),
                total_pnl_units INTEGER
                    CHECK (total_pnl_units IS NULL OR typeof(total_pnl_units) = 'integer'),
                nav_units INTEGER
                    CHECK (nav_units IS NULL OR typeof(nav_units) = 'integer'),
                fees_units INTEGER
                    CHECK (fees_units IS NULL OR typeof(fees_units) = 'integer'),
                performance_ready INTEGER NOT NULL
                    CHECK (performance_ready IN (0, 1)),
                PRIMARY KEY (strategy_account_id, snapshot_minute)
            )
            """,
            (
                "CREATE INDEX idx_dashboard_snapshots_account_as_of "
                "ON dashboard_snapshots(strategy_account_id, as_of)"
            ),
        ),
    ),
    Migration(
        9,
        "broker_identifiers_are_trading_day_scoped",
        (
            "DROP INDEX idx_strategy_orders_broker_id",
            (
                "CREATE INDEX idx_strategy_orders_broker_id_day "
                "ON strategy_orders(trading_day, broker_order_id) "
                "WHERE broker_order_id IS NOT NULL"
            ),
            "DROP INDEX idx_fills_broker_trade_id",
            (
                "CREATE INDEX idx_fills_broker_trade_id_day "
                "ON fills(substr(traded_at, 1, 10), broker_trade_id) "
                "WHERE broker_trade_id IS NOT NULL"
            ),
            "ALTER TABLE reconciliation_runs ADD COLUMN strategy_account_id TEXT",
            (
                "CREATE INDEX idx_reconciliation_strategy_started "
                "ON reconciliation_runs(strategy_account_id, started_at)"
            ),
            "DROP INDEX idx_broker_order_history_last_seen",
            "ALTER TABLE broker_order_history RENAME TO broker_order_history_v5",
            """
            CREATE TABLE broker_order_history (
                account_key TEXT NOT NULL,
                trading_day TEXT NOT NULL,
                broker_order_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (account_key, trading_day, broker_order_id)
            )
            """,
            """
            INSERT INTO broker_order_history(
                account_key, trading_day, broker_order_id, payload_json,
                first_seen_at, last_seen_at
            )
            SELECT account_key, substr(first_seen_at, 1, 10), broker_order_id,
                   payload_json, first_seen_at, last_seen_at
            FROM broker_order_history_v5
            """,
            "DROP TABLE broker_order_history_v5",
            (
                "CREATE INDEX idx_broker_order_history_last_seen "
                "ON broker_order_history(account_key, last_seen_at)"
            ),
            "DROP INDEX idx_broker_trade_history_last_seen",
            "ALTER TABLE broker_trade_history RENAME TO broker_trade_history_v5",
            """
            CREATE TABLE broker_trade_history (
                account_key TEXT NOT NULL,
                trading_day TEXT NOT NULL,
                broker_trade_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (account_key, trading_day, broker_trade_id)
            )
            """,
            """
            INSERT INTO broker_trade_history(
                account_key, trading_day, broker_trade_id, payload_json,
                first_seen_at, last_seen_at
            )
            SELECT account_key, substr(first_seen_at, 1, 10), broker_trade_id,
                   payload_json, first_seen_at, last_seen_at
            FROM broker_trade_history_v5
            """,
            "DROP TABLE broker_trade_history_v5",
            (
                "CREATE INDEX idx_broker_trade_history_last_seen "
                "ON broker_trade_history(account_key, last_seen_at)"
            ),
        ),
    ),
    # No table has a foreign key referencing fills; preserve every existing row.
    Migration(
        10,
        "explicit_zero_fallback_fill_prices",
        (
            """
            CREATE TABLE fills_v10 (
                fill_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL REFERENCES strategy_orders(order_id),
                broker_trade_id TEXT,
                fill_fingerprint TEXT NOT NULL UNIQUE,
                security TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
                quantity INTEGER NOT NULL CHECK (typeof(quantity) = 'integer' AND quantity > 0),
                price_units INTEGER NOT NULL CHECK (typeof(price_units) = 'integer'),
                commission_units INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(commission_units) = 'integer' AND commission_units >= 0),
                tax_units INTEGER NOT NULL DEFAULT 0
                    CHECK (typeof(tax_units) = 'integer' AND tax_units >= 0),
                traded_at TEXT NOT NULL,
                booked_at TEXT NOT NULL,
                commission_known INTEGER NOT NULL DEFAULT 1 CHECK (commission_known IN (0,1)),
                tax_known INTEGER NOT NULL DEFAULT 1 CHECK (tax_known IN (0,1)),
                price_source TEXT NOT NULL DEFAULT 'BROKER_TRADE'
                    CHECK (price_source IN ('BROKER_TRADE','ORDER_PRICE_FALLBACK','ZERO_FALLBACK')),
                price_known INTEGER NOT NULL DEFAULT 1 CHECK (price_known IN (0,1)),
                CHECK (
                    (price_source = 'ZERO_FALLBACK' AND price_units = 0 AND price_known = 0)
                    OR (price_source = 'ORDER_PRICE_FALLBACK' AND price_units > 0 AND price_known = 0)
                    OR (price_source = 'BROKER_TRADE' AND price_units > 0 AND price_known = 1)
                )
            )
            """,
            """
            INSERT INTO fills_v10 (
                fill_id, order_id, broker_trade_id, fill_fingerprint, security, side,
                quantity, price_units, commission_units, tax_units, traded_at, booked_at,
                commission_known, tax_known, price_source, price_known
            )
            SELECT fill_id, order_id, broker_trade_id, fill_fingerprint, security, side,
                   quantity, price_units, commission_units, tax_units, traded_at, booked_at,
                   commission_known, tax_known, price_source, price_known FROM fills
            """,
            "DROP TABLE fills",
            "ALTER TABLE fills_v10 RENAME TO fills",
            (
                "CREATE INDEX idx_fills_broker_trade_id_day "
                "ON fills(substr(traded_at, 1, 10), broker_trade_id) "
                "WHERE broker_trade_id IS NOT NULL"
            ),
        ),
    ),
    Migration(
        11,
        "same_day_broker_order_observations",
        (
            "DROP INDEX idx_broker_order_history_last_seen",
            "ALTER TABLE broker_order_history RENAME TO broker_order_history_v9",
            """
            CREATE TABLE broker_order_history (
                account_key TEXT NOT NULL,
                trading_day TEXT NOT NULL,
                broker_order_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                observation_no INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (account_key, trading_day, broker_order_id, observation_no)
            )
            """,
            """
            INSERT INTO broker_order_history (
                account_key, trading_day, broker_order_id, payload_json, first_seen_at, last_seen_at
            )
            SELECT account_key, trading_day, broker_order_id, payload_json, first_seen_at, last_seen_at
            FROM broker_order_history_v9
            """,
            "DROP TABLE broker_order_history_v9",
            "CREATE INDEX idx_broker_order_history_last_seen ON broker_order_history(account_key,last_seen_at)",
        ),
    ),
)

LATEST_SCHEMA_VERSION = MIGRATIONS[-1].version


def migration_checksum(migration: Migration) -> str:
    """Return a stable checksum; applied migration text must never be edited."""

    digest = hashlib.sha256()
    for value in (str(migration.version), migration.name) + migration.statements:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, byteorder="big"))
        digest.update(encoded)
    return digest.hexdigest()


def connect_database(path: DatabasePath) -> sqlite3.Connection:
    """Open a configured connection; callers own and must close it."""

    database = str(path)
    connection = sqlite3.connect(database, timeout=5.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    if database != ":memory:":
        connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


def _bootstrap_history(connection: sqlite3.Connection) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY CHECK (typeof(version) = 'integer' AND version > 0),
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def apply_migrations(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration] = MIGRATIONS,
    target_version: Optional[int] = None,
) -> int:
    """Apply ordered forward migrations and return the resulting version."""

    _bootstrap_history(connection)
    ordered = tuple(migrations)
    versions = tuple(migration.version for migration in ordered)
    if versions != tuple(range(1, len(ordered) + 1)):
        raise SchemaMigrationError("migrations must be consecutive and start at version 1")
    maximum = versions[-1] if versions else 0
    target = maximum if target_version is None else target_version
    if type(target) is not int or target < 0 or target > maximum:
        raise SchemaMigrationError("target schema version is invalid")

    rows = connection.execute(
        "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
    ).fetchall()
    applied = tuple(int(row["version"]) for row in rows)
    if applied != tuple(range(1, len(applied) + 1)):
        raise SchemaMigrationError("schema migration history is not consecutive")
    if applied and applied[-1] > maximum:
        raise SchemaMigrationError("database schema is newer than this application")
    for row in rows:
        expected = ordered[int(row["version"]) - 1]
        if row["name"] != expected.name:
            raise SchemaMigrationError("schema migration history does not match application")
        if row["checksum"] != migration_checksum(expected):
            raise SchemaMigrationError("schema migration checksum does not match application")

    current = applied[-1] if applied else 0
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if user_version != current:
        raise SchemaMigrationError("PRAGMA user_version does not match migration history")
    if current > target:
        raise SchemaMigrationError("down-migration is not supported; restore a backup")

    for migration in ordered[current:target]:
        connection.execute("BEGIN IMMEDIATE")
        try:
            for statement in migration.statements:
                connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations(version, name, checksum) VALUES (?, ?, ?)",
                (migration.version, migration.name, migration_checksum(migration)),
            )
            connection.execute("PRAGMA user_version = {}".format(migration.version))
            connection.commit()
        except BaseException as exc:
            connection.rollback()
            raise SchemaMigrationError(
                "failed to apply schema migration {}".format(migration.version)
            ) from exc
        current = migration.version
    return current


def migrate_database(
    path: DatabasePath,
    target_version: Optional[int] = None,
) -> int:
    """Open, migrate and close a database."""

    connection = connect_database(path)
    try:
        return apply_migrations(connection, target_version=target_version)
    finally:
        connection.close()
