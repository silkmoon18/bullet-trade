"""Small durable cache for broker order and trade observations."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Optional, Tuple, Union, cast

from .domain import SHANGHAI_TZ, as_shanghai_time
from .schema import connect_database, migrate_database


DatabasePath = Union[str, Path]


class BrokerHistoryError(RuntimeError):
    """Raised when a broker observation cannot be persisted or read."""


@dataclass(frozen=True)
class BrokerHistorySnapshot:
    orders: Tuple[Mapping[str, object], ...]
    trades: Tuple[Mapping[str, object], ...]


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    enum_value = getattr(value, "value", None)
    if enum_value is not None:
        return str(enum_value)
    return str(value)


def _payload_json(payload: Mapping[str, object]) -> str:
    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def broker_trading_day(
    payload: Mapping[str, object],
    kind: str,
    fallback: Optional[datetime] = None,
) -> Optional[str]:
    """Return the exchange trading day used to scope reusable QMT ids."""

    explicit = str(payload.get("_broker_trading_day") or "").strip()
    if explicit:
        try:
            return datetime.strptime(explicit[:10], "%Y-%m-%d").date().isoformat()
        except ValueError:
            pass
    keys = (
        ("time", "trade_time", "traded_time")
        if kind == "trade"
        else ("order_time", "time", "add_time")
    )
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        parsed = _event_datetime(value)
        if parsed is not None:
            return as_shanghai_time(parsed).date().isoformat()
    if fallback is None:
        return None
    return as_shanghai_time(fallback).date().isoformat()


def _event_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif type(value) in (int, float):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000
        if number < 946_684_800:
            return None
        try:
            parsed = datetime.fromtimestamp(number, tz=SHANGHAI_TZ)
        except (OSError, OverflowError, ValueError):
            return None
    elif type(value) is str:
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            number = int(text)
            if len(text) in (13, 16):
                number //= 1000 if len(text) == 13 else 1_000_000
            if number >= 946_684_800 and len(str(number)) == 10:
                try:
                    return datetime.fromtimestamp(number, tz=SHANGHAI_TZ)
                except (OSError, OverflowError, ValueError):
                    return None
        parsed = None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            for pattern in (
                "%Y%m%d%H%M%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y%m%d",
                "%Y-%m-%d",
            ):
                try:
                    parsed = datetime.strptime(text, pattern)
                    break
                except ValueError:
                    continue
        if parsed is None:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed


def _merge_payload(
    previous: Mapping[str, object], current: Mapping[str, object]
) -> Mapping[str, object]:
    """Keep later state while retaining useful fields absent from a callback."""

    merged = dict(previous)
    for key, value in current.items():
        if (
            key in ("order_id", "trade_id", "order_sysid")
            and str(value or "").strip() in ("", "0")
            and str(previous.get(key) or "").strip() not in ("", "0")
        ):
            continue
        if key in ("price", "traded_price", "deal_balance"):
            try:
                previous_number = float(previous.get(key) or 0.0)
            except (TypeError, ValueError):
                previous_number = 0.0
            try:
                current_number = float(value) if value not in (None, "") else 0.0
            except (TypeError, ValueError):
                current_number = 0.0
            if previous_number > 0 and current_number <= 0:
                continue
        if value not in (None, "") or key not in merged:
            merged[key] = value
    for known_key, value_keys in (
        ("commission_known", ("commission", "commission_fee")),
        ("tax_known", ("tax", "stamp_tax")),
    ):
        if previous.get(known_key) is True and current.get(known_key) is not True:
            merged[known_key] = True
            for key in value_keys:
                if key in previous:
                    merged[key] = previous[key]
    return merged


def _order_identity(row: Mapping[str, object]) -> dict:
    """Only identity fields, not changing fill quantities, state or prices."""
    side = str(row.get("side") or row.get("order_side") or "").upper()
    if not side and type(row.get("is_buy")) is bool:
        side = "BUY" if row["is_buy"] else "SELL"
    timestamp = _event_datetime(row.get("order_time") or row.get("time"))
    values = {
        "security": row.get("security"),
        "side": side,
        "quantity": row.get("amount") or row.get("quantity"),
        "remark": row.get("order_remark") or row.get("remark"),
        "sysid": row.get("order_sysid") or row.get("sysid"),
        "time": as_shanghai_time(timestamp).isoformat() if timestamp else None,
    }
    return {
        key: str(value).strip()
        for key, value in values.items() if value not in (None, "", 0, "0")
    }


def _matching_order_observation(rows, current):
    """Return one compatible observation, never pick between conflicting orders."""
    identity = _order_identity(current)
    candidates = [index for index, row in enumerate(rows) if all(
        identity[key] == value for key, value in _order_identity(row).items() if key in identity
    )]
    if len(candidates) == 1:
        return candidates[0]
    exact = [index for index in candidates if _order_identity(rows[index]) == identity]
    return exact[0] if len(exact) == 1 else None


class SQLiteBrokerHistoryStore:
    """Persist every normalized QMT observation that reached this server."""

    def __init__(self, database_path: DatabasePath) -> None:
        self.database_path = Path(database_path)
        migrate_database(self.database_path)

    def record_order(
        self,
        account_key: str,
        order: Mapping[str, object],
        observed_at: Optional[datetime] = None,
    ) -> bool:
        return self._record(
            "broker_order_history",
            "broker_order_id",
            "order_id",
            account_key,
            order,
            observed_at,
        )

    def record_trade(
        self,
        account_key: str,
        trade: Mapping[str, object],
        observed_at: Optional[datetime] = None,
    ) -> bool:
        return self._record(
            "broker_trade_history",
            "broker_trade_id",
            "trade_id",
            account_key,
            trade,
            observed_at,
        )

    def record_orders(
        self, account_key: str, orders: Iterable[Mapping[str, object]]
    ) -> int:
        return sum(self.record_order(account_key, row) for row in orders)

    def record_trades(
        self, account_key: str, trades: Iterable[Mapping[str, object]]
    ) -> int:
        return sum(self.record_trade(account_key, row) for row in trades)

    def snapshot(self, account_key: str) -> BrokerHistorySnapshot:
        return BrokerHistorySnapshot(
            orders=self.list_orders(account_key),
            trades=self.list_trades(account_key),
        )

    def list_orders(
        self,
        account_key: str,
        *,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
        status: Optional[object] = None,
    ) -> Tuple[Mapping[str, object], ...]:
        rows = self._read("broker_order_history", account_key)
        expected_status = getattr(status, "value", status)
        return tuple(
            row
            for row in rows
            if (order_id is None or str(row.get("order_id")) == str(order_id))
            and (security is None or row.get("security") == security)
            and (
                status is None
                or str(getattr(row.get("status"), "value", row.get("status")))
                == str(expected_status)
            )
        )

    def list_trades(
        self,
        account_key: str,
        *,
        order_id: Optional[str] = None,
        security: Optional[str] = None,
    ) -> Tuple[Mapping[str, object], ...]:
        rows = self._read("broker_trade_history", account_key)
        return tuple(
            row
            for row in rows
            if (order_id is None or str(row.get("order_id")) == str(order_id))
            and (security is None or row.get("security") == security)
        )

    def _record(
        self,
        table: str,
        id_column: str,
        payload_id_key: str,
        account_key: str,
        payload: Mapping[str, object],
        observed_at: Optional[datetime],
    ) -> bool:
        account = str(account_key).strip()
        broker_id = str(payload.get(payload_id_key) or "").strip()
        if not account or not broker_id:
            return False
        observed = as_shanghai_time(observed_at or datetime.now(SHANGHAI_TZ))
        timestamp = observed.isoformat()
        kind = "trade" if table == "broker_trade_history" else "order"
        trading_day = broker_trading_day(payload, kind, observed)
        if trading_day is None:
            return False
        connection = connect_database(self.database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing_rows = connection.execute(
                "SELECT * FROM {} WHERE account_key = ? "
                "AND trading_day = ? AND {} = ?".format(
                    table, id_column
                ),
                (account, trading_day, broker_id),
            ).fetchall()
            existing = existing_rows[0] if existing_rows else None
            observation_no = 0
            if kind == "order":
                index = _matching_order_observation(
                    [json.loads(row["payload_json"]) for row in existing_rows], payload
                )
                existing = existing_rows[index] if index is not None else None
                observation_no = existing["observation_no"] if existing is not None else max(
                    (row["observation_no"] for row in existing_rows), default=-1
                ) + 1
            merged = dict(payload)
            if existing is not None:
                previous = json.loads(cast(str, existing["payload_json"]))
                merged = dict(_merge_payload(previous, payload))
            connection.execute(
                """
                INSERT INTO {table}(
                    account_key, trading_day, {id_column}, payload_json,
                    first_seen_at, last_seen_at{extra_column}
                ) VALUES (?, ?, ?, ?, ?, ?{extra_placeholder})
                ON CONFLICT(account_key, trading_day, {id_column}{extra_column}) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    last_seen_at = excluded.last_seen_at
                """.format(
                    table=table, id_column=id_column,
                    extra_column=", observation_no" if kind == "order" else "",
                    extra_placeholder=", ?" if kind == "order" else "",
                ),
                (
                    account,
                    trading_day,
                    broker_id,
                    _payload_json(merged),
                    timestamp,
                    timestamp,
                ) + ((observation_no,) if kind == "order" else ()),
            )
            connection.commit()
            return True
        except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
            connection.rollback()
            raise BrokerHistoryError("failed to persist broker history") from exc
        finally:
            connection.close()

    def _read(
        self, table: str, account_key: str
    ) -> Tuple[Mapping[str, object], ...]:
        connection = connect_database(self.database_path)
        try:
            rows = connection.execute(
                "SELECT trading_day, payload_json FROM {} "
                "WHERE account_key = ? ORDER BY trading_day, first_seen_at".format(
                    table
                ),
                (account_key,),
            ).fetchall()
            result = []
            kind = "trade" if table == "broker_trade_history" else "order"
            for row in rows:
                payload = json.loads(cast(str, row["payload_json"]))
                stored_day = str(row["trading_day"])
                payload_day = broker_trading_day(payload, kind)
                # The v5 cache keyed only by the numeric broker id. If QMT
                # reused that id, its last payload overwrote the older day.
                # Do not expose such a falsely dated migrated observation.
                if payload_day is not None and payload_day != stored_day:
                    continue
                payload["_broker_trading_day"] = stored_day
                result.append(payload)
            return tuple(result)
        except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
            raise BrokerHistoryError("failed to read broker history") from exc
        finally:
            connection.close()


def merge_broker_rows(
    current: Iterable[Mapping[str, object]],
    history: Iterable[Mapping[str, object]],
    id_key: str,
) -> list[Mapping[str, object]]:
    """Return durable history with the newest current observation winning."""

    kind = "trade" if id_key == "trade_id" else "order"
    merged = {}
    for row in history:
        broker_id = str(row.get(id_key) or "").strip()
        if not broker_id:
            continue
        historical = dict(row)
        historical["_broker_history_only"] = True
        day = broker_trading_day(historical, kind)
        merged[(day, broker_id, len(merged) if kind == "order" else 0)] = historical
    for row in current:
        broker_id = str(row.get(id_key) or "").strip()
        if not broker_id:
            continue
        day = broker_trading_day(row, kind)
        candidates = [
            key for key in merged
            if key[1] == broker_id and (day is None or key[0] == day)
        ]
        if kind == "order":
            index = _matching_order_observation([merged[key] for key in candidates], row)
            key = candidates[index] if index is not None else (day, broker_id, len(merged))
        else:
            key = candidates[0] if len(candidates) == 1 else (day, broker_id, 0)
        merged[key] = dict(_merge_payload(merged.get(key, {}), row))
        if day is not None:
            merged[key]["_broker_trading_day"] = day
        merged[key]["_broker_history_only"] = False
    return list(merged.values())
