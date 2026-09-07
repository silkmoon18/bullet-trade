from dataclasses import replace
import json

import pytest

from bullet_trade.server.adapters.big_qmt import _normalize_trade
from bullet_trade.server.adapters.big_qmt import BigQmtBrokerAdapter
from bullet_trade.server.adapters.qmt import QmtBrokerAdapter
from bullet_trade.server.strategy.broker_contract import (
    BIG_QMT_CAPABILITIES,
    XTQUANT_DIRECT_CAPABILITIES,
    BrokerContractError,
    CapabilityState,
    normalize_trade_batch,
    normalize_trade_evidence,
    require_strategy_ledger_v1,
    strategy_ledger_v1_blockers,
    load_verified_capabilities,
)
from bullet_trade.server.strategy.domain import (
    FillPriceSource,
    OrderSide,
    UnpricedFillPolicy,
)


def _verified_profile(profile):
    return replace(
        profile,
        client_tag_roundtrip=CapabilityState.SUPPORTED,
        stable_order_id=CapabilityState.SUPPORTED,
        stable_trade_id=CapabilityState.SUPPORTED,
        trade_order_link=CapabilityState.SUPPORTED,
        order_side_for_trade=CapabilityState.SUPPORTED,
        fee_fields=CapabilityState.SUPPORTED,
        order_status=CapabilityState.SUPPORTED,
        current_orders_query=CapabilityState.SUPPORTED,
        current_trades_query=CapabilityState.SUPPORTED,
        working_orders_query=CapabilityState.SUPPORTED,
        order_lookback_days=1,
        trade_lookback_days=1,
    )


def test_qmt_adapters_expose_their_strategy_ledger_profiles():
    assert (
        QmtBrokerAdapter.strategy_ledger_capabilities()
        is XTQUANT_DIRECT_CAPABILITIES
    )
    assert BigQmtBrokerAdapter.strategy_ledger_capabilities() is BIG_QMT_CAPABILITIES


@pytest.mark.parametrize(
    "profile", [XTQUANT_DIRECT_CAPABILITIES, BIG_QMT_CAPABILITIES]
)
def test_unprobed_qmt_profile_is_not_live_ready(profile):
    blockers = strategy_ledger_v1_blockers(profile)
    assert "stable_trade_id=PROBE_REQUIRED" in blockers
    assert "order_lookback_days<1" in blockers
    with pytest.raises(BrokerContractError, match="does not satisfy"):
        require_strategy_ledger_v1(profile)


@pytest.mark.parametrize(
    "profile", [XTQUANT_DIRECT_CAPABILITIES, BIG_QMT_CAPABILITIES]
)
def test_verified_qmt_profile_can_use_order_mapping_for_trade_side(profile):
    verified = _verified_profile(profile)
    assert verified.direct_trade_side is CapabilityState.UNSUPPORTED
    assert strategy_ledger_v1_blockers(verified) == ()
    require_strategy_ledger_v1(verified)


def test_unknown_fee_capability_does_not_block_execution_evidence():
    profile = replace(
        _verified_profile(XTQUANT_DIRECT_CAPABILITIES),
        fee_fields=CapabilityState.UNSUPPORTED,
    )

    assert strategy_ledger_v1_blockers(profile) == ()
    require_strategy_ledger_v1(profile)


def test_durable_local_history_can_replace_native_cross_day_query():
    verified_without_lookback = replace(
        _verified_profile(XTQUANT_DIRECT_CAPABILITIES),
        order_lookback_days=0,
        trade_lookback_days=0,
    )
    assert strategy_ledger_v1_blockers(
        verified_without_lookback, durable_broker_history=True
    ) == ()
    require_strategy_ledger_v1(
        verified_without_lookback, durable_broker_history=True
    )


def test_verified_capability_evidence_loads_for_matching_adapter(tmp_path):
    path = tmp_path / "capabilities.json"
    states = {
        "client_tag_roundtrip": True,
        "stable_order_id": True,
        "stable_trade_id": True,
        "trade_order_link": True,
        "direct_trade_side": False,
        "order_side_for_trade": True,
        "fee_fields": True,
        "order_status": True,
        "current_orders_query": True,
        "current_trades_query": True,
        "working_orders_query": True,
    }
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "adapter_kind": "XTQUANT_DIRECT",
                "verified_at": "2026-08-11T20:00:00+08:00",
                "probe_report": "runtime-probe/probe_report.json",
                "order_lookback_days": 1,
                "trade_lookback_days": 1,
                "capabilities": states,
            }
        ),
        encoding="utf-8",
    )
    profile = load_verified_capabilities(path, "XTQUANT_DIRECT")
    assert strategy_ledger_v1_blockers(profile) == ()
    with pytest.raises(BrokerContractError, match="adapter does not match"):
        load_verified_capabilities(path, "BIG_QMT")


def test_incomplete_capability_evidence_is_rejected(tmp_path):
    path = tmp_path / "capabilities.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "adapter_kind": "XTQUANT_DIRECT",
                "verified_at": "2026-08-11",
                "probe_report": "probe.json",
                "capabilities": {},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(BrokerContractError, match="non-boolean"):
        load_verified_capabilities(path, "XTQUANT_DIRECT")


def test_trade_order_link_without_order_side_does_not_satisfy_side_contract():
    profile = replace(
        _verified_profile(XTQUANT_DIRECT_CAPABILITIES),
        order_side_for_trade=CapabilityState.PROBE_REQUIRED,
    )
    assert "trade_side_has_no_order_mapping" in strategy_ledger_v1_blockers(profile)


def test_trade_side_maps_only_through_its_broker_order_id():
    evidence = normalize_trade_evidence(
        {
            "trade_id": "T-1",
            "trade_id_source": "broker",
            "order_id": "O-1",
            "security": "510050.XSHG",
            "amount": 100,
            "price": 2.5,
            "commission_fee": 1.25,
            "commission_known": True,
            "tax": 0.0,
            "tax_known": True,
            "time": "2026-08-10 10:00:00",
        },
        {"O-1": {"order_id": "O-1", "is_buy": False}},
    )
    assert evidence.broker_trade_id == "T-1"
    assert evidence.side is OrderSide.SELL
    assert evidence.commission_units == 12_500
    assert evidence.tax_units == 0


def test_trade_without_matching_side_is_rejected():
    with pytest.raises(BrokerContractError, match="side cannot be mapped"):
        normalize_trade_evidence(
            {
                "trade_id": "T-1",
                "trade_id_source": "broker",
                "order_id": "O-missing",
                "security": "510050.XSHG",
                "amount": 100,
                "price": 2.5,
                "commission_fee": 0,
                "tax": 0,
                "time": "2026-08-10 10:00:00",
            },
            {},
        )


def test_synthetic_trade_id_is_rejected():
    with pytest.raises(BrokerContractError, match="missing or synthetic"):
        normalize_trade_evidence(
            {
                "trade_id": "synthetic-id",
                "trade_id_source": "synthetic",
                "order_id": "O-1",
                "security": "510050.XSHG",
                "amount": 100,
                "price": 2.5,
                "side": "BUY",
                "commission_fee": 0,
                "tax": 0,
                "time": "2026-08-10 10:00:00",
            },
            {},
        )


def test_missing_fee_evidence_is_preserved_as_unknown():
    evidence = normalize_trade_evidence(
        {
            "trade_id": "T-1",
            "trade_id_source": "broker",
            "order_id": "O-1",
            "security": "510050.XSHG",
            "amount": 100,
            "price": 2.5,
            "side": "BUY",
            "commission_fee": 0,
            "commission_known": False,
            "tax": 0,
            "tax_known": True,
            "time": "2026-08-10 10:00:00",
        },
        {},
    )

    assert evidence.commission_units is None
    assert evidence.tax_units == 0


def test_zero_trade_price_uses_positive_deal_balance():
    evidence = normalize_trade_evidence(
        {
            "trade_id": "T-price-from-balance",
            "trade_id_source": "broker",
            "order_id": "O-1",
            "security": "510050.XSHG",
            "amount": 100,
            "price": 0,
            "traded_price": 0,
            "deal_balance": 250.0,
            "side": "SELL",
            "time": "2026-08-10 10:00:00",
        },
        {},
    )

    assert evidence.price_units == 2_500_000
    assert evidence.price_source is FillPriceSource.BROKER_TRADE
    assert evidence.price_known is True


def test_conservative_policy_uses_full_order_protection_price():
    trade = {
        "trade_id": "T-estimated",
        "trade_id_source": "broker",
        "order_id": "O-1",
        "security": "510050.XSHG",
        "amount": 2400,
        "price": 0,
        "traded_price": 0,
        "deal_balance": 0,
        "side": "SELL",
        "time": "2026-08-10 10:00:00",
    }
    order = {
        "order_id": "O-1",
        "amount": 2400,
        "filled": 2400,
        "order_price": 0.771,
        "is_buy": False,
    }

    with pytest.raises(BrokerContractError, match="price is invalid"):
        normalize_trade_evidence(trade, {"O-1": order})

    evidence = normalize_trade_evidence(
        trade,
        {"O-1": order},
        UnpricedFillPolicy.CONSERVATIVE_ORDER_PRICE,
    )
    assert evidence.price_units == 771_000
    assert evidence.price_source is FillPriceSource.ORDER_PRICE_FALLBACK
    assert evidence.price_known is False


def test_conservative_policy_rejects_partial_order_price_fallback():
    trade = {
        "trade_id": "T-partial",
        "trade_id_source": "broker",
        "order_id": "O-1",
        "security": "510050.XSHG",
        "amount": 100,
        "price": 0,
        "side": "SELL",
        "time": "2026-08-10 10:00:00",
    }
    order = {
        "order_id": "O-1",
        "amount": 200,
        "filled": 100,
        "order_price": 2.5,
        "is_buy": False,
    }

    with pytest.raises(BrokerContractError, match="price is invalid"):
        normalize_trade_evidence(
            trade,
            {"O-1": order},
            UnpricedFillPolicy.CONSERVATIVE_ORDER_PRICE,
        )


@pytest.mark.parametrize(
    "price, balance, order_price, expected_price, source",
    [
        (2.5, 260, 2.7, 2_500_000, FillPriceSource.BROKER_TRADE),
        (0, 260, 2.7, 2_600_000, FillPriceSource.BROKER_TRADE),
        (0, 0, 2.7, 2_700_000, FillPriceSource.ORDER_PRICE_FALLBACK),
        (0, 0, 0, 0, FillPriceSource.ZERO_FALLBACK),
        (None, None, None, 0, FillPriceSource.ZERO_FALLBACK),
    ],
)
def test_zero_fallback_is_last_resort(price, balance, order_price, expected_price, source):
    trade = {
        "trade_id": "T-zero", "trade_id_source": "broker", "order_id": "O-1",
        "security": "510050.XSHG", "amount": 100, "side": "SELL",
        "price": price, "deal_balance": balance, "time": "2026-09-07 10:00:00",
    }
    evidence = normalize_trade_evidence(trade, {
        "O-1": {"amount": 100, "filled": 100, "order_price": order_price},
    }, UnpricedFillPolicy.ZERO_FALLBACK)
    assert evidence.price_units == expected_price
    assert evidence.price_source is source
    assert evidence.price_known is (source is FillPriceSource.BROKER_TRADE)
    assert evidence.commission_units is None
    assert evidence.tax_units is None


@pytest.mark.parametrize("field, value", [("amount", 0), ("trade_id", ""), ("order_id", ""), ("side", "")])
def test_zero_fallback_does_not_invent_non_price_evidence(field, value):
    trade = {
        "trade_id": "T-zero", "trade_id_source": "broker", "order_id": "O-1",
        "security": "510050.XSHG", "amount": 100, "side": "SELL",
        "price": 0, "time": "2026-09-07 10:00:00",
    }
    trade[field] = value
    with pytest.raises(BrokerContractError):
        normalize_trade_evidence(trade, {}, UnpricedFillPolicy.ZERO_FALLBACK)


@pytest.mark.parametrize(
    "fee_field, fee_value",
    [("commission_fee", -1), ("tax", "-0.1")],
)
def test_negative_broker_fees_are_rejected(fee_field, fee_value):
    trade = {
        "trade_id": "T-1",
        "trade_id_source": "broker",
        "order_id": "O-1",
        "security": "510050.XSHG",
        "amount": 100,
        "price": 2.5,
        "side": "BUY",
        "commission_fee": 0,
        "tax": 0,
        "time": "2026-08-10 10:00:00",
    }
    trade[fee_field] = fee_value
    with pytest.raises(BrokerContractError, match="cannot be negative"):
        normalize_trade_evidence(trade, {})


def test_duplicate_identical_trades_are_one_fill_but_conflicts_are_rejected():
    first = {
        "trade_id": "T-1",
        "trade_id_source": "broker",
        "order_id": "O-1",
        "security": "510050.XSHG",
        "amount": 100,
        "price": "2.50",
        "commission_fee": "1.00",
        "tax": "0",
        "time": "2026-08-10 10:00:00",
    }
    second = dict(first)
    trades = normalize_trade_batch(
        [first, second],
        [{"order_id": "O-1", "side": "BUY"}],
    )
    assert len(trades) == 1
    assert trades[0].side is OrderSide.BUY

    conflicting = dict(first, amount=200)
    with pytest.raises(BrokerContractError, match="conflicting"):
        normalize_trade_batch(
            [first, conflicting],
            [{"order_id": "O-1", "side": "BUY"}],
        )


def test_same_trade_id_on_different_trading_days_is_not_a_duplicate():
    first = {
        "trade_id": "T-REUSED",
        "trade_id_source": "broker",
        "order_id": "O-REUSED",
        "security": "510050.XSHG",
        "amount": 100,
        "price": "2.50",
        "side": "BUY",
        "time": "2026-08-27 10:00:00",
    }
    second = dict(first, side="SELL", time="2026-09-03 10:00:00")

    trades = normalize_trade_batch([first, second], [])

    assert len(trades) == 2
    assert [trade.side for trade in trades] == [OrderSide.BUY, OrderSide.SELL]


def test_big_qmt_normalization_marks_native_ids_and_fee_presence():
    normalized = _normalize_trade(
        {
            "m_strTradeID": "T-1",
            "m_strOrderSysID": "O-1",
            "m_dCommission": 0.0,
            "m_dTax": 0.0,
        }
    )
    assert normalized["trade_id"] == "T-1"
    assert normalized["trade_id_source"] == "broker"
    assert normalized["commission_known"] is True
    assert normalized["tax_known"] is True

    xttrade = _normalize_trade(
        {
            "traded_id": "XT-T-1",
            "order_id": "XT-O-1",
            "used_commission": 5.25,
        }
    )
    assert xttrade["trade_id"] == "XT-T-1"
    assert xttrade["trade_id_source"] == "broker"
    assert xttrade["commission_fee"] == 5.25
    assert xttrade["commission_known"] is True

    native_time = _normalize_trade(
        {
            "m_strTradeID": "T-time",
            "m_strTradeDate": "20260810",
            "m_strTradeTime": "100000",
        }
    )
    assert native_time["time"] == "2026-08-10 10:00:00"

    missing = _normalize_trade({"m_strOrderSysID": "O-2"})
    assert missing["trade_id_source"] == "missing"
    assert missing["commission_known"] is False
    assert missing["tax_known"] is False

    upstream_default = _normalize_trade(
        {
            "trade_id": "T-2",
            "commission_fee": 0,
            "commission_known": False,
            "tax": 0,
            "tax_known": False,
        }
    )
    assert upstream_default["commission_known"] is False
    assert upstream_default["tax_known"] is False


def test_hhmmss_without_trade_date_is_not_misread_as_unix_time():
    with pytest.raises(BrokerContractError, match="trade time is invalid"):
        normalize_trade_evidence(
            {
                "trade_id": "T-1",
                "trade_id_source": "broker",
                "order_id": "O-1",
                "security": "510050.XSHG",
                "amount": 100,
                "price": 2.5,
                "side": "BUY",
                "commission_fee": 0,
                "tax": 0,
                "time": 100000,
            },
            {},
        )
