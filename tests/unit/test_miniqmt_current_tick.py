import pytest

from bullet_trade.data.providers.miniqmt import MiniQMTProvider


class _FullTickXtData:
    def __init__(self):
        self.full_tick_calls = []
        self.last_quote_calls = []

    def get_full_tick(self, codes):
        self.full_tick_calls.append(tuple(codes))
        return {
            "510050.SH": {
                "lastPrice": 2.501,
                "time": 1783043331000,
                "bidPrice": [2.5],
                "askPrice": [2.502],
            }
        }

    def get_last_quote(self, code):
        self.last_quote_calls.append(code)
        raise AssertionError("fresh full tick must be preferred")


def test_current_tick_prefers_full_tick_and_preserves_market_time(monkeypatch):
    xtdata = _FullTickXtData()
    monkeypatch.setattr(
        MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: xtdata),
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})

    tick = provider.get_current_tick("510050.XSHG")

    assert tick == {
        "sid": "510050.XSHG",
        "last_price": 2.501,
        "dt": "2026-07-03T09:48:51",
        "bidPrice": [2.5],
        "askPrice": [2.502],
    }
    assert xtdata.full_tick_calls == [("510050.SH",)]
    assert xtdata.last_quote_calls == []


def test_current_tick_never_invents_timestamp(monkeypatch):
    class MissingTimeXtData:
        def get_full_tick(self, codes):
            return {codes[0]: {"lastPrice": 2.501}}

        def get_last_quote(self, code):
            return {"lastPrice": 2.501}

    monkeypatch.setattr(
        MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: MissingTimeXtData()),
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})

    def no_kline(*args, **kwargs):
        raise RuntimeError("no kline")

    monkeypatch.setattr(
        provider,
        "get_price",
        no_kline,
    )

    assert provider.get_current_tick("510050.XSHG") is None


def test_tplus_uses_qmt_t0_fund_sector_and_caches_for_the_day(monkeypatch):
    class SectorXtData:
        def __init__(self):
            self.calls = 0

        def get_stock_list_in_sector(self, sector):
            assert sector == "T+0基金"
            self.calls += 1
            return ["520998.SH", "159998.SZ"]

    xtdata = SectorXtData()
    monkeypatch.setattr(
        MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: xtdata),
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})

    assert provider.get_tplus("520998.XSHG") == 0
    assert provider.get_tplus("510300.XSHG") == 1
    assert provider.get_tplus("159998.XSHE") == 0
    assert xtdata.calls == 1


def test_etf_metadata_retains_qmt_limits_even_on_listing_day(monkeypatch):
    from datetime import date

    class ListingXtData(_FullTickXtData):
        def get_instrument_detail(self, code):
            return {
                "OpenDate": date.today().strftime("%Y%m%d"),
                "UpStopPrice": 2.750,
                "DownStopPrice": 2.250,
            }

        def get_instrument_type(self, code):
            return {"fund": True, "etf": True}

    xtdata = ListingXtData()
    monkeypatch.setattr(
        MiniQMTProvider, "_ensure_xtdata", staticmethod(lambda: xtdata)
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})
    tick = provider.get_current_tick("510050.XSHG")

    assert tick["high_limit"] == 2.750
    assert tick["low_limit"] == 2.250


def test_tplus_missing_qmt_sector_defaults_to_t1(monkeypatch):
    class MissingSectorXtData:
        def get_stock_list_in_sector(self, sector):
            raise RuntimeError("sector unavailable")

    monkeypatch.setattr(
        MiniQMTProvider,
        "_ensure_xtdata",
        staticmethod(lambda: MissingSectorXtData()),
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})
    assert provider.get_tplus("520999.XSHG") == 1


def test_tplus_explicit_code_override_works_without_qmt_sector(monkeypatch):
    from bullet_trade.data import api

    # Read the checked-in defaults, independent of overrides from other tests.
    monkeypatch.setattr(api, "_security_overrides_loaded", False)
    monkeypatch.setattr(api, "_security_overrides", {})
    monkeypatch.setattr(
        MiniQMTProvider, "_ensure_xtdata",
        staticmethod(lambda: pytest.fail("explicit code must not need QMT")),
    )
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})
    assert provider.get_tplus("520890.XSHG") == 0
    assert provider.get_tplus("520890.SH") == 0
    assert provider.get_tplus("518880.XSHG") == 0


@pytest.mark.parametrize("override, expected", [(0, 0), (1, 1), (True, None), (2, None), ("0", None)])
def test_tplus_only_uses_explicit_valid_code_override(monkeypatch, override, expected):
    from bullet_trade.data import api

    monkeypatch.setattr(api, "_security_overrides_loaded", True)
    monkeypatch.setattr(api, "_security_overrides", {
        "by_category": {"fund": {"tplus": 0}},
        "by_prefix": {"510": {"tplus": 0}},
        "by_code": {"520890.XSHG": {"tplus": override}},
    })
    assert api.get_security_tplus_override("520890.XSHG") == expected
    assert api.get_security_tplus_override("510300.XSHG") is None


@pytest.mark.parametrize("failure", ["empty", "exception"])
def test_tplus_retries_failed_sector_then_caches_success(monkeypatch, failure):
    from bullet_trade.data import api
    from bullet_trade.data.providers import miniqmt

    monkeypatch.setattr(api, "_security_overrides_loaded", True)
    monkeypatch.setattr(api, "_security_overrides", {})
    clock = [100.0]
    monkeypatch.setattr(miniqmt.time, "monotonic", lambda: clock[0])

    class RecoveringXtData:
        calls = 0

        def get_stock_list_in_sector(self, sector):
            self.calls += 1
            if self.calls == 1:
                if failure == "exception":
                    raise RuntimeError("temporary failure")
                return []
            return ["520999.SH"]

    xtdata = RecoveringXtData()
    monkeypatch.setattr(MiniQMTProvider, "_ensure_xtdata", staticmethod(lambda: xtdata))
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})
    assert provider.get_tplus("520999.XSHG") == 1
    assert provider._t0_funds_cache_day is None
    clock[0] = 159.0
    assert provider.get_tplus("520999.XSHG") == 1
    assert xtdata.calls == 1
    clock[0] = 160.0
    assert provider.get_tplus("520999.XSHG") == 0
    assert provider.get_tplus("510300.XSHG") == 1
    assert xtdata.calls == 2


def test_tplus_explicit_t1_takes_priority_over_sector(monkeypatch):
    from bullet_trade.data import api

    monkeypatch.setattr(api, "_security_overrides_loaded", True)
    monkeypatch.setattr(api, "_security_overrides", {
        "by_code": {"510300.XSHG": {"tplus": 1}},
    })
    provider = MiniQMTProvider({"cache_dir": None, "mode": "live"})
    provider._t0_funds = {"510300.SH"}
    assert provider.get_tplus("510300.XSHG") == 1
