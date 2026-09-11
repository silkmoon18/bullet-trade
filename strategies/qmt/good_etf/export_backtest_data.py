# coding: utf-8
"""一次性数据准备工具：在聚宽研究中运行全文，不上传到 QMT 策略目录。

无账号凭据、无交易接口；使用已登录研究环境的数据权限。
历史池按前交易日查询；简称采用数据源返回值，不承诺历史名称版本。
"""
START_DATE = "2026-08-03"
END_DATE = "2026-08-28"
OUTPUT_FILE = "good_etf_backtest_data.json"

import datetime as dt
import json
import math


def qmt_code(code):
    return code.replace(".XSHG", ".SH").replace(".XSHE", ".SZ")


def number(raw):
    try:
        result = float(raw)
        return result if math.isfinite(result) and result > 0 else None
    except (ValueError, TypeError):
        return None


def build_history(api, start_date, end_date):
    days = list(api.get_trade_days(start_date=start_date, end_date=end_date))
    if not days:
        raise RuntimeError("指定范围没有交易日")
    result = {"schema": 1, "source": "JoinQuant research; unadjusted high_limit; unit_net_value",
              "name_basis": "provider_display_name_not_guaranteed_historical",
              "sessions": {}}
    for day in days:
        calendar = list(api.get_trade_days(end_date=day, count=2))
        if len(calendar) != 2 or calendar[-1] != day:
            raise RuntimeError("前交易日缺失: " + str(day))
        previous = calendar[0]
        pool = api.get_all_securities(types=["etf"], date=previous)
        codes = list(pool.index)
        if not codes:
            raise RuntimeError("历史 ETF 池为空: " + str(previous))
        navs = api.get_extras("unit_net_value", codes, start_date=previous, end_date=previous, df=True)
        if navs.empty or str(navs.index[-1])[:10] != str(previous):
            raise RuntimeError("历史净值日期不匹配: " + str(previous))
        # 只导出当日盘前已知涨停价，不取当日收盘价/最高价/成交额作为决策数据。
        limits = api.get_price(codes, end_date=day, count=1, frequency="daily",
                               fields=["high_limit"], fq=None, panel=False)
        if limits.empty or not {"time", "code", "high_limit"}.issubset(limits.columns):
            raise RuntimeError("历史涨停价数据缺失: " + str(day))
        limits = limits[limits["time"].astype(str).str[:10] == str(day)].set_index("code")
        securities = {}
        for code in codes:
            row = {"name": str(pool.loc[code, "display_name"]), "index_name": "",
                   "nav": number(navs.iloc[-1].get(code))}
            if code in limits.index:
                raw = limits.loc[code, "high_limit"]
                ceiling = number(raw)
                if ceiling is not None:
                    row["high_limit"] = ceiling
                elif str(raw).lower() == "inf":
                    row["high_limit"] = None  # 数据源明确无上限；NaN 不等于无限制
            securities[qmt_code(code)] = row
        result["sessions"][day.strftime("%Y%m%d")] = {
            "previous_date": previous.strftime("%Y%m%d"), "securities": securities}
        print("已整理 {}: ETF {} 只，净值缺失 {} 只，涨停价缺失 {} 只".format(
            day, len(codes), sum(row["nav"] is None for row in securities.values()),
            sum("high_limit" not in row for row in securities.values())))
    return result


def main():
    import jqdata
    result = build_history(jqdata, START_DATE, END_DATE)
    result["exported_at"] = dt.datetime.now().isoformat()
    # 聚宽研究文件空间保存；这不是策略/券商写操作。不覆盖已有同名文件。
    with open(OUTPUT_FILE, "x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    print("导出完成，请下载 {} 并填写 QMT 策略的 BACKTEST_DATA_FILE".format(OUTPUT_FILE))


if __name__ == "__main__":
    main()
