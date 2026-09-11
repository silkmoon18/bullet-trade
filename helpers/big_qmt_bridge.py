#encoding:gbk
"""Paste this single file into full QMT's strategy-trading editor.

Execution bridge only: no selection, allocation, ledger, retries or chasing.
ASCII source is valid GBK as well as UTF-8. See docs/big-qmt-bridge.md.
"""

# ---- Local settings (never commit account IDs or tokens) ----
ACCOUNT_ID = ""                    # Empty: account selected in QMT's UI.
ACCOUNT_TYPE = "STOCK"              # This bridge currently supports STOCK only.
BRIDGE_PORT = 9001                  # BT listens on 127.0.0.1, never public.
BRIDGE_TOKEN = ""                   # Same as BIG_QMT_GATEWAY_PASSWORD.
ENABLE_TRADING = False              # passorder gate; does not enable server strategies.
ENABLE_CANCEL = False               # Independent cancel gate.
TIMER_MS = 500                      # Network pump, NOT an order retry interval.
MAX_SUBSCRIPTIONS = 100             # Subscribe only targets and owned positions.

import builtins
import errno
import json
import math
import select
import socket
import time
from collections import OrderedDict

BUILD_ID = "20260912_bridge_v1"
MAX_FRAME = 2 * 1024 * 1024
_runtime = None


def _api(name):
    fn = globals().get(name) or getattr(builtins, name, None)
    if not callable(fn):
        raise RuntimeError("QMT API unavailable: " + name)
    return fn


def _get(row, name, default=None):
    return row.get(name, default) if isinstance(row, dict) else getattr(row, name, default)


def _clean(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if hasattr(value, "item"):
        return _clean(value.item())
    return str(value)


def _raw(row):
    if isinstance(row, dict):
        return _clean(row)
    return {name: _clean(getattr(row, name)) for name in dir(row) if name.startswith("m_")}


def _qmt(code):
    return str(code).replace(".XSHG", ".SH").replace(".XSHE", ".SZ")


def _jq(code):
    return str(code).replace(".SH", ".XSHG").replace(".SZ", ".XSHE")


def _security(row):
    return _jq(str(_get(row, "m_strInstrumentID", "")) + "." + str(_get(row, "m_strExchangeID", "")))


def _id(value):
    text = str(value or "").strip()
    return "" if text in ("0", "-1") else text


def _date_time(row, date_field, time_field):
    day, clock = str(_get(row, date_field, "")), str(_get(row, time_field, ""))
    if not day or not clock:
        return None
    text = day.replace("-", "") + clock.replace(":", "").zfill(6)
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(text, "%Y%m%d%H%M%S"))
    except ValueError:
        return None


def _order(row):
    remark = _get(row, "m_strRemark") or _get(row, "m_strUserOrderId") or _get(row, "m_strOrderRemark", "")
    return dict(
        order_id=_id(_get(row, "m_strOrderSysID")), security=_security(row),
        amount=_get(row, "m_nVolumeTotalOriginal"), filled=_get(row, "m_nVolumeTraded"),
        raw_status=_get(row, "m_nOrderStatus"), side={23: "BUY", 24: "SELL"}.get(_get(row, "m_nOpType"), ""),
        price=_get(row, "m_dTradedPrice"), order_price=_get(row, "m_dLimitPrice"),
        order_remark=remark, remark=remark, qmt_user_order_id=remark,
        order_time=_date_time(row, "m_strInsertDate", "m_strInsertTime"),
        status_message=_get(row, "m_strCancelInfo") or _get(row, "m_strErrorMsg"), raw=_raw(row),
    )


def _trade(row):
    remark = _get(row, "m_strRemark") or _get(row, "m_strUserOrderId") or _get(row, "m_strOrderRemark", "")
    return dict(
        trade_id=_id(_get(row, "m_strTradeID")),
        order_id=_id(_get(row, "m_strOrderSysID")), security=_security(row),
        amount=_get(row, "m_nVolume"), price=_get(row, "m_dTradePrice") or _get(row, "m_dPrice"),
        side={23: "BUY", 24: "SELL"}.get(_get(row, "m_nOpType"), ""),
        time=_date_time(row, "m_strTradeDate", "m_strTradeTime"),
        order_remark=remark, remark=remark,
        commission_fee=_get(row, "m_dCommission"), tax=_get(row, "m_dTax"),
        commission_known=_get(row, "m_dCommission") is not None,
        tax_known=_get(row, "m_dTax") is not None, raw=_raw(row),
    )


class BridgeRuntime:
    def __init__(self, context, account):
        self.c, self.account = context, account
        self.sock = None
        self.connected = self.welcomed = self.closed = False
        self.incoming = self.outgoing = b""
        self.next_connect = self.last_heartbeat = 0
        self.connect_started = 0
        self.seen = OrderedDict()
        self.subscriptions = {}
        self.details = {}
        self.details_day = ""
        self.pumping = False

    def rows(self, kind):
        result = _api("get_trade_detail_data")(self.account, ACCOUNT_TYPE, kind)
        if result is None:
            raise RuntimeError("QMT query returned no result: " + kind)
        return list(result)

    def account_info(self):
        rows = self.rows("account")
        if len(rows) != 1 or _get(rows[0], "m_dAvailable") is None:
            raise RuntimeError("QMT account not ready")
        row = rows[0]
        return dict(available_cash=_get(row, "m_dAvailable"), total_value=_get(row, "m_dBalance"),
                    positions_value=_get(row, "m_dInstrumentValue"), raw=_raw(row))

    def health(self):
        try:
            self.account_info()
            ready = True
        except Exception:
            ready = False
        return dict(ready=ready, trading_enabled=ENABLE_TRADING, cancel_enabled=ENABLE_CANCEL,
                    build_id=BUILD_ID)

    def send(self, message):
        if self.sock is None or self.closed:
            return
        frame = json.dumps(_clean(message), ensure_ascii=True, allow_nan=False).encode() + b"\n"
        if len(self.outgoing) + len(frame) > MAX_FRAME:
            self.disconnect()
            raise RuntimeError("QMT bridge output overflow; reconnect and reconcile")
        self.outgoing += frame

    def event(self, name, payload):
        if self.welcomed:
            self.send(dict(type="event", event=name, payload=payload))

    def detail(self, code):
        day = time.strftime("%Y%m%d")
        if day != self.details_day:
            self.details, self.details_day = {}, day
        if code not in self.details:
            self.details[code] = self.c.get_instrument_detail(code) or {}
        return self.details[code]

    def quotes(self, payload):
        result = {}
        for code, values in payload.items():
            values = values if isinstance(values, list) else [values]
            rows = []
            detail = self.detail(code)
            for tick in values:
                row = dict(tick)
                # Never replace the quote's source timestamp with the receive time.
                row.update({key: detail[key] for key in ("UpStopPrice", "DownStopPrice", "PriceTick") if key in detail})
                rows.append(row)
            result[_jq(code)] = rows
        return result

    def on_quote(self, payload):
        if not self.closed and self.welcomed:
            self.event("tick", self.quotes(payload))

    def subscriptions_to(self, codes):
        desired = {_qmt(code) for code in codes}
        if len(desired) > MAX_SUBSCRIPTIONS:
            raise ValueError("Too many QMT quote subscriptions")
        for code in set(self.subscriptions) - desired:
            self.c.unsubscribe_quote(self.subscriptions.pop(code))
        for code in desired - set(self.subscriptions):
            sub = self.c.subscribe_quote(code, period="tick", dividend_type="none", result_type="dict", callback=self.on_quote)
            if not isinstance(sub, int) or sub <= 0:
                raise RuntimeError("QMT quote subscription failed: " + code)
            self.subscriptions[code] = sub
        return sorted(desired)

    def dispatch(self, action, payload, boundary):
        if payload.get("account_id", self.account) != self.account:
            raise ValueError("Account mismatch")
        if action == "/health":
            return self.health()
        if action == "/account":
            return self.account_info()
        if action == "/positions":
            return [dict(security=_security(row), amount=_get(row, "m_nVolume"),
                         closeable_amount=_get(row, "m_nCanUseVolume"), cost_basis=_get(row, "m_dOpenPrice"),
                         market_value=_get(row, "m_dMarketValue"), raw=_raw(row)) for row in self.rows("position")]
        if action in ("/orders", "/order_status"):
            rows = [_order(row) for row in self.rows("order")]
            if action == "/order_status":
                return next((row for row in rows if row["order_id"] == str(payload["order_id"])), {})
            return rows
        if action == "/trades":
            return [_trade(row) for row in self.rows("deal")]
        if action == "/data/subscriptions":
            return self.subscriptions_to(payload["symbols"])
        code = _qmt(payload.get("security", ""))
        if action in ("/data/snapshot", "/data/current_tick", "/data/live_current"):
            ticks = self.c.get_full_tick([code])
            if not ticks or not ticks.get(code):
                raise RuntimeError("QMT quote missing: " + code)
            return self.quotes(ticks)[_jq(code)][-1]
        if action == "/data/security_info":
            detail = dict(self.detail(code))
            detail.update(code=_jq(code), display_name=detail.get("InstrumentName", ""))
            return detail
        if action == "/data/tplus":
            return 0 if code in self.c.get_stock_list_in_sector("T+0\u57fa\u91d1") else 1
        if action == "/data/trade_days":
            return dict(values=self.c.get_trading_dates(code or "000001.SH", payload.get("start", ""),
                        payload.get("end", ""), payload.get("count", -1), "1d"))
        if action == "/place_order":
            if not ENABLE_TRADING:
                raise RuntimeError("TRADING_DISABLED")
            side, amount = payload.get("side"), payload.get("amount")
            price, pr_type = payload.get("price"), payload.get("pr_type", 11)
            style = payload.get("style") or {}
            if price is None:
                price = style.get("price", style.get("protect_price"))
            if side not in ("BUY", "SELL") or type(amount) is not int or amount <= 0:
                raise ValueError("Invalid order side/amount")
            if price is None or not math.isfinite(float(price)) or float(price) <= 0:
                raise ValueError("An explicit limit/protection price is required")
            if not code.endswith((".SH", ".SZ")):
                raise ValueError("Only SH/SZ cash securities are supported")
            if pr_type not in (11, 42, 43, 44, 45, 46, 47, 48):
                raise ValueError("Unsupported QMT price type")
            tag = str(payload.get("qmt_user_order_id") or "")
            if not tag or len(tag.encode("ascii")) > 23:
                raise ValueError("Missing or invalid QMT order tag")
            fn = _api("passorder")
            boundary[0] = True
            fn(23 if side == "BUY" else 24, 1101, self.account, code, pr_type, float(price), amount,
               "bt_bridge", 2, tag, self.c)
            # passorder return values are NOT stable broker order IDs.
            return dict(order_id="", status="submit_unknown", submit_unknown=True, qmt_user_order_id=tag)
        if action == "/cancel_order":
            if not ENABLE_CANCEL:
                raise RuntimeError("CANCEL_ORDER_DISABLED")
            order_id = str(payload.get("order_id") or "")
            if order_id in ("", "0"):
                raise ValueError("Missing order_id")
            fn = _api("cancel")
            boundary[0] = True
            fn(order_id, self.account, ACCOUNT_TYPE, self.c)
            return dict(order_id=order_id, status="submit_unknown", submit_unknown=True)
        raise ValueError("Unsupported bridge action: " + action)

    def command(self, message):
        key = str(message.get("id") or "")
        if not key:
            raise ValueError("Command ID required")
        if key in self.seen:
            self.send(self.seen[key])
            return
        boundary = [False]
        response = dict(type="response", id=key, ok=False)
        write = message.get("action") in ("/place_order", "/cancel_order")
        reserved = False
        try:
            if time.time() > float(message.get("deadline", 0)):
                raise RuntimeError("COMMAND_EXPIRED")
            if write:
                if len(self.seen) >= 50000:
                    raise RuntimeError("COMMAND_CACHE_FULL: restart only after reconciliation")
                # Only writes need deduplication. Never evict an uncertain write.
                self.seen[key] = dict(response, code="SUBMIT_UNKNOWN")
                reserved = True
            response.update(ok=True, value=self.dispatch(message["action"], message.get("payload") or {}, boundary))
        except Exception as exc:
            response.update(error=str(exc), code="QMT_ACTION_FAILED", broker_called=boundary[0])
        if reserved:
            self.seen[key] = response
        self.send(response)

    def pump(self):
        if self.closed or self.pumping:
            return
        self.pumping = True
        try:
            self._pump()
        except (OSError, ValueError, RuntimeError) as exc:
            print("[bt_bridge] connection paused: " + str(exc))
            self.disconnect()
        finally:
            self.pumping = False

    def _pump(self):
        now = time.monotonic()
        if self.sock is None:
            if now < self.next_connect:
                return
            self.sock = socket.socket()
            self.sock.setblocking(False)
            error = self.sock.connect_ex(("127.0.0.1", BRIDGE_PORT))
            if error not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, 10035, 10036):
                raise OSError(error, "BT bridge unavailable")
            self.connect_started = now
        readable, writable, exceptional = select.select([self.sock], [self.sock], [self.sock], 0)
        if exceptional:
            raise OSError("QMT bridge socket failed")
        if not self.connected:
            if now - self.connect_started > 5:
                raise OSError("QMT bridge connect timed out")
            if not writable:
                return
            if self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                raise OSError("QMT bridge connect failed")
            self.connected = True
            self.send(dict(type="hello", version=1, token=BRIDGE_TOKEN, account_id=self.account,
                           account_type=ACCOUNT_TYPE, health=self.health()))
        if readable:
            data = self.sock.recv(65536)
            if not data:
                raise OSError("BT bridge disconnected")
            self.incoming += data
            if len(self.incoming) > MAX_FRAME:
                raise ValueError("QMT bridge input too large")
        for _ in range(32):
            if b"\n" not in self.incoming:
                break
            raw, self.incoming = self.incoming.split(b"\n", 1)
            message = json.loads(raw.decode("utf-8"))
            if message.get("type") == "welcome" and message.get("version") == 1:
                self.welcomed = True
                print("[bt_bridge] connected | trading=" + str(ENABLE_TRADING))
            elif self.welcomed and message.get("type") == "command":
                self.command(message)
            else:
                raise ValueError("Invalid QMT bridge message")
        if self.welcomed and now - self.last_heartbeat >= 2:
            self.send(dict(type="heartbeat", health=self.health()))
            self.last_heartbeat = now
        if self.outgoing and writable:
            try:
                size = self.sock.send(self.outgoing)
                self.outgoing = self.outgoing[size:]
            except BlockingIOError:
                pass

    def disconnect(self):
        if self.sock is not None:
            self.sock.close()
        self.sock = None
        self.connected = self.welcomed = False
        self.incoming = self.outgoing = b""
        self.next_connect = time.monotonic() + 2

    def close(self):
        self.closed = True
        self.disconnect()
        # stop() runs after the trade connection closes: no broker writes here.
        for sub in self.subscriptions.values():
            try:
                self.c.unsubscribe_quote(sub)
            except Exception:
                pass
        self.subscriptions.clear()


def init(ContextInfo):
    global _runtime
    if _runtime is not None and not _runtime.closed:
        raise RuntimeError("Bridge already running")
    if getattr(ContextInfo, "do_back_test", False):
        raise RuntimeError("Bridge requires strategy-trading mode, not backtest")
    account_id = ACCOUNT_ID or globals().get("account") or getattr(ContextInfo, "accountID", "")
    if not account_id or not BRIDGE_TOKEN or ACCOUNT_TYPE != "STOCK":
        raise RuntimeError("Configure account, BRIDGE_TOKEN and STOCK account type")
    ContextInfo.set_account(str(account_id))
    runtime = BridgeRuntime(ContextInfo, str(account_id))
    _runtime = runtime
    try:
        ContextInfo.run_time("on_timer", str(TIMER_MS) + "nMilliSecond", "20200101000000")
    except Exception:
        runtime.close()
        raise
    print("[bt_bridge] " + BUILD_ID + " | waiting for BT | trading=" + str(ENABLE_TRADING))


def on_timer(ContextInfo):
    if _runtime is not None:
        _runtime.pump()


def handlebar(ContextInfo):
    pass  # Historical bars must NEVER execute commands.


def stop(ContextInfo):
    if _runtime is not None:
        _runtime.close()


def _event(name, row, converter=_raw):
    if _runtime is None or _runtime.closed:
        return
    if str(_get(row, "m_strAccountID", _runtime.account)) != _runtime.account:
        return
    _runtime.event(name, converter(row))


def order_callback(ContextInfo, orderInfo):
    _event("order", orderInfo, _order)


def deal_callback(ContextInfo, dealInfo):
    _event("trade", dealInfo, _trade)


def account_callback(ContextInfo, accountInfo):
    _event("account", accountInfo)


def position_callback(ContextInfo, positionInfo):
    _event("position", positionInfo)


def orderError_callback(ContextInfo, orderArgs, errorMsg):
    if _runtime is not None and not _runtime.closed:
        _runtime.event("order_error", dict(arguments=_raw(orderArgs), message=str(errorMsg)))
