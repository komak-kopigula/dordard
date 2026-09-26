"""
Bot Scalping v24 DEMO — USDT.D MTF + 5M BOS + Capital Protection
===================================================================
IMPORTANT:
- HARD LOCK: Binance Futures DEMO only. No real-account fallback.
- Entry direction is driven by USDT.D 1H/2H/3H/4H and 5M BOS.
- Rising USDT.D => crypto SHORT bias.
- Falling USDT.D => crypto LONG bias.
- Entry requires a confirmed 5M BOS plus confluence filters.
- Profit protection uses portfolio equity (realized + floating PnL) and
  per-position profit locks.
- TIME_LIMIT is 3h maximum, but losing stagnant trades may be closed earlier.
- No unconditional ban for profitable TIME_LIMIT exits.
- Native exchange STOP/TP orders are used when available; local monitoring
  remains as an emergency layer.
"""

import os
import sys
import time
import math
import inspect
import threading
import queue
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

import numpy as np
import pandas as pd
import ta
from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager

try:
    from binance.exceptions import BinanceAPIException
except Exception:
    BinanceAPIException = Exception

try:
    from tvDatafeed import TvDatafeed, Interval
    TV_AVAILABLE = True
except Exception:
    TvDatafeed = None
    Interval = None
    TV_AVAILABLE = False

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ============================================================================
# HARD DEMO LOCK / CLIENT
# ============================================================================
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

# Never change this to False in this build.
BINANCE_DEMO = True
if not API_KEY or not API_SECRET:
    raise RuntimeError("API_KEY / API_SECRET belum tersedia di environment.")

# python-binance 1.0.37+ supports demo=True. A compatibility fallback is kept
# only for the REST URL; there is intentionally NO real Futures URL fallback.
try:
    sig = inspect.signature(Client.__init__)
    if "demo" in sig.parameters:
        client = Client(API_KEY, API_SECRET, demo=True, ping=False)
    else:
        client = Client(API_KEY, API_SECRET, ping=False)
        client.demo = True
        client.testnet = False
        client.FUTURES_URL = "https://demo-fapi.binance.com/fapi"
except Exception as exc:
    raise RuntimeError(f"Gagal membuat Binance DEMO client: {exc}") from exc

# Enforce demo Futures REST endpoint after constructor as an additional lock.
client.demo = True
client.testnet = False
client.FUTURES_URL = "https://demo-fapi.binance.com/fapi"

WS_MAX_QUEUE_SIZE = 2000
DEPTH_SOCKET_CHUNK = 8


def _create_twm():
    kwargs = {
        "api_key": API_KEY,
        "api_secret": API_SECRET,
        "testnet": False,
        "max_queue_size": WS_MAX_QUEUE_SIZE,
    }
    try:
        params = inspect.signature(ThreadedWebsocketManager.__init__).parameters
        kwargs = {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        kwargs.pop("max_queue_size", None)
    try:
        return ThreadedWebsocketManager(**kwargs)
    except TypeError:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)


twm = _create_twm()
# Current python-binance TWM creates its AsyncClient internally. Inject demo=True
# into that parameter dict before twm.start(). This keeps websocket environment
# aligned with the demo REST client.
if hasattr(twm, "_client_params"):
    twm._client_params["demo"] = True
    twm._client_params["testnet"] = False

# ============================================================================
# CONFIGURATION
# ============================================================================
LEVERAGE = 20
MAX_POSITIONS = 2
MAX_MARGIN_USDT = 2.0
RISK_USDT_PER_TRADE = 0.40

# Entry quality gates
MIN_ENTRY_SCORE = 78
ADX_MIN = 18.0
MIN_VOLUME_RATIO = 1.05
MIN_24H_QUOTE_VOL = 5_000_000.0
MAX_SPREAD_BPS = 25.0
CORRELATION_BLOCK = 0.85
BOS_LOOKBACK = 60
BOS_PIVOT_LEFT = 2
BOS_PIVOT_RIGHT = 2
BOS_MAX_AGE_CANDLES = 3
BOS_ATR_BUFFER = 0.08

# ATR risk model
ATR_TP_MULT = 3.0
ATR_SL_MULT = 1.65
MIN_TP_PCT = 0.023
MAX_TP_PCT = 0.035
MIN_SL_PCT = 0.015
MAX_SL_PCT = 0.022

# Position management
MAX_HOLD_SECONDS = 3 * 60 * 60
EARLY_STALE_SECONDS = 90 * 60
COOLDOWN_SEC = 300
MONITOR_INT = 0.20
SCAN_INTERVAL = 4.0
SLOT_FILL_INT = 0.25
BATCH_SIZE = 12
MAX_WORKERS = 4

# Profit lock, per-position. This is NOT a traditional always-on trailing stop.
PROFIT_LOCK_ENABLED = True
PROFIT_LOCK_LEVELS = (
    (0.75, 0.00, "BE"),
    (1.00, 0.20, "LOCK_0.2R"),
    (1.50, 0.55, "LOCK_0.55R"),
    (2.00, 1.00, "LOCK_1R"),
)

# Portfolio/equity guard. More profit => less tolerated giveback.
EQUITY_GUARD_ENABLED = True
EQUITY_GUARD_ARM = 1.00
EQUITY_GUARD_BAN_SECONDS = 90 * 60
EQUITY_GUARD_LOCK_LOSERS_FIRST = True
EQUITY_GUARD_CLOSE_ALL_IF_BROKEN = True

# Loss circuits
SL_BAN_SECONDS = 3 * 60 * 60
CASCADE_BAN_SECONDS = 60 * 60
TIME_LIMIT_LOSS_BAN_SECONDS = 60 * 60
CONSEC_FIRST_PAUSE = 30 * 60
CONSEC_FIRST_THRESHOLD = 4
CONSEC_HARD_PAUSE = 3 * 60 * 60
CONSEC_HARD_THRESHOLD = 7
DAILY_LOSS_LIMIT = -20.0

# Native protective orders
USE_NATIVE_PROTECTIVE_ORDERS = True
NATIVE_ORDER_WORKING_TYPE = "MARK_PRICE"

# WebSocket health
WS_STALE_SEC = 15
MARKPRICE_FRESH_SEC = 5
KLINE_FRESH_SEC = 30

# REST safety
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0
REST_418_COOLDOWN = 900.0
REST_RETRIES = 2

# USDT.D TradingView
TV_SYMBOL = "USDT.D"
TV_EXCHANGE = "CRYPTOCAP"
TV_INTERVALS = (
    ("1h", "in_1_hour"),
    ("2h", "in_2_hour"),
    ("3h", "in_3_hour"),
    ("4h", "in_4_hour"),
)
USDTD_REFRESH_SEC = 90
USDTD_MIN_BARS = 80
USDTD_REQUIRE_ALIGNMENT = 3  # 3/4 agreeing is enough; 4/4 is preferred.

# Symbols — focus on liquid markets; unavailable symbols are filtered at startup.
SYMBOLS = list(dict.fromkeys([
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "LTCUSDT", "UNIUSDT", "NEARUSDT", "APTUSDT",
    "ARBUSDT", "OPUSDT", "INJUSDT", "SUIUSDT", "SEIUSDT",
    "FETUSDT", "WLDUSDT", "AAVEUSDT", "TONUSDT", "1000PEPEUSDT",
    "WIFUSDT", "JUPUSDT", "CRVUSDT", "COMPUSDT", "MKRUSDT",
    "SNXUSDT"
]))

# ============================================================================
# GLOBAL STATE
# ============================================================================
_lock = threading.RLock()
_kline_lock = threading.RLock()
_rest_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q = queue.Queue()
_hot_syms = deque(maxlen=30)

_precision_cache: Dict[str, int] = {}
_step_cache: Dict[str, float] = {}
_min_qty_cache: Dict[str, float] = {}
_symbol_info_cache: Dict[str, Dict[str, Any]] = {}
_ticker_cache: Dict[str, Dict[str, float]] = {}
_ticker_ts = 0.0
_rest_price_cache: Dict[str, Tuple[float, float]] = {}

_kline_cache: Dict[str, pd.DataFrame] = {}
_ws_mark_price: Dict[str, Tuple[float, float]] = {}
_ws_ticker_cache: Dict[str, Dict[str, float]] = {}
_ws_ticker_ts = 0.0
_ws_last_msg_ts = time.time()

live_positions: Dict[str, Dict[str, Any]] = {}
cooldown_list: Dict[str, float] = {}
trade_log: List[Dict[str, Any]] = []
# Prevent duplicate entries on the same confirmed 5m candle.
_entry_taken_candle: Dict[str, int] = {}

_last_err_print = defaultdict(float)
_api_fail_streak = 0
_api_ok_last = time.time()
_rest_last_ts = 0.0
_rest_block_until = 0.0

_order_state_uncertain = False
_entry_lock_reason = ""

_sl_ban_until = 0.0
_cascade_ban_until = 0.0
_time_limit_ban_until = 0.0
_profit_guard_until = 0.0
_sl_ban_trigger = ""
_cascade_ban_trigger = ""
_time_limit_ban_trigger = ""
_profit_guard_trigger = ""

_ks = {"active": False, "reason": "", "resume": 0.0, "consec": 0, "daily": 0.0, "day_reset": 0.0}

_stats = {
    "trades": 0,
    "wins": 0,
    "losses": 0,
    "pnl": 0.0,
    "best": 0.0,
    "worst": 0.0,
    "ath_equity": 0.0,
    "equity_floor": 0.0,
    "floating": 0.0,
    "tp_exit": 0,
    "sl_exit": 0,
    "profit_lock_exit": 0,
    "time_limit_exit": 0,
    "flip_exit": 0,
    "native_exit": 0,
    "sl_ban_count": 0,
    "cascade_ban_count": 0,
    "time_limit_ban_count": 0,
    "profit_guard_count": 0,
    "cascade_close_count": 0,
    "regime_block": 0,
    "macro_block": 0,
    "bos_block": 0,
    "liquidity_block": 0,
    "correlation_block": 0,
    "ws_block": 0,
    "start": time.time(),
    "hist": deque(maxlen=200),
}

# Macro state
_usdtd_lock = threading.RLock()
_usdtd_state = {
    "signal": "UNKNOWN",  # LONG / SHORT / UNKNOWN; maps to crypto direction
    "score": 0,
    "states": {"1h": "N", "2h": "N", "3h": "N", "4h": "N"},
    "last_update": 0.0,
    "value": 0.0,
    "reason": "NO_DATA",
}
_tv_client = None

# Flash breaker is retained as a safety layer, not as the main direction model.
_btc_tick = deque(maxlen=250)
_btc_breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0}

_equity_peak = 0.0
_profit_guard_in_progress = False

_leverage_done = set()

# ============================================================================
# UTILS / REST
# ============================================================================
def _log_err(tag: str, exc: Exception, cooldown: float = 10.0):
    now = time.time()
    if now - _last_err_print[tag] >= cooldown:
        print(f"  ⚠️ [{tag}] {type(exc).__name__}: {exc}")
        _last_err_print[tag] = now


def _log_warn(tag: str, msg: str, cooldown: float = 10.0):
    now = time.time()
    if now - _last_err_print[tag] >= cooldown:
        print(f"  ⚠️ [{tag}] {msg}")
        _last_err_print[tag] = now


def _api_ok():
    global _api_fail_streak, _api_ok_last
    _api_fail_streak = 0
    _api_ok_last = time.time()


def _api_fail(tag: str):
    global _api_fail_streak
    _api_fail_streak += 1
    if _api_fail_streak in (10, 25, 50, 100) or _api_fail_streak % 250 == 0:
        idle = time.time() - _api_ok_last
        print(f"  🚨 API FAIL STREAK {_api_fail_streak}x (idle {idle:.0f}s) — {tag}")


def _rest_call(tag: str, fn, *args, retries: int = REST_RETRIES, **kwargs):
    global _rest_last_ts, _rest_block_until
    last_exc = None
    for attempt in range(retries + 1):
        try:
            with _rest_lock:
                wait = max(0.0, _rest_block_until - time.time())
                if wait > 0:
                    time.sleep(wait)
                gap = time.time() - _rest_last_ts
                if gap < REST_MIN_INTERVAL:
                    time.sleep(REST_MIN_INTERVAL - gap)
                _rest_last_ts = time.time()
            result = fn(*args, **kwargs)
            _api_ok()
            return result
        except Exception as exc:
            last_exc = exc
            text = str(exc).upper()
            now = time.time()
            if "403" in text or "REQUEST BLOCKED" in text or "CLOUDFRONT" in text:
                _rest_block_until = max(_rest_block_until, now + REST_403_COOLDOWN)
                _api_fail(f"{tag}_403")
                _log_warn("REST_403", f"{tag}: pause {REST_403_COOLDOWN:.0f}s", 30)
                break
            if "418" in text:
                _rest_block_until = max(_rest_block_until, now + REST_418_COOLDOWN)
                _api_fail(f"{tag}_418")
                _log_warn("REST_418", f"{tag}: pause {REST_418_COOLDOWN:.0f}s", 30)
                break
            if "429" in text or "TOO MANY REQUESTS" in text:
                _rest_block_until = max(_rest_block_until, now + REST_429_COOLDOWN)
                _api_fail(f"{tag}_429")
                _log_warn("REST_429", f"{tag}: pause {REST_429_COOLDOWN:.0f}s", 30)
                break
            _api_fail(tag)
            if attempt < retries:
                time.sleep(min(2.0, 0.5 * (2 ** attempt)))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"REST call failed: {tag}")


def _new_client_order_id(prefix: str, sym: str) -> str:
    # <= 32 chars, unique enough for this single-process bot.
    return f"Q24_{prefix}_{sym}_{int(time.time()*1000)%1000000000}"[:32]


def _get_order_fill(order: Dict[str, Any]) -> Tuple[float, float]:
    try:
        q = float(order.get("executedQty", 0) or 0)
        p = float(order.get("avgPrice", 0) or 0)
        if p <= 0:
            cum_quote = float(order.get("cumQuote", 0) or 0)
            if q > 0 and cum_quote > 0:
                p = cum_quote / q
        return q, p
    except Exception:
        return 0.0, 0.0


def _create_market_order_safe(sym: str, side: str, quantity: float, reduce_only: bool = False):
    """Never blindly re-submit a market POST after an ambiguous error."""
    global _order_state_uncertain, _entry_lock_reason
    cid = _new_client_order_id("C" if reduce_only else "O", sym)
    kwargs = {
        "symbol": sym,
        "side": side,
        "type": "MARKET",
        "quantity": quantity,
        "newOrderRespType": "RESULT",
        "newClientOrderId": cid,
    }
    if reduce_only:
        kwargs["reduceOnly"] = True
    try:
        return _rest_call(f"market_{sym}", client.futures_create_order, retries=0, **kwargs)
    except Exception as first_exc:
        msg = str(first_exc).upper()
        if any(x in msg for x in ("403", "418", "429", "CLOUDFRONT")):
            raise
        try:
            found = _rest_call(
                f"verify_{sym}",
                client.futures_get_order,
                symbol=sym,
                origClientOrderId=cid,
                retries=0,
            )
            if found:
                return found
        except Exception:
            pass
        with _lock:
            _order_state_uncertain = True
            _entry_lock_reason = f"ORDER_STATE_UNKNOWN {sym}"
        raise RuntimeError(f"ORDER STATUS UNKNOWN {sym} clientOrderId={cid}: {first_exc}") from first_exc


def _cancel_order_safe(sym: str, order_id: Optional[int]):
    if not order_id:
        return
    try:
        _rest_call(
            f"cancel_{sym}_{order_id}",
            client.futures_cancel_order,
            symbol=sym,
            orderId=order_id,
            retries=0,
        )
    except Exception as exc:
        _log_warn(f"cancel_{sym}", f"orderId={order_id}: {exc}", 15)


def _normalize_symbol_info(info: Dict[str, Any]):
    sym = info.get("symbol")
    if not sym:
        return
    _symbol_info_cache[sym] = info
    _precision_cache[sym] = int(info.get("quantityPrecision", 3) or 3)
    min_qty = 0.0
    step = 0.0
    for f in info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            min_qty = float(f.get("minQty", 0) or 0)
            step = float(f.get("stepSize", 0) or 0)
    _min_qty_cache[sym] = min_qty
    _step_cache[sym] = step


def _round_qty(sym: str, raw: float) -> float:
    if raw <= 0:
        return 0.0
    step = _step_cache.get(sym, 0.0)
    prec = _precision_cache.get(sym, 3)
    if step > 0:
        value = math.floor(raw / step) * step
    else:
        value = round(raw, prec)
    return max(0.0, round(value, prec))


def get_qty(sym: str, price: float, sl_pct: float) -> float:
    if price <= 0 or sl_pct <= 0:
        return 0.0
    # Risk-based notional with hard margin/notional cap.
    notional_by_risk = RISK_USDT_PER_TRADE / sl_pct
    max_notional = MAX_MARGIN_USDT * LEVERAGE
    target_notional = min(notional_by_risk, max_notional)
    raw = target_notional / price
    q = _round_qty(sym, raw)
    min_qty = _min_qty_cache.get(sym, 0.0)
    if min_qty > 0 and q < min_qty:
        q = _round_qty(sym, min_qty)
    return q


def price_live(sym: str) -> float:
    now = time.time()
    cached = _ws_mark_price.get(sym)
    if cached:
        px, ts = cached
        if px > 0 and now - ts <= MARKPRICE_FRESH_SEC:
            return px
    old = _rest_price_cache.get(sym)
    if old and now - old[1] < 1.0:
        return old[0]
    try:
        data = _rest_call(f"price_{sym}", client.futures_symbol_ticker, symbol=sym, retries=0)
        px = float(data.get("price", 0) or 0)
        if px > 0:
            _rest_price_cache[sym] = (px, time.time())
        return px
    except Exception as exc:
        _log_err(f"price_{sym}", exc, 15)
        return 0.0


def tickers_all() -> Dict[str, Dict[str, float]]:
    global _ticker_cache, _ticker_ts
    now = time.time()
    if _ws_ticker_cache and now - _ws_ticker_ts < 15:
        return _ws_ticker_cache
    if _ticker_cache and now - _ticker_ts < 15:
        return _ticker_cache
    try:
        raw = _rest_call("futures_ticker_all", client.futures_ticker, retries=1)
        _ticker_cache = {
            d["symbol"]: {
                "pct": float(d.get("priceChangePercent", 0) or 0),
                "vol": float(d.get("quoteVolume", 0) or 0),
                "last": float(d.get("lastPrice", 0) or 0),
            }
            for d in raw if d.get("symbol")
        }
        _ticker_ts = time.time()
    except Exception as exc:
        _log_err("futures_ticker", exc, 30)
    return _ticker_cache

# ============================================================================
# ORDER BOOK
# ============================================================================
class OrderBookEngine:
    def __init__(self):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.hist: Dict[str, deque] = defaultdict(lambda: deque(maxlen=12))
        self.lock = threading.RLock()

    def update(self, sym: str, bids_raw: list, asks_raw: list, ts: Optional[float] = None):
        try:
            bids = sorted([(float(p), float(q)) for p, q in bids_raw], key=lambda x: x[0], reverse=True)
            asks = sorted([(float(p), float(q)) for p, q in asks_raw], key=lambda x: x[0])
            if not bids or not asks:
                return
            bid_vol = sum(q for _, q in bids)
            ask_vol = sum(q for _, q in asks)
            total = bid_vol + ask_vol
            imb = (bid_vol - ask_vol) / (total + 1e-9)
            now = ts or time.time()
            with self.lock:
                self.cache[sym] = {
                    "bids": bids,
                    "asks": asks,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "imb": imb,
                    "best_bid": bids[0][0],
                    "best_ask": asks[0][0],
                    "ts": now,
                }
                self.hist[sym].append((now, bid_vol, ask_vol))
        except Exception:
            pass

    def get(self, sym: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.cache.get(sym)

    def spread_bps(self, sym: str, price: float) -> float:
        b = self.get(sym)
        if not b or price <= 0:
            return 999.0
        return max(0.0, (b["best_ask"] - b["best_bid"]) / price * 10_000.0)

    def aligned_imbalance(self, sym: str, side: str) -> float:
        b = self.get(sym)
        if not b:
            return 0.0
        return b["imb"] if side == "LONG" else -b["imb"]

    def wall_penalty(self, sym: str, side: str, price: float) -> bool:
        b = self.get(sym)
        if not b or price <= 0:
            return False
        levels = b["asks"] if side == "LONG" else b["bids"]
        total = b["ask_vol"] if side == "LONG" else b["bid_vol"]
        if not levels or total <= 0:
            return False
        avg = total / len(levels)
        for px, qty in levels[:10]:
            dist = abs(px - price) / price
            if dist <= 0.0015 and (qty >= 3.5 * avg or qty >= 0.40 * total):
                return True
        return False

    def spoofing(self, sym: str, side: str) -> bool:
        with self.lock:
            h = list(self.hist.get(sym, []))
        if len(h) < 3:
            return False
        now, bid_now, ask_now = h[-1]
        for ts, bid_old, ask_old in h[:-1]:
            if 0.4 <= now - ts <= 2.5:
                if side == "LONG" and bid_old > 0 and bid_now < bid_old * 0.55:
                    return True
                if side == "SHORT" and ask_old > 0 and ask_now < ask_old * 0.55:
                    return True
        return False


order_book = OrderBookEngine()

# ============================================================================
# INDICATORS / MARKET STRUCTURE
# ============================================================================
def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["open", "high", "low", "close", "volume", "tbbase"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"].replace(0, np.nan).fillna(1e-9)
    tb = df.get("tbbase", volume * 0.5)

    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    macd = ta.trend.MACD(close, 12, 26, 9)
    df["macd"] = macd.macd()
    df["mh"] = macd.macd_diff()
    df["e5"] = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"] = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e20"] = ta.trend.EMAIndicator(close, 20).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()
    df["vm"] = volume.rolling(20).mean()
    df["vr"] = volume / df["vm"].replace(0, np.nan).fillna(1e-9)

    sell = (volume - tb).clip(lower=0)
    delta = tb - sell
    df["delta"] = delta
    df["delta_ratio"] = delta / volume
    df["br"] = tb / volume
    df["cvd"] = delta.rolling(10).sum()

    rng = (high - low).replace(0, np.nan).fillna(1e-9)
    body = (close - df["open"]).abs()
    df["rng"] = rng
    df["body_ratio"] = body / rng
    df["upper_wick_ratio"] = (high - df[["open", "close"]].max(axis=1)) / rng
    df["lower_wick_ratio"] = (df[["open", "close"]].min(axis=1) - low) / rng
    df["m3"] = (close - close.shift(3)) / close.shift(3)
    df["m5"] = (close - close.shift(5)) / close.shift(5)
    return df


def _closed_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) < 20:
        return df
    now_ms = int(time.time() * 1000)
    if "ct" in df.columns:
        out = df[df["ct"] <= now_ms].copy()
        if len(out) >= 20:
            return out
    return df.copy()


def _pivot_levels(df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    d = _closed_df(df)
    if d is None or len(d) < BOS_LOOKBACK:
        return None, None
    d = d.iloc[-BOS_LOOKBACK:]
    highs = []
    lows = []
    n = len(d)
    L = BOS_PIVOT_LEFT
    R = BOS_PIVOT_RIGHT
    for i in range(L, n - R):
        h = float(d["high"].iloc[i])
        l = float(d["low"].iloc[i])
        if h >= max(float(x) for x in d["high"].iloc[i-L:i+R+1]):
            highs.append((i, h))
        if l <= min(float(x) for x in d["low"].iloc[i-L:i+R+1]):
            lows.append((i, l))
    # Only use pivots that occur before the current closed candle.
    max_idx = n - 2
    highs = [x for x in highs if x[0] <= max_idx]
    lows = [x for x in lows if x[0] <= max_idx]
    sh = highs[-1][1] if highs else None
    sl = lows[-1][1] if lows else None
    return sh, sl


def detect_recent_bos(df: pd.DataFrame, side: str) -> Dict[str, Any]:
    d = _closed_df(df)
    if d is None or len(d) < BOS_LOOKBACK:
        return {"ok": False, "level": 0.0, "age": 999, "reason": "NO_DATA"}
    d = d.iloc[-BOS_LOOKBACK:].reset_index(drop=True)
    atr = float(d["atr"].iloc[-1]) if np.isfinite(d["atr"].iloc[-1]) else 0.0
    sh, sl = _pivot_levels(d)
    if atr <= 0:
        return {"ok": False, "level": 0.0, "age": 999, "reason": "ATR"}

    current_idx = len(d) - 1
    candidates = []
    for age in range(0, BOS_MAX_AGE_CANDLES):
        idx = current_idx - age
        if idx < 1:
            continue
        row = d.iloc[idx]
        prev = d.iloc[idx-1]
        if side == "LONG" and sh is not None:
            crossed = float(row["close"]) > sh + BOS_ATR_BUFFER * atr and float(prev["close"]) <= sh + BOS_ATR_BUFFER * atr
            body_ok = float(row["body_ratio"]) >= 0.45
            if crossed and body_ok:
                # Acceptance: latest candle still above the broken level.
                latest_ok = float(d.iloc[-1]["close"]) >= sh
                candidates.append((age, sh, latest_ok, float(row["vr"])))
        if side == "SHORT" and sl is not None:
            crossed = float(row["close"]) < sl - BOS_ATR_BUFFER * atr and float(prev["close"]) >= sl - BOS_ATR_BUFFER * atr
            body_ok = float(row["body_ratio"]) >= 0.45
            if crossed and body_ok:
                latest_ok = float(d.iloc[-1]["close"]) <= sl
                candidates.append((age, sl, latest_ok, float(row["vr"])))

    if not candidates:
        return {"ok": False, "level": 0.0, "age": 999, "reason": "NO_BOS"}
    age, level, accepted, vr = sorted(candidates, key=lambda x: x[0])[0]
    if not accepted:
        return {"ok": False, "level": level, "age": age, "reason": "NO_ACCEPTANCE"}
    return {"ok": True, "level": level, "age": age, "reason": "BOS_CONFIRMED", "vr": vr}


def regime_from_df(df: pd.DataFrame) -> Tuple[str, float]:
    d = _closed_df(df)
    if d is None or len(d) < 60:
        return "UNKNOWN", 0.0
    r = d.iloc[-1]
    bull = r["close"] > r["e20"] > r["e50"]
    bear = r["close"] < r["e20"] < r["e50"]
    adx = float(r["adx"])
    if adx >= 30 and bull:
        return "TRENDING_BULL", adx
    if adx >= 30 and bear:
        return "TRENDING_BEAR", adx
    if adx >= ADX_MIN and bull:
        return "BULL", adx
    if adx >= ADX_MIN and bear:
        return "BEAR", adx
    return "RANGE", adx

# ============================================================================
# KLINES / DATA
# ============================================================================
def _bootstrap_klines(sym: str, interval: str, limit: int = 160) -> Optional[pd.DataFrame]:
    try:
        kl = _rest_call(
            f"bootstrap_{sym}",
            client.futures_klines,
            symbol=sym,
            interval=interval,
            limit=limit,
        )
        cols = ["time", "open", "high", "low", "close", "volume", "ct", "qv", "trades", "tbbase", "tbquote", "ignore"]
        df = pd.DataFrame(kl, columns=cols)
        for c in ["open", "high", "low", "close", "volume", "tbbase", "tbquote"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["time"] = pd.to_numeric(df["time"], errors="coerce")
        df["ct"] = pd.to_numeric(df["ct"], errors="coerce")
        df = _compute_indicators(df)
        with _kline_lock:
            _kline_cache[sym] = df
        return df
    except Exception as exc:
        _log_err(f"bootstrap_{sym}", exc, 30)
        return None


def _append_closed_kline(sym: str, k: Dict[str, Any]):
    try:
        row = {
            "time": int(k["t"]),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "ct": int(k["T"]),
            "qv": float(k.get("q", 0)),
            "trades": int(k.get("n", 0)),
            "tbbase": float(k.get("V", 0)),
            "tbquote": float(k.get("Q", 0)),
            "ignore": 0,
        }
        with _kline_lock:
            df = _kline_cache.get(sym)
            if df is None:
                return
            base_cols = ["time", "open", "high", "low", "close", "volume", "ct", "qv", "trades", "tbbase", "tbquote", "ignore"]
            base = df[base_cols].copy()
            if len(base) and int(base.iloc[-1]["time"]) == row["time"]:
                base = base.iloc[:-1]
            base = pd.concat([base, pd.DataFrame([row])], ignore_index=True).tail(300)
            _kline_cache[sym] = _compute_indicators(base.reset_index(drop=True))
    except Exception as exc:
        _log_err(f"append_kline_{sym}", exc, 30)


def ohlcv(sym: str, interval: str = Client.KLINE_INTERVAL_5MINUTE, limit: int = 160) -> Optional[pd.DataFrame]:
    with _kline_lock:
        df = _kline_cache.get(sym)
    if df is None:
        return _bootstrap_klines(sym, interval, limit)
    return df

# ============================================================================
# USDT.D MTF ENGINE
# ============================================================================
class USDTDMTFEngine:
    def __init__(self):
        self.tv = None
        self.ready = False
        self.lock = _usdtd_lock

    def _get_tv(self):
        if self.tv is not None:
            return self.tv
        if not TV_AVAILABLE:
            raise RuntimeError("tvdatafeed-enhanced tidak terpasang")
        user = os.getenv("TV_USERNAME")
        pwd = os.getenv("TV_PASSWORD")
        # No-login is supported by tvdatafeed-enhanced, but some symbols may be limited.
        try:
            if user and pwd:
                self.tv = TvDatafeed(user, pwd)
            else:
                self.tv = TvDatafeed()
            return self.tv
        except Exception as exc:
            self.tv = None
            raise RuntimeError(f"TradingView init gagal: {exc}") from exc

    @staticmethod
    def _classify(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < USDTD_MIN_BARS:
            return "N", 0.0, 0.0
        d = df.copy()
        for c in ["open", "high", "low", "close"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")
        close = d["close"]
        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        r = close.iloc[-1]
        s = (close.iloc[-8] and (r - close.iloc[-8]) / close.iloc[-8]) if close.iloc[-8] else 0.0
        bull = r > ema20.iloc[-1] > ema50.iloc[-1] and s > 0.0008
        bear = r < ema20.iloc[-1] < ema50.iloc[-1] and s < -0.0008
        if bull:
            return "B", float(r), float(s)
        if bear:
            return "S", float(r), float(s)
        # weaker slope-only state is neutral, by design.
        return "N", float(r), float(s)

    def update(self):
        global _usdtd_state
        try:
            tv = self._get_tv()
            states = {}
            values = []
            slopes = []
            for label, attr in TV_INTERVALS:
                iv = getattr(Interval, attr)
                df = tv.get_hist(
                    symbol=TV_SYMBOL,
                    exchange=TV_EXCHANGE,
                    interval=iv,
                    n_bars=140,
                )
                st, value, slope = self._classify(df)
                states[label] = st
                if value:
                    values.append(value)
                slopes.append(slope)

            bulls = sum(v == "B" for v in states.values())
            bears = sum(v == "S" for v in states.values())
            if bulls >= USDTD_REQUIRE_ALIGNMENT and bears == 0:
                signal = "SHORT"
                score = 25 + bulls * 5
                reason = f"USDT.D bullish {bulls}/4 -> crypto SHORT"
            elif bears >= USDTD_REQUIRE_ALIGNMENT and bulls == 0:
                signal = "LONG"
                score = 25 + bears * 5
                reason = f"USDT.D bearish {bears}/4 -> crypto LONG"
            else:
                signal = "UNKNOWN"
                score = 0
                reason = f"USDT.D mixed: bull={bulls}/4 bear={bears}/4"
            with self.lock:
                _usdtd_state = {
                    "signal": signal,
                    "score": score,
                    "states": states,
                    "last_update": time.time(),
                    "value": values[-1] if values else 0.0,
                    "reason": reason,
                }
                self.ready = signal in ("LONG", "SHORT")
        except Exception as exc:
            with self.lock:
                _usdtd_state["signal"] = "UNKNOWN"
                _usdtd_state["reason"] = f"USDT.D update error: {exc}"
                _usdtd_state["last_update"] = time.time()
            _log_err("usdt_d", exc, 60)

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return dict(_usdtd_state)


usdtd = USDTDMTFEngine()

# ============================================================================
# MACRO BTC FLASH SAFETY
# ============================================================================
def _update_btc_tick(price: float, ts: Optional[float] = None):
    global _btc_breaker
    now = ts or time.time()
    _btc_tick.append((now, price))
    cutoff = now - 8.0
    baseline = None
    for t, p in _btc_tick:
        if t >= cutoff:
            baseline = p
            break
    if not baseline or baseline <= 0:
        return
    delta = (price - baseline) / baseline
    if delta <= -0.003:
        _btc_breaker = {"active": True, "type": "CRASH", "until": now + 120, "delta": delta}
    elif delta >= 0.003:
        _btc_breaker = {"active": True, "type": "PUMP", "until": now + 120, "delta": delta}


def _btc_veto(side: str) -> bool:
    now = time.time()
    if _btc_breaker["active"] and now < _btc_breaker["until"]:
        if _btc_breaker["type"] == "CRASH" and side == "LONG":
            return True
        if _btc_breaker["type"] == "PUMP" and side == "SHORT":
            return True
    return False

# ============================================================================
# RISK / EQUITY
# ============================================================================
class RiskManager:
    @staticmethod
    def levels(entry: float, side: str, atr: float, regime: str) -> Dict[str, float]:
        atr_pct = atr / entry if entry > 0 else 0.015
        tp = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_MULT * atr_pct))
        sl = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * atr_pct))
        if regime == "TRENDING_BULL" or regime == "TRENDING_BEAR":
            tp = min(MAX_TP_PCT, tp * 1.05)
            sl = min(MAX_SL_PCT, max(MIN_SL_PCT, sl * 0.98))
        if side == "LONG":
            tp_price = entry * (1 + tp)
            sl_price = entry * (1 - sl)
        else:
            tp_price = entry * (1 - tp)
            sl_price = entry * (1 + sl)
        return {
            "tp_pct": tp,
            "sl_pct": sl,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "atr_pct": atr_pct,
        }


def estimate_pnl(pos: Dict[str, Any], price: float) -> float:
    try:
        entry = float(pos["entry"])
        qty = float(pos["qty"])
        side = pos["side"]
        gross = (price - entry) * qty if side == "LONG" else (entry - price) * qty
        fee_rate = 0.0005
        fees = (entry * qty + price * qty) * fee_rate
        return gross - fees
    except Exception:
        return 0.0


def current_equity() -> Tuple[float, float]:
    floating = 0.0
    for sym, pos in list(live_positions.items()):
        if pos.get("_r"):
            continue
        px = _mark_price_only(sym) or price_live(sym)
        if px > 0:
            floating += estimate_pnl(pos, px)
    _stats["floating"] = floating
    return _stats["pnl"] + floating, floating


def _mark_price_only(sym: str) -> float:
    cached = _ws_mark_price.get(sym)
    if cached:
        px, ts = cached
        if px > 0 and time.time() - ts <= MARKPRICE_FRESH_SEC:
            return px
    return 0.0


def equity_floor_for_peak(peak: float) -> float:
    if peak < EQUITY_GUARD_ARM:
        return -999.0
    if peak < 2.0:
        return max(0.40, peak * 0.50)
    if peak < 5.0:
        return peak * 0.65
    if peak < 10.0:
        return peak * 0.75
    return peak * 0.80


def _activate_ban(kind: str, seconds: int, trigger: str):
    global _sl_ban_until, _cascade_ban_until, _time_limit_ban_until, _profit_guard_until
    global _sl_ban_trigger, _cascade_ban_trigger, _time_limit_ban_trigger, _profit_guard_trigger
    until = time.time() + seconds
    with _lock:
        if kind == "SL":
            _sl_ban_until = max(_sl_ban_until, until)
            _sl_ban_trigger = trigger
            _stats["sl_ban_count"] += 1
        elif kind == "CASCADE":
            _cascade_ban_until = max(_cascade_ban_until, until)
            _cascade_ban_trigger = trigger
            _stats["cascade_ban_count"] += 1
        elif kind == "TIME":
            _time_limit_ban_until = max(_time_limit_ban_until, until)
            _time_limit_ban_trigger = trigger
            _stats["time_limit_ban_count"] += 1
        elif kind == "PROFIT":
            _profit_guard_until = max(_profit_guard_until, until)
            _profit_guard_trigger = trigger
            _stats["profit_guard_count"] += 1


def _circuit_snapshot() -> Tuple[str, float]:
    now = time.time()
    items = [
        ("SL_BAN", max(0.0, _sl_ban_until - now)),
        ("CASCADE_BAN", max(0.0, _cascade_ban_until - now)),
        ("TIME_BAN", max(0.0, _time_limit_ban_until - now)),
        ("PROFIT_GUARD", max(0.0, _profit_guard_until - now)),
    ]
    active = [(n, r) for n, r in items if r > 0]
    if not active:
        return "", 0.0
    n, r = max(active, key=lambda x: x[1])
    h = int(r // 3600)
    m = int((r % 3600) // 60)
    s = int(r % 60)
    return f"{n} {h:02d}:{m:02d}:{s:02d}", r


def _close_losing_positions(reason: str, exclude: Optional[set] = None):
    exclude = set(exclude or [])
    candidates = []
    for sym, pos in list(live_positions.items()):
        if sym in exclude or pos.get("_r"):
            continue
        px = _mark_price_only(sym) or price_live(sym)
        if px <= 0:
            continue
        pnl = estimate_pnl(pos, px)
        if pnl < 0:
            candidates.append((pnl, sym, px))
    candidates.sort(key=lambda x: x[0])
    for pnl, sym, px in candidates:
        live_close(sym, reason, px)
        _stats["cascade_close_count"] += 1


def _maybe_profit_guard():
    global _equity_peak, _profit_guard_in_progress
    if not EQUITY_GUARD_ENABLED or _profit_guard_in_progress:
        return
    equity, _ = current_equity()
    if equity > _equity_peak:
        _equity_peak = equity
    _stats["ath_equity"] = max(_stats["ath_equity"], _equity_peak)
    floor = equity_floor_for_peak(_equity_peak)
    _stats["equity_floor"] = floor if floor > -900 else 0.0
    if floor <= -900 or equity > floor:
        return
    _profit_guard_in_progress = True
    try:
        _activate_ban("PROFIT", EQUITY_GUARD_BAN_SECONDS, "EQUITY_GIVEBACK")
        print(f"  🧱 [EQUITY GUARD] equity:{equity:+.4f}U peak:{_equity_peak:+.4f}U floor:{floor:+.4f}U")
        if EQUITY_GUARD_LOCK_LOSERS_FIRST:
            _close_losing_positions("EQUITY_GUARD")
        equity_after, _ = current_equity()
        if EQUITY_GUARD_CLOSE_ALL_IF_BROKEN and equity_after <= floor:
            for sym in list(live_positions.keys()):
                if sym in live_positions and not live_positions[sym].get("_r"):
                    live_close(sym, "EQUITY_GUARD_FINAL")
    finally:
        _profit_guard_in_progress = False

# ============================================================================
# TRADE RECORD / LEARNING (OBSERVATIONAL ONLY)
# ============================================================================
@dataclass
class TradeRecord:
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    pnl: float
    won: bool
    regime: str
    score: float
    signals: List[str]
    hold_seconds: float
    exit_reason: str
    peak_r: float
    timestamp: float = field(default_factory=time.time)


class LearningLayer:
    def __init__(self):
        self.trades: List[TradeRecord] = []
        self.by_regime = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
        self.by_symbol = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})

    def add(self, t: TradeRecord):
        self.trades.append(t)
        bucket = self.by_regime[t.regime]
        bucket["w"] += int(t.won)
        bucket["l"] += int(not t.won)
        bucket["pnl"] += t.pnl
        sb = self.by_symbol[t.symbol]
        sb["w"] += int(t.won)
        sb["l"] += int(not t.won)
        sb["pnl"] += t.pnl
        if len(self.trades) > 1000:
            self.trades = self.trades[-500:]

    def avg_win(self):
        x = [t.pnl for t in self.trades if t.won]
        return sum(x) / len(x) if x else 0.0

    def avg_loss(self):
        x = [abs(t.pnl) for t in self.trades if not t.won]
        return sum(x) / len(x) if x else 0.0


learning = LearningLayer()

# ============================================================================
# POSITION / NATIVE PROTECTIVE ORDERS
# ============================================================================
def _place_native_protection(sym: str, pos: Dict[str, Any]) -> bool:
    if not USE_NATIVE_PROTECTIVE_ORDERS:
        return False
    sl_id = None
    tp_id = None
    try:
        side = pos["side"]
        exit_side = "SELL" if side == "LONG" else "BUY"
        common = {
            "symbol": sym,
            "side": exit_side,
            "reduceOnly": True,
            "workingType": NATIVE_ORDER_WORKING_TYPE,
            "priceProtect": True,
        }
        sl = _rest_call(
            f"native_sl_{sym}",
            client.futures_create_order,
            type="STOP_MARKET",
            stopPrice=pos["sl_price"],
            quantity=pos["qty"],
            newClientOrderId=_new_client_order_id("S", sym),
            newOrderRespType="RESULT",
            retries=0,
            **common,
        )
        sl_id = sl.get("orderId")
        tp = _rest_call(
            f"native_tp_{sym}",
            client.futures_create_order,
            type="TAKE_PROFIT_MARKET",
            stopPrice=pos["tp_price"],
            quantity=pos["qty"],
            newClientOrderId=_new_client_order_id("T", sym),
            newOrderRespType="RESULT",
            retries=0,
            **common,
        )
        tp_id = tp.get("orderId")
        pos["native_sl_id"] = sl_id
        pos["native_tp_id"] = tp_id
        pos["native_protection"] = bool(sl_id and tp_id)
        if not pos["native_protection"]:
            raise RuntimeError("native SL/TP orderId tidak lengkap")
        return True
    except Exception as exc:
        pos["native_protection"] = False
        pos["native_sl_id"] = None
        pos["native_tp_id"] = None
        _log_warn(f"native_protection_{sym}", f"fallback local SL/TP: {exc}", 30)
        # If only one order landed, cancel it before falling back to local checks.
        _cancel_order_safe(sym, sl_id)
        _cancel_order_safe(sym, tp_id)
        return False


def _cancel_native_protection(sym: str, pos: Dict[str, Any]):
    _cancel_order_safe(sym, pos.get("native_sl_id"))
    _cancel_order_safe(sym, pos.get("native_tp_id"))
    pos["native_sl_id"] = None
    pos["native_tp_id"] = None


def _finalize_closed_position(sym: str, reason: str, price: float, filled_qty: Optional[float] = None, native: bool = False):
    with _lock:
        pos = live_positions.get(sym)
        if not pos or pos.get("_r"):
            return
        qty = float(pos.get("qty", 0) or 0)
        close_qty = float(filled_qty or qty)
        if close_qty <= 0:
            close_qty = qty
        # Native orders should normally close the whole position. For a partial fill,
        # only reduce local qty and keep monitoring the remainder.
        if close_qty + 1e-12 < qty:
            pos["qty"] = max(0.0, qty - close_qty)
            if native:
                print(f"  📡 [{reason}] {sym} partial close {close_qty:.8g}/{qty:.8g}")
                return
        live_positions.pop(sym, None)

    entry = float(pos["entry"])
    side = pos["side"]
    gross = (price - entry) * close_qty if side == "LONG" else (entry - price) * close_qty
    fees = (entry * close_qty + price * close_qty) * 0.0005
    pnl = gross - fees
    hold = time.time() - float(pos["open_time"])
    won = pnl >= 0
    peak_pnl = float(pos.get("peak_pnl", 0.0))
    risk_u = max(float(pos.get("risk_u", 0.0)), 1e-9)
    peak_r = peak_pnl / risk_u

    learning.add(TradeRecord(
        symbol=sym,
        direction=side,
        entry_price=entry,
        exit_price=price,
        pnl=pnl,
        won=won,
        regime=pos.get("regime", "UNKNOWN"),
        score=float(pos.get("score", 0)),
        signals=pos.get("signals", []),
        hold_seconds=hold,
        exit_reason=reason,
        peak_r=peak_r,
    ))

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    _stats["wins"] += int(won)
    _stats["losses"] += int(not won)
    _stats["best"] = max(_stats["best"], pnl)
    _stats["worst"] = min(_stats["worst"], pnl)
    _stats["trades"] += 1
    if reason == "TP":
        _stats["tp_exit"] += 1
    elif reason == "SL":
        _stats["sl_exit"] += 1
    elif reason == "PROFIT_LOCK":
        _stats["profit_lock_exit"] += 1
    elif reason.startswith("TIME_LIMIT"):
        _stats["time_limit_exit"] += 1
    elif reason == "SIGNAL_FLIP":
        _stats["flip_exit"] += 1
    elif native:
        _stats["native_exit"] += 1

    trade_log.append({
        "sym": sym,
        "side": side,
        "pnl": round(pnl, 5),
        "reason": reason,
        "hold": int(hold),
        "peak_r": round(peak_r, 2),
    })
    if len(trade_log) > 100:
        del trade_log[:-100]

    if reason == "SL":
        _activate_ban("SL", SL_BAN_SECONDS, sym)
        _close_losing_positions("CASCADE_AFTER_SL", exclude={sym})
        if _stats["cascade_close_count"]:
            _activate_ban("CASCADE", CASCADE_BAN_SECONDS, sym)
    elif reason.startswith("TIME_LIMIT") and pnl < 0:
        _activate_ban("TIME", TIME_LIMIT_LOSS_BAN_SECONDS, sym)

    with _lock:
        cooldown_list[sym] = time.time() + COOLDOWN_SEC
        _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    _maybe_profit_guard()

    icon = "🟢" if won else "🔴"
    print(f"  {icon} CLOSE {sym} {side} — {reason} | PnL:{pnl:+.5f}U | hold:{hold:.0f}s | peak:{peak_r:.2f}R")


def live_close(sym: str, reason: str, price: Optional[float] = None):
    with _lock:
        pos = live_positions.get(sym)
    if not pos or pos.get("_r"):
        return
    _cancel_native_protection(sym, pos)
    px = float(price or price_live(sym) or pos["entry"])
    side = pos["side"]
    exit_side = "SELL" if side == "LONG" else "BUY"
    qty = float(pos.get("qty", 0) or 0)
    if qty <= 0:
        return
    try:
        order = _create_market_order_safe(sym, exit_side, qty, reduce_only=True)
        filled, fill_px = _get_order_fill(order)
        if fill_px > 0:
            px = fill_px
        if filled <= 0:
            filled = qty
        _finalize_closed_position(sym, reason, px, filled_qty=filled, native=False)
    except Exception as exc:
        _log_err(f"close_{sym}", exc, 5)
        # Put local position back; entry should remain locked until reconciliation.
        with _lock:
            if sym not in live_positions:
                live_positions[sym] = pos
        return


def _update_profit_lock(sym: str, pos: Dict[str, Any], price: float) -> bool:
    if not PROFIT_LOCK_ENABLED:
        return False
    pnl = estimate_pnl(pos, price)
    pos["peak_pnl"] = max(float(pos.get("peak_pnl", 0.0)), pnl)
    risk_u = max(float(pos.get("risk_u", 0.0)), 1e-9)
    peak_r = pos["peak_pnl"] / risk_u
    lock_r = None
    label = ""
    for trigger_r, target_r, name in PROFIT_LOCK_LEVELS:
        if peak_r >= trigger_r:
            lock_r = target_r
            label = name
    if lock_r is None:
        return False

    target_pnl = lock_r * risk_u
    # Add a small fee cushion for BE / positive locking.
    fee_cushion = max(0.0005 * float(pos["entry"]) * float(pos["qty"]) * 2, 0.01)
    if lock_r == 0.0:
        target_pnl = fee_cushion

    move = target_pnl / max(float(pos["qty"]), 1e-9)
    entry = float(pos["entry"])
    if pos["side"] == "LONG":
        proposed = entry + move
        current = float(pos.get("guard_stop_price", pos["sl_price"]))
        new_stop = max(current, proposed)
    else:
        proposed = entry - move
        current = float(pos.get("guard_stop_price", pos["sl_price"]))
        new_stop = min(current, proposed)
    changed = abs(new_stop - float(pos.get("guard_stop_price", pos["sl_price"]))) > 0
    pos["guard_stop_price"] = new_stop
    pos["lock_label"] = label
    return changed

# ============================================================================
# ENTRY SCORER
# ============================================================================
def score_entry(sym: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    d = _closed_df(df)
    if d is None or len(d) < 65:
        return None
    row = d.iloc[-1]
    atr = float(row["atr"])
    if not np.isfinite(atr) or atr <= 0:
        return None

    macro = usdtd.snapshot()
    macro_side = macro.get("signal")
    if macro_side not in ("LONG", "SHORT"):
        _stats["macro_block"] += 1
        return None

    regime, adx = regime_from_df(d)
    if regime in ("RANGE", "UNKNOWN") or adx < ADX_MIN:
        _stats["regime_block"] += 1
        return None

    execution_side = macro_side
    bos = detect_recent_bos(d, execution_side)
    if not bos.get("ok"):
        _stats["bos_block"] += 1
        return None

    close = float(row["close"])
    e20 = float(row["e20"])
    e50 = float(row["e50"])
    rsi = float(row["rsi"])
    vr = float(row["vr"])
    delta_ratio = float(row["delta_ratio"])

    aligned_trend = close > e20 > e50 if execution_side == "LONG" else close < e20 < e50
    if not aligned_trend:
        return None

    # Do NOT reward RSI extremes. They are treated as late-entry risk.
    if execution_side == "LONG" and not (48 <= rsi <= 67):
        return None
    if execution_side == "SHORT" and not (33 <= rsi <= 52):
        return None

    if vr < MIN_VOLUME_RATIO:
        _stats["liquidity_block"] += 1
        return None

    tickers = tickers_all()
    tv = tickers.get(sym, {})
    if float(tv.get("vol", 0)) < MIN_24H_QUOTE_VOL:
        _stats["liquidity_block"] += 1
        return None

    px_live = price_live(sym)
    if px_live <= 0:
        return None
    spread = order_book.spread_bps(sym, px_live)
    if spread > MAX_SPREAD_BPS:
        _stats["liquidity_block"] += 1
        return None

    if _btc_veto(execution_side):
        return None

    score = 0.0
    signals = []
    # Mandatory pillars dominate the score.
    score += 40
    signals.append(f"BOS_{'BULL' if execution_side=='LONG' else 'BEAR'}[{bos['age']}]")
    score += min(40, float(macro.get("score", 0)))
    signals.append(f"USDTD_{macro_side}[{macro.get('score',0)}]")

    if aligned_trend:
        score += 10
        signals.append("EMA20/50_ALIGNED[10]")
    if adx >= 30:
        score += 10
        signals.append(f"ADX{adx:.0f}[10]")
    elif adx >= 22:
        score += 6
        signals.append(f"ADX{adx:.0f}[6]")
    else:
        score += 3
        signals.append(f"ADX{adx:.0f}[3]")

    if vr >= 1.5:
        score += 5
        signals.append(f"VOL{vr:.1f}x[5]")
    elif vr >= 1.1:
        score += 3
        signals.append(f"VOL{vr:.1f}x[3]")
    else:
        signals.append(f"VOL{vr:.1f}x[0]")

    flow_ok = delta_ratio > 0.05 if execution_side == "LONG" else delta_ratio < -0.05
    if flow_ok:
        score += 5
        signals.append(f"FLOW{delta_ratio:+.2f}[5]")

    imb = order_book.aligned_imbalance(sym, execution_side)
    if imb > 0.20:
        score += 5
        signals.append(f"BOOK{imb:+.2f}[5]")
    elif imb < -0.25:
        score -= 8
        signals.append(f"BOOK_AGAINST{imb:+.2f}[-8]")

    if order_book.wall_penalty(sym, execution_side, px_live):
        score -= 6
        signals.append("WALL[-6]")
    if order_book.spoofing(sym, execution_side):
        score -= 10
        signals.append("SPOOF[-10]")

    if score < MIN_ENTRY_SCORE:
        return None

    # Correlation filter against currently open positions.
    with _kline_lock:
        candidate = _kline_cache.get(sym)
    candidate_returns = None
    if candidate is not None and len(candidate) >= 35:
        candidate_returns = _closed_df(candidate)["close"].pct_change().tail(30).dropna()
    if candidate_returns is not None and len(candidate_returns) >= 20:
        for open_sym, pos in list(live_positions.items()):
            if pos.get("_r"):
                continue
            if pos.get("side") != execution_side:
                continue
            with _kline_lock:
                other = _kline_cache.get(open_sym)
            if other is None:
                continue
            other_ret = _closed_df(other)["close"].pct_change().tail(30).dropna()
            m = min(len(candidate_returns), len(other_ret))
            if m < 20:
                continue
            corr = float(np.corrcoef(candidate_returns.tail(m), other_ret.tail(m))[0, 1])
            if np.isfinite(corr) and corr >= CORRELATION_BLOCK:
                _stats["correlation_block"] += 1
                return None

    risk = RiskManager.levels(px_live, execution_side, atr, regime)
    quantity = get_qty(sym, px_live, risk["sl_pct"])
    if quantity <= 0:
        return None

    candle_key = int(row["time"])
    with _lock:
        if _entry_taken_candle.get(sym) == candle_key:
            return None

    return {
        "sym": sym,
        "side": execution_side,
        "candle_key": candle_key,
        "score": score,
        "signals": signals,
        "price": px_live,
        "atr": atr,
        "regime": regime,
        "bias": 1 if execution_side == "LONG" else -1,
        "risk": risk,
        "qty": quantity,
        "macro": macro,
        "bos": bos,
    }

# ============================================================================
# ENTRY / MONITOR
# ============================================================================
def live_open(candidate: Dict[str, Any]) -> bool:
    global _order_state_uncertain
    sym = candidate["sym"]
    side = candidate["side"]
    candle_key = int(candidate.get("candle_key", 0) or 0)
    with _lock:
        if sym in live_positions or len([p for p in live_positions.values() if not p.get("_r")]) >= MAX_POSITIONS:
            return False
        # One entry per symbol per confirmed 5m candle; reserve atomically.
        if candle_key and _entry_taken_candle.get(sym) == candle_key:
            return False
        if candle_key:
            _entry_taken_candle[sym] = candle_key
        # Reserve symbol while order is being created.
        live_positions[sym] = {"_r": True}

    if _order_state_uncertain:
        with _lock:
            live_positions.pop(sym, None)
        return False

    px = price_live(sym)
    if px <= 0:
        with _lock:
            live_positions.pop(sym, None)
        return False

    risk = RiskManager.levels(px, side, candidate["atr"], candidate["regime"])
    qty = get_qty(sym, px, risk["sl_pct"])
    if qty <= 0:
        with _lock:
            live_positions.pop(sym, None)
        return False

    pos = {
        "side": side,
        "entry": px,
        "qty": qty,
        "open_time": time.time(),
        "score": candidate["score"],
        "signals": candidate["signals"],
        "regime": candidate["regime"],
        "atr": candidate["atr"],
        "tp_pct": risk["tp_pct"],
        "sl_pct": risk["sl_pct"],
        "tp_price": risk["tp_price"],
        "sl_price": risk["sl_price"],
        "guard_stop_price": risk["sl_price"],
        "peak_price": px,
        "peak_pnl": 0.0,
        "risk_u": abs(px - risk["sl_price"]) * qty,
        "native_protection": False,
        "native_sl_id": None,
        "native_tp_id": None,
        "lock_label": "NONE",
        "_flip_candidate": None,
        "_flip_count": 0,
    }
    with _lock:
        live_positions[sym] = pos

    if sym not in _leverage_done:
        try:
            _rest_call(
                f"lev_{sym}",
                client.futures_change_leverage,
                symbol=sym,
                leverage=LEVERAGE,
                retries=0,
            )
            _leverage_done.add(sym)
        except Exception as exc:
            _log_err(f"leverage_{sym}", exc, 30)
            with _lock:
                live_positions.pop(sym, None)
            return False

    try:
        order = _create_market_order_safe(
            sym,
            "BUY" if side == "LONG" else "SELL",
            qty,
            reduce_only=False,
        )
        filled, fill_px = _get_order_fill(order)
        if filled <= 0:
            raise RuntimeError(f"order not filled: {order.get('status')}")
        if fill_px > 0:
            px = fill_px
        risk = RiskManager.levels(px, side, candidate["atr"], candidate["regime"])
        with _lock:
            cur = live_positions.get(sym)
            if cur:
                cur.update({
                    "entry": px,
                    "qty": filled,
                    "tp_pct": risk["tp_pct"],
                    "sl_pct": risk["sl_pct"],
                    "tp_price": risk["tp_price"],
                    "sl_price": risk["sl_price"],
                    "guard_stop_price": risk["sl_price"],
                    "risk_u": abs(px - risk["sl_price"]) * filled,
                })
                pos = cur
        if USE_NATIVE_PROTECTIVE_ORDERS:
            _place_native_protection(sym, pos)
        print(f"\n  ✅ ENTRY {sym} {side} @{px:.6g} qty:{filled:.8g} score:{candidate['score']:.0f} | TP:{risk['tp_pct']*100:.2f}% SL:{risk['sl_pct']*100:.2f}%")
        print(f"         Macro:{candidate['macro']['states']} | BOS age:{candidate['bos']['age']} | {' | '.join(candidate['signals'][:8])}")
        return True
    except Exception as exc:
        _log_err(f"entry_{sym}", exc, 10)
        with _lock:
            live_positions.pop(sym, None)
        return False


def _check_time_limit(sym: str, pos: Dict[str, Any], px: float) -> bool:
    hold = time.time() - pos["open_time"]
    pnl = estimate_pnl(pos, px)
    peak_pnl = float(pos.get("peak_pnl", 0.0))
    risk_u = max(float(pos.get("risk_u", 0.0)), 1e-9)
    peak_r = peak_pnl / risk_u

    # Early stale-loss exit: only when genuinely negative and failing to make progress.
    if hold >= EARLY_STALE_SECONDS and hold < MAX_HOLD_SECONDS:
        if pnl < -0.05 and peak_r < 0.40:
            live_close(sym, "TIME_LIMIT_LOSS_EARLY", px)
            return True

    if hold >= MAX_HOLD_SECONDS:
        if pnl < -0.05:
            live_close(sym, "TIME_LIMIT_LOSS", px)
        else:
            live_close(sym, "TIME_LIMIT_PROFIT", px)
        return True
    return False


def _check_signal_flip(sym: str, pos: Dict[str, Any]):
    # This v24 version only flips after a fresh closed 5m BOS in the opposite direction,
    # avoiding the old one-candle indicator-flip behavior.
    with _kline_lock:
        df = _kline_cache.get(sym)
    if df is None:
        return
    d = _closed_df(df)
    if d is None or len(d) < 65:
        return
    candle_key = int(d.iloc[-1]["time"])
    if pos.get("_flip_checked") == candle_key:
        return
    pos["_flip_checked"] = candle_key
    opposite = "SHORT" if pos["side"] == "LONG" else "LONG"
    bos = detect_recent_bos(d, opposite)
    if not bos.get("ok"):
        pos["_flip_count"] = 0
        return
    macro = usdtd.snapshot()
    if macro.get("signal") != opposite:
        pos["flip_candidate"] = None
        pos["_flip_count"] = 0
        return
    pos["_flip_count"] = int(pos.get("_flip_count", 0)) + 1
    if pos["_flip_count"] >= 1:
        _stats["flip_exit"] += 1
        live_close(sym, "SIGNAL_FLIP", price_live(sym))


def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if not pos or pos.get("_r"):
            continue
        px = _mark_price_only(sym) or price_live(sym)
        if px <= 0:
            continue

        side = pos["side"]
        pos["peak_price"] = max(pos.get("peak_price", px), px) if side == "LONG" else min(pos.get("peak_price", px), px)
        pos["peak_pnl"] = max(pos.get("peak_pnl", 0.0), estimate_pnl(pos, px))
        _update_profit_lock(sym, pos, px)

        # Native hard SL/TP are preferred when both are live.
        native = bool(pos.get("native_protection"))
        guard_stop = float(pos.get("guard_stop_price", pos["sl_price"]))

        if side == "LONG":
            if px >= pos["tp_price"] and not native:
                live_close(sym, "TP", px)
                continue
            if px <= pos["sl_price"] and not native:
                live_close(sym, "SL", px)
                continue
            if px <= guard_stop and pos.get("peak_pnl", 0.0) > 0 and guard_stop > pos["sl_price"]:
                live_close(sym, "PROFIT_LOCK", px)
                continue
        else:
            if px <= pos["tp_price"] and not native:
                live_close(sym, "TP", px)
                continue
            if px >= pos["sl_price"] and not native:
                live_close(sym, "SL", px)
                continue
            if px >= guard_stop and pos.get("peak_pnl", 0.0) > 0 and guard_stop < pos["sl_price"]:
                live_close(sym, "PROFIT_LOCK", px)
                continue

        if _check_time_limit(sym, pos, px):
            continue
        _check_signal_flip(sym, pos)

    _maybe_profit_guard()

# ============================================================================
# KILL SWITCH / BAN CHECK
# ============================================================================
def ks_update(pnl: float):
    _ks["daily"] += pnl
    if pnl < 0:
        _ks["consec"] += 1
    else:
        _ks["consec"] = 0


def ks_check() -> Tuple[bool, str]:
    circuit, rem = _circuit_snapshot()
    if rem > 0:
        return True, circuit
    if _order_state_uncertain:
        return True, _entry_lock_reason or "ORDER_STATE_UNKNOWN"
    now = time.time()
    day = now - (now % 86400)
    if day > _ks["day_reset"]:
        _ks["day_reset"] = day
        _ks["daily"] = 0.0
        _ks["consec"] = 0
        _ks["active"] = False
    if _ks["daily"] <= DAILY_LOSS_LIMIT:
        _ks.update({"active": True, "reason": f"daily_loss {_ks['daily']:.2f}U", "resume": day + 86400})
        return True, _ks["reason"]
    if _ks["consec"] >= CONSEC_HARD_THRESHOLD:
        _ks.update({"active": True, "reason": f"consec({_ks['consec']})", "resume": now + CONSEC_HARD_PAUSE})
        return True, _ks["reason"]
    if _ks["consec"] >= CONSEC_FIRST_THRESHOLD:
        _ks.update({"active": True, "reason": f"soft_consec({_ks['consec']})", "resume": now + CONSEC_FIRST_PAUSE})
        return True, _ks["reason"]
    if _ks["active"]:
        if now < _ks["resume"]:
            return True, _ks["reason"]
        _ks["active"] = False
    return False, ""

# ============================================================================
# SCANNING / THREADS
# ============================================================================
def scan_one(sym: str):
    try:
        df = ohlcv(sym)
        if df is None:
            return None
        return score_entry(sym, df)
    except Exception as exc:
        _log_err(f"scan_{sym}", exc, 20)
        return None


def scan_batch(syms: List[str]) -> List[Dict[str, Any]]:
    out = []
    futs = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    try:
        for f in as_completed(futs, timeout=6):
            try:
                r = f.result(timeout=1)
                if r:
                    out.append(r)
            except Exception:
                pass
    except Exception:
        pass
    return out


def select_scan_symbols(syms: List[str], max_n: int = BATCH_SIZE) -> List[str]:
    tk = tickers_all()
    valid = [s for s in syms if s not in live_positions and tk.get(s, {}).get("vol", 0) >= MIN_24H_QUOTE_VOL]
    movers = sorted(valid, key=lambda s: abs(tk.get(s, {}).get("pct", 0)), reverse=True)
    hot = [s for s in _hot_syms if s in valid]
    return list(dict.fromkeys(hot[:5] + movers[:max_n]))[:max_n]


def t_slot_filler(syms: List[str]):
    while True:
        try:
            blocked, _ = ks_check()
            with _lock:
                open_count = len([p for p in live_positions.values() if not p.get("_r")])
            if blocked or open_count >= MAX_POSITIONS:
                time.sleep(SLOT_FILL_INT)
                continue

            macro = usdtd.snapshot()
            if macro.get("signal") not in ("LONG", "SHORT"):
                time.sleep(1.0)
                continue

            # Entry only on a fresh scan cycle and healthy websocket.
            if time.time() - _ws_last_msg_ts > WS_STALE_SEC:
                _stats["ws_block"] += 1
                time.sleep(1.0)
                continue

            scan_syms = select_scan_symbols(syms)
            if not scan_syms:
                time.sleep(1.0)
                continue
            results = scan_batch(scan_syms)
            results.sort(key=lambda x: x["score"], reverse=True)
            for candidate in results:
                with _lock:
                    current_open = len([p for p in live_positions.values() if not p.get("_r")])
                if current_open >= MAX_POSITIONS:
                    break
                live_open(candidate)
        except Exception as exc:
            _log_err("slot_filler", exc, 20)
        time.sleep(SCAN_INTERVAL)


def t_rescan(syms: List[str]):
    while True:
        try:
            _rescan_q.get(timeout=10)
            time.sleep(0.15)
            blocked, _ = ks_check()
            if blocked:
                continue
            results = scan_batch(select_scan_symbols(syms, 20))
            results.sort(key=lambda x: x["score"], reverse=True)
            with _lock:
                open_count = len([p for p in live_positions.values() if not p.get("_r")])
            for candidate in results:
                if open_count >= MAX_POSITIONS:
                    break
                if live_open(candidate):
                    open_count += 1
        except queue.Empty:
            pass
        except Exception as exc:
            _log_err("rescan", exc, 30)


def t_monitor():
    while True:
        try:
            monitor_positions()
        except Exception as exc:
            _log_err("monitor", exc, 10)
        time.sleep(MONITOR_INT)


def t_usdtd():
    while True:
        usdtd.update()
        time.sleep(USDTD_REFRESH_SEC)

# ============================================================================
# WEBSOCKET HANDLERS
# ============================================================================
def handle_all_ticker(msg):
    global _ws_last_msg_ts, _ws_ticker_cache, _ws_ticker_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        arr = data if isinstance(data, list) else [data]
        cache = {}
        for d in arr:
            if not isinstance(d, dict) or not d.get("s"):
                continue
            try:
                cache[d["s"]] = {
                    "pct": float(d.get("P", 0) or 0),
                    "vol": float(d.get("q", 0) or 0),
                    "last": float(d.get("c", 0) or 0),
                }
            except Exception:
                continue
        if cache:
            _ws_ticker_cache = cache
            _ws_ticker_ts = time.time()
    except Exception as exc:
        _log_err("ws_ticker", exc, 30)


def handle_mark_price(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        arr = data if isinstance(data, list) else [data]
        now = time.time()
        for d in arr:
            if not isinstance(d, dict):
                continue
            sym = d.get("s")
            p = d.get("p")
            if sym and p:
                px = float(p)
                if px > 0:
                    _ws_mark_price[sym] = (px, now)
    except Exception as exc:
        _log_err("ws_mark", exc, 30)


def handle_kline(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        k = data.get("k")
        if not k or not k.get("x"):
            return
        sym = data.get("s") or k.get("s")
        if sym:
            _append_closed_kline(sym, k)
    except Exception as exc:
        _log_err("ws_kline", exc, 30)


def handle_btc_agg(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict):
            return
        p = data.get("p")
        t = data.get("T")
        if p:
            _update_btc_tick(float(p), float(t) / 1000 if t else None)
    except Exception as exc:
        _log_err("ws_btc", exc, 30)


def handle_depth(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict):
            return
        sym = data.get("s")
        if not sym:
            stream = msg.get("stream", "") if isinstance(msg, dict) else ""
            if "@depth" in stream:
                sym = stream.split("@")[0].upper()
        if sym:
            order_book.update(sym, data.get("b", []), data.get("a", []))
    except Exception as exc:
        _log_err("ws_depth", exc, 30)


def handle_user_data(msg):
    try:
        if not isinstance(msg, dict):
            return
        if msg.get("e") != "ORDER_TRADE_UPDATE":
            return
        o = msg.get("o", {})
        sym = o.get("s")
        status = o.get("X")
        order_type = o.get("o")
        if not sym:
            return
        if status == "FILLED" and order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            with _lock:
                pos = live_positions.get(sym)
            if not pos or pos.get("_r"):
                return
            fill_qty = float(o.get("z", 0) or 0)
            fill_px = float(o.get("ap", 0) or 0)
            if fill_px <= 0:
                fill_px = price_live(sym) or float(pos["entry"])
            reason = "TP" if order_type == "TAKE_PROFIT_MARKET" else "SL"
            _cancel_native_protection(sym, pos)
            _finalize_closed_position(sym, reason, fill_px, filled_qty=fill_qty, native=True)
            print(f"  📡 [NATIVE EXIT] {sym} {order_type} fill:{fill_px:.6g} qty:{fill_qty:.8g}")
    except Exception as exc:
        _log_err("ws_user", exc, 10)

# ============================================================================
# RECONCILIATION / WATCHDOG
# ============================================================================
def reconcile_positions(syms: List[str]):
    global _order_state_uncertain, _entry_lock_reason
    try:
        data = _rest_call("reconcile_positions", client.futures_position_information, retries=0)
        exchange = {}
        wanted = set(syms)
        for p in data or []:
            sym = p.get("symbol")
            if sym not in wanted:
                continue
            amt = float(p.get("positionAmt", 0) or 0)
            if abs(amt) > 0:
                exchange[sym] = amt

        with _lock:
            local = {s: p for s, p in live_positions.items() if not p.get("_r")}

        mismatch = False
        for sym, pos in local.items():
            if sym not in exchange:
                # Likely native exit missed by WS; approximate with mark price and
                # finalize without issuing a duplicate market order.
                px = price_live(sym)
                if px > 0:
                    _cancel_native_protection(sym, pos)
                    _finalize_closed_position(sym, "RECONCILE_CLOSE", px, filled_qty=pos.get("qty"), native=True)
                else:
                    mismatch = True
            else:
                expected_sign = 1 if pos["side"] == "LONG" else -1
                if np.sign(exchange[sym]) != expected_sign:
                    mismatch = True

        # Exchange has a bot-symbol position that local state does not know.
        unknown = [s for s in exchange if s not in local]
        if unknown:
            mismatch = True
            _log_warn("reconcile", f"exchange-only position(s): {unknown}", 30)

        _order_state_uncertain = mismatch
        _entry_lock_reason = "POSITION_MISMATCH" if mismatch else ""
    except Exception as exc:
        _log_err("reconcile", exc, 30)


def t_reconcile(syms: List[str]):
    while True:
        try:
            reconcile_positions(syms)
        except Exception as exc:
            _log_err("reconcile_thread", exc, 30)
        time.sleep(15)


def t_ws_watchdog():
    while True:
        idle = time.time() - _ws_last_msg_ts
        if idle > WS_STALE_SEC:
            print(f"  🚨 WS STALE {idle:.0f}s — NEW ENTRY LOCKED")
        time.sleep(5)

# ============================================================================
# STARTUP / DASHBOARD
# ============================================================================
def preflight(syms: List[str]):
    # REST must prove it is demo-only before trading.
    url = str(getattr(client, "FUTURES_URL", ""))
    if "demo-fapi.binance.com" not in url:
        raise RuntimeError(f"DEMO SAFETY LOCK FAILED: FUTURES_URL={url}")
    _rest_call("futures_ping", client.futures_ping, retries=0)
    info = _rest_call("exchange_info", client.futures_exchange_info, retries=0)
    valid = set()
    for item in info.get("symbols", []):
        _normalize_symbol_info(item)
        if item.get("status") == "TRADING" and item.get("quoteAsset") == "USDT":
            valid.add(item.get("symbol"))
    active = [s for s in syms if s in valid]
    if not active:
        raise RuntimeError("Tidak ada simbol yang valid/TRADING.")
    positions = _rest_call("preflight_positions", client.futures_position_information, retries=0)
    open_positions = []
    wanted = set(active)
    for p in positions or []:
        if p.get("symbol") in wanted and abs(float(p.get("positionAmt", 0) or 0)) > 0:
            open_positions.append((p.get("symbol"), p.get("positionAmt")))
    if open_positions:
        raise RuntimeError(f"Posisi bot masih terbuka saat startup: {open_positions}")
    return active


def bootstrap_all(syms: List[str]):
    print(f"  📥 Bootstrap 5m {len(syms)} simbol via DEMO REST...")
    ok = 0
    for i, s in enumerate(syms, 1):
        if _bootstrap_klines(s, Client.KLINE_INTERVAL_5MINUTE, 160) is not None:
            ok += 1
        if i < len(syms):
            time.sleep(REST_MIN_INTERVAL)
    print(f"  ✅ Bootstrap: {ok}/{len(syms)} siap")


def print_dashboard():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0.0
    sess = max((time.time() - _stats["start"]) / 3600.0, 1e-9)
    tph = n / sess
    equity, floating = current_equity()
    macro = usdtd.snapshot()
    circuit, _ = _circuit_snapshot()
    avg_win = learning.avg_win()
    avg_loss = learning.avg_loss()
    bep = abs(avg_loss) / (abs(avg_loss) + avg_win) * 100 if avg_win > 0 or avg_loss > 0 else 50.0
    print("\n" + "─" * 76)
    print("🔔 BOT SCALPING v24 DEMO — USDT.D MTF + 5M BOS ENGINE")
    print(f"🎯 Trades:{n} WR:{wr:.1f}% ({_stats['wins']}W/{_stats['losses']}L) | {tph:.1f}T/hr")
    print(f"💚 Realized:{_stats['pnl']:+.5f}U | Floating:{floating:+.5f}U | Equity:{equity:+.5f}U")
    print(f"🏆 Peak Equity:{_stats['ath_equity']:+.5f}U | Floor:{_stats['equity_floor']:+.5f}U")
    states = macro.get("states", {})
    print(f"🧭 USDT.D: {macro.get('signal','UNKNOWN')} | [1h:{states.get('1h','N')} 2h:{states.get('2h','N')} 3h:{states.get('3h','N')} 4h:{states.get('4h','N')}]")
    print(f"📈 Exit: TP:{_stats['tp_exit']} SL:{_stats['sl_exit']} Lock:{_stats['profit_lock_exit']} Time:{_stats['time_limit_exit']} Flip:{_stats['flip_exit']}")
    print(f"🛡️ Blocks: Macro:{_stats['macro_block']} BOS:{_stats['bos_block']} Liq:{_stats['liquidity_block']} Corr:{_stats['correlation_block']} WS:{_stats['ws_block']}")
    print(f"🛑 Circuit:{circuit or 'READY'} | SLban:{_stats['sl_ban_count']} Cascade:{_stats['cascade_ban_count']} TimeBan:{_stats['time_limit_ban_count']} ProfitGuard:{_stats['profit_guard_count']}")
    print(f"📊 BEP-WR est:{bep:.1f}% | AvgWin:{avg_win:+.4f}U | AvgLoss:{avg_loss:+.4f}U")
    if trade_log:
        print("─" * 62)
        print("📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"  {em} {t['sym']:<14} {t['side']:<5} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']} peak:{t['peak_r']:.2f}R")
    print("─" * 76)


def run_bot():
    print("╔" + "═" * 74 + "╗")
    print("║  BOT SCALPING v24 DEMO — HARD DEMO SAFETY LOCK".ljust(75) + "║")
    print("║  USDT.D 1H/2H/3H/4H → 5M BOS → CONFLUENCE → ENTRY".ljust(75) + "║")
    print("║  Rising USDT.D = CRYPTO SHORT | Falling USDT.D = CRYPTO LONG".ljust(75) + "║")
    print("║  Equity Guard + Profit Lock + Native SL/TP + Reconciliation".ljust(75) + "║")
    print("╚" + "═" * 74 + "╝")

    syms = preflight(SYMBOLS)
    print(f"  ✅ DEMO endpoint locked: {client.FUTURES_URL}")
    print(f"  ✅ Symbols active: {len(syms)}")
    if not TV_AVAILABLE:
        raise RuntimeError("tvdatafeed-enhanced wajib terpasang untuk USDT.D MTF engine.")
    bootstrap_all(syms)

    twm.start()
    # Market-data streams. Current python-binance uses demo=True on the internal
    # AsyncClient; its Futures demo stream base is the Binance Futures test stream.
    twm.start_all_mark_price_socket(callback=handle_mark_price, fast=False)
    twm.start_futures_multiplex_socket(callback=handle_all_ticker, streams=["!ticker@arr"])
    kline_streams = [f"{s.lower()}@kline_5m" for s in syms]
    twm.start_futures_multiplex_socket(callback=handle_kline, streams=kline_streams)
    twm.start_futures_multiplex_socket(callback=handle_btc_agg, streams=["btcusdt@aggTrade"])
    for i in range(0, len(syms), DEPTH_SOCKET_CHUNK):
        chunk = syms[i:i+DEPTH_SOCKET_CHUNK]
        twm.start_futures_multiplex_socket(
            callback=handle_depth,
            streams=[f"{s.lower()}@depth10" for s in chunk],
        )
        time.sleep(0.15)
    try:
        twm.start_futures_user_socket(callback=handle_user_data)
    except Exception as exc:
        _log_err("user_socket", exc, 0)
        # User stream is required for native exits; keep running local exits but lock entries.
        print("  🚨 USER DATA SOCKET GAGAL — entry baru akan dikunci sampai sinkronisasi pulih")

    threading.Thread(target=t_usdtd, daemon=True).start()
    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_reconcile, args=(syms,), daemon=True).start()
    threading.Thread(target=t_ws_watchdog, daemon=True).start()

    cycle = 0
    while True:
        cycle += 1
        macro = usdtd.snapshot()
        circuit, _ = _circuit_snapshot()
        equity, floating = current_equity()
        with _lock:
            open_count = len([p for p in live_positions.values() if not p.get("_r")])
        ws_idle = time.time() - _ws_last_msg_ts
        ws_flag = f" | WS_IDLE:{ws_idle:.0f}s" if ws_idle > WS_STALE_SEC else ""
        macro_states = macro.get("states", {})
        state_txt = f"[1h:{macro_states.get('1h','N')} 2h:{macro_states.get('2h','N')} 3h:{macro_states.get('3h','N')} 4h:{macro_states.get('4h','N')}]"
        print("\n" + "═" * 76)
        print(f"#{cycle} {time.strftime('%H:%M:%S')} | DEMO | USDT.D:{macro.get('signal','UNKNOWN')} {state_txt} | POS:{open_count}/{MAX_POSITIONS}")
        print(f"PnL:{_stats['pnl']:+.4f}U | Float:{floating:+.4f}U | Equity:{equity:+.4f}U | Peak:{_stats['ath_equity']:+.4f}U | {circuit or 'READY'}{ws_flag}")
        blocked, reason = ks_check()
        if blocked:
            print(f"🚨 ENTRY LOCK: {reason}")
        elif open_count >= MAX_POSITIONS:
            print("✅ Slots full — monitoring only")
        else:
            print("🔍 Entry engine armed — macro + BOS + confluence + liquidity gates")
        if cycle % 30 == 0:
            print_dashboard()
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot DEMO dihentikan manual.")
    except Exception as exc:
        print(f"\n❌ BOT DEMO STARTUP STOP: {type(exc).__name__}: {exc}")
        raise
