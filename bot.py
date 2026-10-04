"""
Bot Scalping v22.0 — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
EXECUTION MODES:
- DEMO_TESTNET = True  -> Mengirim order ke Binance Futures Testnet (Demo)
- DEMO_TESTNET = False -> Mengirim order NYATA ke Akun Real Binance Futures

FEATURES:
- Dynamic Toggle Mode (NORMAL <-> INVERTED saat Loss)
- MAX_POSITIONS = 1
- ORDER_USDT = 3.0 USDT
- LEVERAGE = 20x
- Dashboard Lengkap: PnL Net, ATH PnL, Best Single Trade, Worst Single Trade
- History Last 5 dengan Jam Entry & Jam Exit
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import os
import time
import inspect
import math
import threading
import queue
import numpy as np
import pandas as pd
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
import ta

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════
#  MODE PENGATURAN AKUN (DEMO TESTNET VS REAL AKUN)
# ═══════════════════════════════════════════════════════════════════════════
DEMO_TESTNET = True  # Set True untuk Testnet Demo, False untuk Akun Real

if DEMO_TESTNET:
    api_key = os.getenv("TESTNET_API_KEY") or os.getenv("API_KEY")
    api_secret = os.getenv("TESTNET_API_SECRET") or os.getenv("API_SECRET")
else:
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")

try:
    client = Client(api_key, api_secret, testnet=DEMO_TESTNET)
    if DEMO_TESTNET:
        client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
    else:
        client.FUTURES_URL = "https://fapi.binance.com/fapi"
except Exception as e:
    print(f"⚠️ Gagal inisialisasi Binance Client: {e}")
    client = Client(api_key, api_secret)

WS_MAX_QUEUE_SIZE = 2000
DEPTH_SOCKET_CHUNK = 8
MARK_PRICE_FAST = False

def _create_twm():
    kwargs = {"api_key": api_key, "api_secret": api_secret}
    try:
        params = inspect.signature(ThreadedWebsocketManager.__init__).parameters
        if "max_queue_size" in params:
            kwargs["max_queue_size"] = WS_MAX_QUEUE_SIZE
    except Exception:
        pass
    try:
        return ThreadedWebsocketManager(**kwargs)
    except TypeError:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)
    except Exception:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)

twm = _create_twm()

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION & PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 3.0
MAX_POSITIONS = 1

# Scanning & Concurrency
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01

# REST API Safety
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0
REST_418_COOLDOWN = 900.0

# Scoring & Filter
MIN_SCORE = 55

# Risk Management (ATR Multipliers)
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8

MIN_TP_PCT       = 0.025
MAX_TP_PCT       = 0.035
MIN_SL_PCT       = 0.015
MAX_SL_PCT       = 0.025
MAX_HOLD_SECONDS = 6120   # 1 Jam 42 Menit batas maksimal hold posisi

# Microstructure & Correlation
WALL_RATIO_THRESHOLD = 2.5
WALL_DEPTH_PCT       = 0.35
WALL_PROXIMITY_PCT   = 0.005

BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# Kill Switch
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

# ═══════════════════════════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT", "APEUSDT",
    "CRVUSDT", "1000SHIBUSDT", "COMPUSDT", "MKRUSDT", "SNXUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════════════════════════
#  ORDER BOOK ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    def __init__(self):
        self._cache = {}
        self._history = defaultdict(lambda: deque(maxlen=10))
        self._lock = threading.Lock()

    def update(self, symbol: str, bids_raw: list, asks_raw: list, ts: float = None):
        if ts is None: ts = time.time()
        try:
            bids = [(float(p), float(q)) for p, q in bids_raw]
            asks = [(float(p), float(q)) for p, q in asks_raw]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            bid_vol = sum(q for _, q in bids)
            ask_vol = sum(q for _, q in asks)
            tot_vol = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / (tot_vol + 1e-9)

            with self._lock:
                self._cache[symbol] = {
                    "bids": bids, "asks": asks,
                    "imbalance": imbalance,
                    "ts": ts
                }
        except Exception:
            pass

    def check_walls(self, symbol: str, current_price: float, side: str) -> Tuple[bool, str, float, float, float]:
        with self._lock:
            book = self._cache.get(symbol)
        if not book: return False, "NO_DATA", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]
            tot_ask = sum(q for _, q in asks)
            if not asks or tot_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)

            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]
            tot_bid = sum(q for _, q in bids)
            if not bids or tot_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = tot_bid / len(bids)

            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_bid or qty >= WALL_DEPTH_PCT * tot_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult

        return False, "OK", 0.0, 0.0, 0.0

order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  MACRO BTC ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0}
        self.lock = threading.Lock()

    def update_tick(self, price: float, ts: float = None):
        if ts is None: ts = time.time()
        with self.lock:
            self.tick_history.append((ts, price))
            cutoff = ts - BTC_WINDOW_SEC
            baseline_price = None
            for t_ts, t_px in self.tick_history:
                if t_ts >= cutoff:
                    baseline_price = t_px
                    break

            if baseline_price and baseline_price > 0:
                delta = (price - baseline_price) / baseline_price
                if delta <= BTC_CRASH_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "CRASH"):
                    self.breaker = {"active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN}
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {"active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN}

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    if self.breaker["type"] == "CRASH" and side == "LONG": return True, f"BTC Crash Breaker ({rem:.0f}s)"
                    elif self.breaker["type"] == "PUMP" and side == "SHORT": return True, f"BTC Pump Breaker ({rem:.0f}s)"
                else:
                    self.breaker["active"] = False
                    self.breaker["type"] = "NONE"
        return False, "OK"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN"}

# ═══════════════════════════════════════════════════════════════════════════
#  RISK & REGIME ENGINES
# ═══════════════════════════════════════════════════════════════════════════

class DynamicRiskManager:
    @staticmethod
    def calculate_levels(entry_price: float, execution_side: str, atr: float) -> Dict[str, float]:
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.015
        tp_pct = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_RESTORED_MULTIPLIER * atr_pct))
        sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_RESTORED_MULTIPLIER * atr_pct))

        if execution_side == "LONG":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        return {"tp_pct": tp_pct, "sl_pct": sl_pct, "tp_price": tp_price, "sl_price": sl_price}

class MarketRegime:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < 55: return "RANGE", 0, 0
        row = df.iloc[-2]
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50

        if adx > 35 and bull_stack: return "TRENDING_BULL", adx, 1.0
        elif adx > 35 and bear_stack: return "TRENDING_BEAR", adx, -1.0
        else: return "RANGE", 30, 0

class SignalScorer:
    def get_signal(self, df: pd.DataFrame, symbol: str = None) -> Tuple[Optional[str], int, List[str], float, str, float]:
        if df is None or len(df) < 55: return None, 0, [], 0.0, "UNKNOWN", 0.0
        
        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df)
        short_score, short_sigs = self._score_short(df)
        atr = df["atr"].iloc[-2]

        if long_score >= MIN_SCORE and long_score > short_score:
            return "LONG", long_score, long_sigs, atr, regime, bias
        elif short_score >= MIN_SCORE and short_score > long_score:
            return "SHORT", short_score, short_sigs, atr, regime, bias

        return None, 0, [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame) -> Tuple[int, List[str]]:
        row = df.iloc[-2]
        score, sigs = 0, []
        if row["close"] > row["e5"] > row["e9"]: score += 30; sigs.append("EMA_Bull")
        if row["rsi"] > 50: score += 25; sigs.append("RSI_Bull")
        return score, sigs

    def _score_short(self, df: pd.DataFrame) -> Tuple[int, List[str]]:
        row = df.iloc[-2]
        score, sigs = 0, []
        if row["close"] < row["e5"] < row["e9"]: score += 30; sigs.append("EMA_Bear")
        if row["rsi"] < 50: score += 25; sigs.append("RSI_Bear")
        return score, sigs

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ticker_cache    = {}
_ticker_ts       = 0
_lock            = threading.Lock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q        = queue.Queue()

_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}

# TRACKING REKAP PNL LENGKAP
_stats = {
    "trades": 0, "wins": 0, "losses": 0, 
    "pnl": 0.0, "ath_pnl": 0.0,
    "best": 0.0, "worst": 0.0, # Best & Worst Single Trade
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0,
    "start": time.time(),
}

is_logic_inverted = False 

live_positions = {}
trade_log      = [] # Menyimpan riwayat transaksi lengkap
scorer         = SignalScorer()

_last_err_print = defaultdict(float)
_rest_lock = threading.Lock()
_rest_last_ts = 0.0

def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now

def _rest_call(tag, fn, *args, **kwargs):
    global _rest_last_ts
    with _rest_lock:
        gap = time.time() - _rest_last_ts
        if gap < REST_MIN_INTERVAL: time.sleep(REST_MIN_INTERVAL - gap)
        _rest_last_ts = time.time()
    return fn(*args, **kwargs)

def get_precision(symbol):
    if symbol in _precision_cache: return _precision_cache[symbol]
    try:
        info = _rest_call("exchange_info", client.futures_exchange_info)
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except Exception: pass
    return 2

def qty(symbol, price):
    raw = (ORDER_USDT * LEVERAGE) / price
    return round(raw, get_precision(symbol))

def price_live(symbol):
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if px > 0 and (time.time() - ts) < MARKPRICE_FRESH_SEC: return px
    try:
        px = float(_rest_call(f"price_{symbol}", client.futures_symbol_ticker, symbol=symbol)["price"])
        return px
    except Exception: return 0.0

def _compute_indicators(df):
    close = df["close"]
    high, low, volume = df["high"], df["low"], df["volume"].replace(0, 1e-9)

    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    df["e5"]  = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"]  = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()
    return df

def run_ta(df): return _compute_indicators(df)

def _bootstrap_klines(symbol, interval, limit=100):
    try:
        kl = _rest_call(f"klines_{symbol}", client.futures_klines, symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume"]: df[c] = df[c].astype(float)
        df = _compute_indicators(df)
        with _kline_lock: _kline_cache[symbol] = df
        return df
    except Exception: return None

def ohlcv(symbol, interval, limit=100):
    with _kline_lock: df = _kline_cache.get(symbol)
    if df is not None: return df
    return _bootstrap_klines(symbol, interval, limit)

def set_leverage_safe(symbol, leverage=LEVERAGE):
    try: _rest_call(f"leverage_{symbol}", client.futures_change_leverage, symbol=symbol, leverage=leverage)
    except Exception as e: _log_err(f"leverage_{symbol}", e)

# ═══════════════════════════════════════════════════════════════════════════
#  ORDER EXECUTION & POSITION TRACKING
# ═══════════════════════════════════════════════════════════════════════════

def live_open(orig_direction, score, sigs, price, atr, regime, bias, sym, risk_profile):
    global is_logic_inverted

    if orig_direction not in ("LONG", "SHORT"): return

    execution_side = ("SHORT" if orig_direction == "LONG" else "LONG") if is_logic_inverted else orig_direction

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS: return
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    if px_now > 0: price = px_now

    q_val = qty(sym, price)
    if q_val <= 0:
        with _lock: live_positions.pop(sym, None)
        return

    set_leverage_safe(sym, LEVERAGE)

    order_side = "BUY" if execution_side == "LONG" else "SELL"
    entry_ts = time.time()
    try:
        order_res = _rest_call(
            f"open_{sym}",
            client.futures_create_order,
            symbol=sym, side=order_side, type="MARKET", quantity=q_val
        )
        entry_price = float(order_res.get("avgPrice", price))
        if entry_price == 0: entry_price = price
    except Exception as e:
        print(f"❌ [ORDER ERROR] Gagal Membuka Posisi ({sym}): {e}")
        with _lock: live_positions.pop(sym, None)
        return

    risk = DynamicRiskManager.calculate_levels(entry_price, execution_side, atr)

    pos = {
        "side": execution_side,
        "entry": entry_price,
        "qty": q_val,
        "open_time": entry_ts,
        "open_time_str": time.strftime("%H:%M:%S", time.localtime(entry_ts)), # Jam Entry
        "tp_price": risk["tp_price"],
        "sl_price": risk["sl_price"],
    }

    with _lock: live_positions[sym] = pos

    mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
    env_str = "DEMO TESTNET" if DEMO_TESTNET else "REAL ACCOUNT"
    print(
        f"\n  🚀 [{env_str}] {sym} EXEC:{execution_side} (Mode:{mode_str}) @{entry_price:.6g} | "
        f"Jam Entry: {pos['open_time_str']} | QTY:{q_val}"
    )

    _stats["trades"] += 1

def live_close(sym, reason, price=None):
    global is_logic_inverted

    with _lock: pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    exit_ts = time.time()
    exit_time_str = time.strftime("%H:%M:%S", time.localtime(exit_ts)) # Jam Exit
    side = pos["side"]
    entry = pos["entry"]
    q_val = pos["qty"]

    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        close_res = _rest_call(
            f"close_{sym}",
            client.futures_create_order,
            symbol=sym, side=close_side, type="MARKET", quantity=q_val, reduceOnly=True
        )
        exit_price = float(close_res.get("avgPrice", price or 0))
        if exit_price == 0: exit_price = price_live(sym)
    except Exception as e:
        print(f"❌ [CLOSE ERROR] Gagal Menutup Posisi ({sym}): {e}")
        with _lock: live_positions[sym] = pos
        return

    gross_pnl = (exit_price - entry) * q_val if side == "LONG" else (entry - exit_price) * q_val
    pnl = gross_pnl - ((entry * q_val + exit_price * q_val) * 0.0005)

    won = pnl >= 0
    e_icon = "🟢" if won else "🔴"

    # LOGIKA TOGGLE INVERT / NORMAL MULTI-LOSS
    if not won:
        is_logic_inverted = not is_logic_inverted
        next_mode = "INVERTED" if is_logic_inverted else "NORMAL"
        print(f"  🔄 [LOGIC TOGGLE] Loss ({pnl:+.4f}U)! Mode berganti ke: {next_mode}")
    else:
        current_mode = "INVERTED" if is_logic_inverted else "NORMAL"
        print(f"  ✅ [LOGIC STABLE] Profit ({pnl:+.4f}U)! Logika bertahan di: {current_mode}")

    print(
        f"  {e_icon} [ORDER CLOSED] {sym} {side} CLOSE — {reason} | "
        f"{entry:.6g}→{exit_price:.6g} | Jam Entry: {pos.get('open_time_str', 'N/A')} | Jam Exit: {exit_time_str} | PnL:{pnl:+.5f}U"
    )

    # UPDATE SUMMARY REKAP PNL
    _stats["pnl"] += pnl
    if _stats["pnl"] > _stats["ath_pnl"]: 
        _stats["ath_pnl"] = _stats["pnl"]

    # REKAP PROFIT TERTINGGI & LOSS TERTINGGI (BEST/WORST SINGLE TRADE)
    if _stats["trades"] == 1:
        _stats["best"] = pnl
        _stats["worst"] = pnl
    else:
        if pnl > _stats["best"]: _stats["best"] = pnl
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if won: _stats["wins"] += 1
    else: _stats["losses"] += 1

    if reason == "SL": _stats["hard_sl"] += 1
    elif reason == "TP": _stats["tp_exit"] += 1
    elif reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

    # SIMPAN KE TRADE LOG DENGAN JAM ENTRY & JAM EXIT
    trade_log.append({
        "sym": sym, 
        "side": side, 
        "entry": round(entry, 7),
        "exit": round(exit_price, 7), 
        "pnl": round(pnl, 5),
        "reason": reason, 
        "hold": int(exit_ts - pos["open_time"]),
        "entry_time": pos.get("open_time_str", "N/A"),
        "exit_time": exit_time_str
    })

    _rescan_q.put(1)

def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"): continue

        hold_time = time.time() - pos["open_time"]
        px = price_live(sym)
        if px == 0: continue

        side, tp_px, sl_px = pos["side"], pos["tp_price"], pos["sl_price"]

        if side == "LONG":
            if px >= tp_px: live_close(sym, "TP", tp_px); continue
            if px <= sl_px: live_close(sym, "SL", sl_px); continue
        elif side == "SHORT":
            if px <= tp_px: live_close(sym, "TP", tp_px); continue
            if px >= sl_px: live_close(sym, "SL", sl_px); continue

        if hold_time > MAX_HOLD_SECONDS:
            live_close(sym, "TIME_LIMIT", px)
            continue

# ═══════════════════════════════════════════════════════════════════════════
#  SCANNER THREADS & DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

def scan_one(sym):
    try:
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None: return None
        df_ta = run_ta(df.copy())
        px_candle, atr_val = df_ta["close"].iloc[-2], df_ta["atr"].iloc[-2]
        if px_candle == 0 or np.isnan(atr_val): return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df_ta, sym)
        if orig_direction is None or orig_direction not in ("LONG", "SHORT"): return None

        execution_side = ("SHORT" if orig_direction == "LONG" else "LONG") if is_logic_inverted else orig_direction
        px_live = price_live(sym)
        if px_live == 0: return None

        btc_vetoed, _ = btc_macro.check_veto(execution_side)
        if btc_vetoed: return None

        risk_profile = DynamicRiskManager.calculate_levels(px_live, execution_side, atr_val)
        return (sym, orig_direction, score, sigs, px_live, atr_val, regime, bias, risk_profile)
    except Exception: return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    for f in as_completed(fut, timeout=5):
        try:
            r = f.result(timeout=1)
            if r: res.append(r)
        except: pass
    return res

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
    env_str = "BINANCE DEMO TESTNET" if DEMO_TESTNET else "BINANCE REAL ACCOUNT"
    
    print(f"\n  {'─'*72}")
    print(f"    🔔 INSTITUTIONAL SCALPING DASHBOARD [{env_str}]")
    print(f"    🎯 Mode Logika: {mode_str} | Trades: {n} | WR: {wr:.0f}% (W:{_stats['wins']} L:{_stats['losses']})")
    print(f"    💰 PnL Net: {pnl:+.5f} USDT | ATH PnL: {_stats['ath_pnl']:+.5f} USDT")
    print(f"    🏆 Best Single Trade: {_stats['best']:+.5f} USDT | 🔻 Worst Single Trade: {_stats['worst']:+.5f} USDT")
    print(f"    📈 Exits -> TP: {_stats['tp_exit']} | SL: {_stats['hard_sl']} | TimeLimit: {_stats['time_limit_exit']}")

    # RIWAYAT 5 KOIN TERAKHIR LENGKAP DENGAN JAM ENTRY DAN EXIT
    if trade_log:
        print(f"    {'─'*62}\n    📋 Last 5 Trades History:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] >= 0 else "🔴"
            print(f"        {em} [{t['entry_time']} -> {t['exit_time']}] {t['sym']:<12} {t['side']:<5} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*72}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0:
                time.sleep(SLOT_FILL_INT); continue

            valid_syms = [s for s in syms if s not in live_positions]
            bs = scan_idx * BATCH_SIZE
            scan_list = valid_syms[bs:bs+BATCH_SIZE]
            scan_idx = (scan_idx + 1) % n_bat

            if scan_list:
                res = scan_batch(scan_list)
                if res:
                    res.sort(key=lambda x: x[2], reverse=True)
                    for r in res[:slots]:
                        if len(live_positions) >= MAX_POSITIONS: break
                        sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                        live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except Exception: pass
        time.sleep(SLOT_FILL_INT)

# ═══════════════════════════════════════════════════════════════════════════
#  WEBSOCKET HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

def handle_mark_price(msg):
    try:
        data = msg.get("data", msg)
        arr = data if isinstance(data, list) else [data]
        now = time.time()
        for d in arr:
            if isinstance(d, dict) and d.get("s") and d.get("p"):
                _ws_mark_price[d["s"]] = (float(d["p"]), now)
    except Exception: pass

def handle_kline_multiplex(msg):
    try:
        data = msg.get("data", msg)
        k = data.get("k")
        if k and k.get("x"):
            sym = data.get("s") or k.get("s")
            if sym:
                with _kline_lock:
                    df = _kline_cache.get(sym)
                    if df is not None:
                        new_row = {"time": int(k["t"]), "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"])}
                        df_new = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True).iloc[-100:]
                        _kline_cache[sym] = _compute_indicators(df_new)
    except Exception: pass

def run_bot():
    env_str = "DEMO TESTNET" if DEMO_TESTNET else "REAL ACCOUNT"
    print("╔════════════════════════════════════════════════════════════════════╗")
    print(f"║  💎 BOT SCALPING v22.0 — MODE: {env_str:<32} ║")
    print("║  1. Mode Awal: NORMAL (LONG->LONG, SHORT->SHORT)                   ║")
    print("║  2. Jika Loss (SL/TimeLimit < 0) -> TOGGLE Invert/Normal           ║")
    print("║  3. Margin = $3.0 | Leverage = 20x | Max Position = 1              ║")
    print("║  4. Live Dashboard: PnL, ATH, Best/Worst Single Trade & Jam Entry  ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    try:
        info = _rest_call("startup_info", client.futures_exchange_info)
        valid = {s["symbol"] for s in info["symbols"] if s["status"] == "TRADING"}
    except Exception as e:
        raise RuntimeError(f"Gagal koneksi ke Binance Futures ({env_str}): {e}")

    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))

    twm.start()
    twm.start_all_mark_price_socket(callback=handle_mark_price)
    kline_streams = [f"{s.lower()}@kline_5m" for s in syms]
    twm.start_futures_multiplex_socket(callback=handle_kline_multiplex, streams=kline_streams)

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    time.sleep(2)
    
    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
        print(f"\n{'═'*68}")
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} Env:[{env_str}] Mode:[{mode_str}] ActivePos:({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")

        if slots == 0: print(f"  ✅ Slots Full — Monitoring Posisi Terbuka di Binance")
        else: print(f"  🔍 Slot Kosong — Scanning Signal...")
        if cycle % 15 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot dihentikan manual.")
    except Exception as e:
        print(f"\n❌ BOT STOPPED: {type(e).__name__}: {e}")
