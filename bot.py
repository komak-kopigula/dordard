"""
Bot Scalping v22.0 — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
EXECUTION MODES:
- DEMO_TESTNET = True  -> Mengirim order ke Binance Futures Testnet (Demo)
- DEMO_TESTNET = False -> Mengirim order NNYATA ke Akun Real Binance Futures

LOGIC FEATURES:
- Dynamic Toggle Mode (NORMAL <-> INVERTED saat Loss)
- MAX_POSITIONS = 1
- ORDER_USDT = 3.0 USDT
- LEVERAGE = 20x
- Includes Last 5 Trades History in Dashboard
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
# Set True  -> Menggunakan API Key & Secret Testnet (fapi.binancefuture.com)
# Set False -> Menggunakan API Key & Secret REAL Binance (fapi.binance.com)
DEMO_TESTNET = True  

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
REST_RETRIES = 2

# Scoring & Filter
MIN_SCORE      = 55
SLIPPAGE_GUARD = 0.0015

# Risk Management (ATR Multipliers)
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8

MIN_TP_PCT        = 0.025
MAX_TP_PCT        = 0.035
MIN_SL_PCT        = 0.015
MAX_SL_PCT        = 0.025
MAX_HOLD_SECONDS  = 6120   # 1 Jam 42 Menit batas maksimal hold posisi

# Microstructure & Correlation
WALL_RATIO_THRESHOLD  = 2.5
WALL_DEPTH_PCT        = 0.35
WALL_PROXIMITY_PCT    = 0.005
IMBALANCE_STRONG_BULL = 0.25
IMBALANCE_STRONG_BEAR = -0.25
SPOOF_DROP_THRESHOLD  = 0.40

BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# Kill Switch
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 20

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

            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0

            with self._lock:
                self._cache[symbol] = {
                    "bids": bids, "asks": asks,
                    "bid_vol": bid_vol, "ask_vol": ask_vol,
                    "imbalance": imbalance,
                    "best_bid": best_bid, "best_ask": best_ask,
                    "ts": ts
                }
                self._history[symbol].append((ts, bid_vol, ask_vol, best_bid, best_ask))
        except Exception:
            pass

    def get_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._cache.get(symbol)

    def get_imbalance(self, symbol: str) -> float:
        book = self.get_book(symbol)
        return book["imbalance"] if book else 0.0

    def check_walls(self, symbol: str, current_price: float, side: str) -> Tuple[bool, str, float, float, float]:
        book = self.get_book(symbol)
        if not book: return False, "NO_DATA", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]
            tot_ask = book["ask_vol"]
            if not asks or tot_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)

            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]
            tot_bid = book["bid_vol"]
            if not bids or tot_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = tot_bid / len(bids)

            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_bid or qty >= WALL_DEPTH_PCT * tot_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult

        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol: str, side: str) -> Tuple[bool, str]:
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if len(hist) < 3: return False, ""
        
        curr_ts, curr_b_vol, curr_a_vol, _, _ = hist[-1]
        for ts, b_vol, a_vol, _, _ in hist[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG" and b_vol > 0:
                    if curr_b_vol < b_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_b_vol / b_vol) * 100
                        return True, f"Bid liquidity pulled ({drop_pct:.0f}% drop)"
                elif side == "SHORT" and a_vol > 0:
                    if curr_a_vol < a_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_a_vol / a_vol) * 100
                        return True, f"Ask liquidity pulled ({drop_pct:.0f}% drop)"
        return False, ""

order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  MACRO BTC & TICK CORRELATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0, "trigger_ts": 0.0}
        self.last_price = 0.0
        self.lock = threading.Lock()

    def update_tick(self, price: float, ts: float = None):
        if ts is None: ts = time.time()
        with self.lock:
            self.last_price = price
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
                    self.breaker = {
                        "active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚨 [BTC FLASH CRASH DETECTED] Drop: {delta*100:+.2f}% | Altcoin LONGs LOCKED!")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {
                        "active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚀 [BTC FLASH PUMP DETECTED] Surge: {delta*100:+.2f}% | Altcoin SHORTs LOCKED!")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]
                    if b_type == "CRASH" and side == "LONG": return True, f"BTC Flash Crash ({rem:.0f}s left)"
                    elif b_type == "PUMP" and side == "SHORT": return True, f"BTC Flash Pump ({rem:.0f}s left)"
                else:
                    self.breaker["active"] = False
                    self.breaker["type"] = "NONE"
        return False, "OK"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN"}

# ═══════════════════════════════════════════════════════════════════════════
#  RISK & REGIME ENGINES
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[bool, bool, str]:
        if df is None or len(df) < 25: return False, False, ""
        row = df.iloc[-2]
        
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0)
        low, high, close = row.get("low", 0.0), row.get("high", 0.0), row.get("close", 0.0)
        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        lw_ratio = row.get("lower_wick_ratio", 0.0)
        uw_ratio = row.get("upper_wick_ratio", 0.0)

        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        wick_bull = lw_ratio >= 0.38
        close_held_bull = close >= (low + 0.45 * rng)
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)

        heavy_buyer = (delta_ratio > 0.20) or (buy_ratio > 0.60)
        wick_bear = uw_ratio >= 0.38
        close_held_bear = close <= (high - 0.45 * rng)
        bear_absorb = vol_spike and heavy_buyer and (wick_bear or close_held_bear)

        return bull_absorb, bear_absorb, ""

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
    REGIME_TRENDING_BULL = "TRENDING_BULL"
    REGIME_TRENDING_BEAR = "TRENDING_BEAR"
    REGIME_RANGE         = "RANGE"
    REGIME_VOLATILE      = "VOLATILE"
    REGIME_EXHAUSTION    = "EXHAUSTION"

    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < 55: return MarketRegime.REGIME_RANGE, 0, 0
        row, prev = df.iloc[-2], df.iloc[-3]
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50

        if adx > 35 and bull_stack: return MarketRegime.REGIME_TRENDING_BULL, adx, 1.0
        elif adx > 35 and bear_stack: return MarketRegime.REGIME_TRENDING_BEAR, adx, -1.0
        else: return MarketRegime.REGIME_RANGE, 30, 0

# ═══════════════════════════════════════════════════════════════════════════
#  SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class SignalWeights:
    def __init__(self):
        self.weights = {
            "ema_bull_stack": 30, "mom_strong": 25, "macd_cross_up": 22,
            "ema_bear_stack": 30, "mom_strong_neg": 25, "macd_cross_down": 22,
        }
        self.history = defaultdict(list)

    def record_outcome(self, signals: List[str], won: bool):
        pass

    def get_adjusted_weight(self, signal_name: str) -> float:
        return self.weights.get(signal_name, 15)

class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

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
_hot_syms        = deque(maxlen=30)

_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()
_ws_ticker_cache = {}
_ws_ticker_ts    = 0
_ws_last_msg_ts  = time.time()
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

is_logic_inverted = False 

live_positions = {}
trade_log      = []
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)

_last_err_print = defaultdict(float)
_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}

def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now

def _rest_call(tag, fn, *args, retries=1, **kwargs):
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
    except Exception:
        pass
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
    except Exception:
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if _ws_ticker_cache and (now - _ws_ticker_ts) < 15: return _ws_ticker_cache
    try:
        raw = _rest_call("futures_ticker", client.futures_ticker)
        _ticker_cache = {t["symbol"]: {"pct": float(t["priceChangePercent"]), "vol": float(t["quoteVolume"]), "last": float(t["lastPrice"])} for t in raw}
        _ticker_ts = now
    except Exception: pass
    return _ticker_cache

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
    
    df["vm"]  = volume.rolling(20).mean()
    df["vr"]  = volume / df["vm"].replace(0, 1e-9)
    df["delta_ratio"] = 0.0
    df["br"] = 0.5
    df["rng"] = (high - low).replace(0, 1e-9)
    df["lower_wick_ratio"] = 0.0
    df["upper_wick_ratio"] = 0.0
    return df

def run_ta(df):
    return _compute_indicators(df)

def _bootstrap_klines(symbol, interval, limit=100):
    try:
        kl = _rest_call(f"klines_{symbol}", client.futures_klines, symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume"]: df[c] = df[c].astype(float)
        df = _compute_indicators(df)
        with _kline_lock: _kline_cache[symbol] = df
        return df
    except Exception:
        return None

def ohlcv(symbol, interval, limit=100):
    with _kline_lock:
        df = _kline_cache.get(symbol)
    if df is not None: return df
    return _bootstrap_klines(symbol, interval, limit)

def ks_check():
    return False, ""

def set_leverage_safe(symbol, leverage=LEVERAGE):
    try:
        _rest_call(f"leverage_{symbol}", client.futures_change_leverage, symbol=symbol, leverage=leverage)
    except Exception as e:
        _log_err(f"leverage_{symbol}", e)

# ═══════════════════════════════════════════════════════════════════════════
#  BINANCE ORDER EXECUTION (DEMO / REAL)
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

    # Set Leverage di Akun Binance Testnet/Real
    set_leverage_safe(sym, LEVERAGE)

    # 🚀 EXECUTE ORDER KE BINANCE FUTURES
    order_side = "BUY" if execution_side == "LONG" else "SELL"
    try:
        order_res = _rest_call(
            f"open_{sym}",
            client.futures_create_order,
            symbol=sym,
            side=order_side,
            type="MARKET",
            quantity=q_val
        )
        entry_price = float(order_res.get("avgPrice", price))
        if entry_price == 0: entry_price = price
    except Exception as e:
        print(f"❌ [ORDER ERROR] Gagal Membuka Posisi di Binance ({sym}): {e}")
        with _lock: live_positions.pop(sym, None)
        return

    risk = DynamicRiskManager.calculate_levels(entry_price, execution_side, atr)

    pos = {
        "side": execution_side,
        "entry": entry_price,
        "qty": q_val,
        "open_time": time.time(),
        "tp_price": risk["tp_price"],
        "sl_price": risk["sl_price"],
        "order_id": order_res.get("orderId"),
    }

    with _lock: live_positions[sym] = pos

    mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
    env_str = "DEMO TESTNET" if DEMO_TESTNET else "REAL ACCOUNT"
    print(
        f"\n  🚀 [{env_str} ORDER] {sym} EXEC:{execution_side} (Mode:{mode_str}) @{entry_price:.6g} | "
        f"QTY:{q_val} | TP:{risk['tp_price']:.6g} | SL:{risk['sl_price']:.6g}"
    )

    _stats["trades"] += 1

def live_close(sym, reason, price=None):
    global is_logic_inverted

    with _lock: pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    side = pos["side"]
    entry = pos["entry"]
    q_val = pos["qty"]

    # 🚀 CLOSE ORDER DI BINANCE FUTURES
    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        close_res = _rest_call(
            f"close_{sym}",
            client.futures_create_order,
            symbol=sym,
            side=close_side,
            type="MARKET",
            quantity=q_val,
            reduceOnly=True
        )
        exit_price = float(close_res.get("avgPrice", price or 0))
        if exit_price == 0: exit_price = price_live(sym)
    except Exception as e:
        print(f"❌ [CLOSE ERROR] Gagal Menutup Posisi di Binance ({sym}): {e}")
        # Masukkan kembali ke tracking jika gagal close
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
        f"{entry:.6g}→{exit_price:.6g} | PnL:{pnl:+.5f}U"
    )

    _stats["pnl"] += pnl
    if _stats["pnl"] > _stats["ath_pnl"]: _stats["ath_pnl"] = _stats["pnl"]

    if won:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if reason == "SL": _stats["hard_sl"] += 1
    elif reason == "TP": _stats["tp_exit"] += 1
    elif reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

    # DASHBOARD HISTORY RIWAYAT 5 TRADES
    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7),
        "exit": round(exit_price, 7), "pnl": round(pnl, 5),
        "reason": reason, "hold": int(time.time() - pos["open_time"]),
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
#  SCANNER THREADS
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
    except Exception:
        return None

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
    print(f"    📈 Exits -> TP: {_stats['tp_exit']} | SL: {_stats['hard_sl']} | TimeLimit: {_stats['time_limit_exit']}")

    if trade_log:
        print(f"    {'─'*62}\n    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] >= 0 else "🔴"
            print(f"        {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
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
    print("║  4. Real Order Execution ke Binance Futures                        ║")
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
