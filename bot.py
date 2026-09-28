"""
Bot Scalping v22.0 — BINANCE FUTURES DEMO EXECUTION — INVERSE ENTRY / 30M MAX HOLD
================================================================================
MODE:
- Market data: LIVE Binance Futures public market data
- Orders: REAL orders are sent ONLY to Binance Futures DEMO
- REST execution endpoint: https://demo-fapi.binance.com/fapi
- No production/mainnet private order endpoint is used.
- API_KEY / API_SECRET MUST be Binance Demo Trading API credentials.
- Entry: real MARKET order on Binance Demo
- Exit: real MARKET reduceOnly order on Binance Demo
- TP/SL/Time/ProfitGuard logic is kept from the original engine.
- TP = 3.5x ATR (clamped 2.5%-3.5%)
- SL = 1.8x ATR (clamped 1.5%-2.5%)
- Trailing Stop: removed
- SL = ban 3 jam + close posisi lain yang sedang floating loss
- CASCADE_AFTER_SL = ban tambahan 1 jam
- TIME_LIMIT = 2-stage:
    Stage 1: maksimal 30 menit
      * profit > 0 pada menit ke-30 -> GRACE +60 menit
      * profit <= 0 pada menit ke-30 -> TIME_LIMIT
    Stage 2: tambahan 60 menit
      * TP -> TP
      * SL -> SL
      * floating PnL <= 0 -> TIME_LIMIT
      * 90 menit total -> TIME_LIMIT
- Profit Guard dynamic berbasis ATH PnL
- Signal Flip Exit: 1 candle 5m closed berlawanan kuat
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
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

# ============================================================================
# BINANCE FUTURES DEMO — HARD ROUTING LOCK
# ============================================================================
# IMPORTANT:
# This build sends REAL orders, but ONLY to Binance Futures DEMO.
# The endpoint is hard-pinned so a production/private order cannot be used.
DEMO_TRADING = True
LIVE_MARKET_DATA = True
DEMO_FUTURES_BASE_URL = "https://demo-fapi.binance.com"
DEMO_FUTURES_URL = DEMO_FUTURES_BASE_URL + "/fapi"

if not api_key or not api_secret:
    raise RuntimeError("API_KEY/API_SECRET belum diisi. Gunakan API key Binance DEMO Trading.")

client = Client(api_key, api_secret)

# python-binance may select FUTURES_TESTNET_URL when testnet routing is enabled.
# Override BOTH futures URL attributes to guarantee demo-fapi routing.
client.FUTURES_URL = DEMO_FUTURES_URL
client.FUTURES_TESTNET_URL = DEMO_FUTURES_URL

# The market-data websocket remains public market data. Private order/account
# execution is REST-only against the DEMO endpoint above.

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
#  CONFIGURATION & INSTITUTIONAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 2.0
MAX_POSITIONS = 2

# ── LOSS CIRCUIT / LOSS LIQUIDATION ────────────────────────────────────────
SL_BAN_SECONDS = 3 * 60 * 60
CASCADE_BAN_SECONDS = 1 * 60 * 60
TIME_LIMIT_BAN_SECONDS = 1 * 60 * 60
SL_LIQUIDATE_LOSERS = True

# ── PROFIT GUARD / ATH GIVEBACK PROTECTION ─────────────────────────────────
PROFIT_GUARD_ENABLED = True
PROFIT_GUARD_ARM_PNL = 1.50
PROFIT_GUARD_GIVEBACK_PCT_LOW = 0.40
PROFIT_GUARD_GIVEBACK_PCT_MID = 0.35
PROFIT_GUARD_GIVEBACK_PCT_HIGH = 0.30
PROFIT_GUARD_GIVEBACK_PCT_MAX = 0.20
PROFIT_GUARD_GIVEBACK_MIN = 0.50
PROFIT_GUARD_BAN_SECONDS = 2 * 60 * 60
PROFIT_GUARD_CLOSE_LOSERS = True

# ── SIGNAL FLIP EXIT ────────────────────────────────────────────────────────
SIGNAL_FLIP_EXIT_ENABLED = True
SIGNAL_FLIP_MIN_SCORE = 65
SIGNAL_FLIP_CONFIRM_CANDLES = 1
SIGNAL_FLIP_MIN_HOLD_SECONDS = 90

# ── HARD 30-MINUTE TIME LIMIT ──────────────────────────────────────────────
# Semua posisi wajib ditutup maksimal pada menit ke-30.
# - Floating loss pada menit ke-30 -> TIME_LIMIT + BAN ENTRY 1 jam
# - Floating profit pada menit ke-30 -> TIME_LIMIT, TANPA BAN
# - TP / SL / exit lain sebelum 30m -> tidak menambah TIME_BAN
TIME_LIMIT_STAGE1_SECONDS = 30 * 60
TIME_LIMIT_GRACE_SECONDS = 0
MAX_TOTAL_HOLD_SECONDS = TIME_LIMIT_STAGE1_SECONDS
TIME_LIMIT_GRACE_REQUIRE_PROFIT = False
TIME_LIMIT_GRACE_EXIT_ON_LOSS = False

# ── Scanning & Concurrency ─────────────────────────────────────────────────
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01
COOLDOWN_SEC  = 300

# ── REST API SAFETY / ANTI-403 ──────────────────────────────────────────────
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0
REST_418_COOLDOWN = 900.0
REST_RETRIES = 2

# ── Scoring & Filter ────────────────────────────────────────────────────────
MIN_SCORE      = 55
SLIPPAGE_GUARD = 0.0015
TTL_5M         = 2

# ── Dynamic Volatility Risk Management ─────────────────────────────────────
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8
MIN_TP_PCT = 0.025
MAX_TP_PCT = 0.035
MIN_SL_PCT = 0.015
MAX_SL_PCT = 0.025

# Legacy constant kept for compatibility in any external references.
# Actual hold logic is now controlled by the two-stage engine above.
MAX_HOLD_SECONDS = MAX_TOTAL_HOLD_SECONDS

# ── Institutional Microstructure ───────────────────────────────────────────
WALL_RATIO_THRESHOLD  = 2.5
WALL_DEPTH_PCT        = 0.35
WALL_PROXIMITY_PCT    = 0.005
IMBALANCE_STRONG_BULL = 0.25
IMBALANCE_STRONG_BEAR = -0.25
SPOOF_DROP_THRESHOLD  = 0.40

# ── Macro BTC Correlation & Flash Crash Engine ──────────────────────────────
BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# ── Kill Switch ─────────────────────────────────────────────────────────────
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

# ── Learning ────────────────────────────────────────────────────────────────
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
#  1. ORDER BOOK ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    def __init__(self):
        self._cache = {}
        self._history = defaultdict(lambda: deque(maxlen=10))
        self._lock = threading.Lock()

    def update(self, symbol: str, bids_raw: list, asks_raw: list, ts: float = None):
        if ts is None:
            ts = time.time()
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
                    "bids": bids,
                    "asks": asks,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "imbalance": imbalance,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "ts": ts,
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
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]
            tot_ask = book["ask_vol"]
            if not asks or tot_ask <= 0:
                return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)
            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]
            tot_bid = book["bid_vol"]
            if not bids or tot_bid <= 0:
                return False, "OK", 0.0, 0.0, 0.0
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
        if len(hist) < 3:
            return False, ""

        curr_ts, curr_b_vol, curr_a_vol, _, _ = hist[-1]
        for ts, b_vol, a_vol, _, _ in hist[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG" and b_vol > 0:
                    if curr_b_vol < b_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_b_vol / b_vol) * 100
                        return True, f"Bid liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
                elif side == "SHORT" and a_vol > 0:
                    if curr_a_vol < a_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_a_vol / a_vol) * 100
                        return True, f"Ask liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
        return False, ""


order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  2. MACRO BTC & TICK CORRELATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0, "trigger_ts": 0.0}
        self.last_price = 0.0
        self.lock = threading.Lock()

    def update_tick(self, price: float, ts: float = None):
        if ts is None:
            ts = time.time()
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
                        "delta": delta, "trigger_ts": ts,
                    }
                    print(f"\n  🚨 [BTC FLASH CRASH] {delta*100:+.2f}% / {BTC_WINDOW_SEC:.1f}s — Altcoin LONGs locked {BTC_BREAKER_COOLDOWN:.0f}s")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {
                        "active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts,
                    }
                    print(f"\n  🚀 [BTC FLASH PUMP] {delta*100:+.2f}% / {BTC_WINDOW_SEC:.1f}s — Altcoin SHORTs locked {BTC_BREAKER_COOLDOWN:.0f}s")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None:
            now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]
                    delta = self.breaker["delta"]
                    if b_type == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash active ({rem:.0f}s left, drop {delta*100:+.2f}%)"
                    if b_type == "PUMP" and side == "SHORT":
                        return True, f"BTC Flash Pump active ({rem:.0f}s left, surge {delta*100:+.2f}%)"
                else:
                    self.breaker["active"] = False
                    self.breaker["type"] = "NONE"
        return False, "OK"

    def get_status_str(self) -> str:
        with self.lock:
            px = self.last_price
            active = self.breaker["active"] and time.time() < self.breaker["until"]
            if active:
                rem = self.breaker["until"] - time.time()
                return f"BTC: ${px:.1f} | 🚨BREAKER ACTIVE [{self.breaker['type']} {self.breaker['delta']*100:+.2f}% ({rem:.0f}s left)]"
            return f"BTC: ${px:.1f} [NORMAL]"


btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
#  3. ABSORPTION & ORDER FLOW ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[bool, bool, str]:
        if df is None or len(df) < 25:
            return False, False, ""
        row = df.iloc[-2]
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0)
        low = row.get("low", 0.0)
        high = row.get("high", 0.0)
        close = row.get("close", 0.0)
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

        details = []
        if bull_absorb:
            details.append(f"BullAbsorb(Vol:{row.get('vr', 1.0):.1f}x|Δ:{delta_ratio:+.2f}|Wick:{lw_ratio:.0%})")
        if bear_absorb:
            details.append(f"BearAbsorb(Vol:{row.get('vr', 1.0):.1f}x|Δ:{delta_ratio:+.2f}|Wick:{uw_ratio:.0%})")
        return bull_absorb, bear_absorb, " ".join(details)

# ═══════════════════════════════════════════════════════════════════════════
#  4. VOLATILITY-ADJUSTED RISK MANAGEMENT
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

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "atr_pct": atr_pct,
        }

# ═══════════════════════════════════════════════════════════════════════════
#  MARKET REGIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════

class MarketRegime:
    REGIME_TRENDING_BULL = "TRENDING_BULL"
    REGIME_TRENDING_BEAR = "TRENDING_BEAR"
    REGIME_RANGE = "RANGE"
    REGIME_VOLATILE = "VOLATILE"
    REGIME_EXHAUSTION = "EXHAUSTION"

    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < 55:
            return MarketRegime.REGIME_RANGE, 0, 0
        row, prev = df.iloc[-2], df.iloc[-3]
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        atr, atr_prev = row["atr"], prev["atr"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull = close > e9 > e21
        mild_bear = close < e9 < e21
        strong_trend = adx > 25
        very_strong_trend = adx > 35
        atr_expand = (atr / atr_prev) > 1.2 if atr_prev > 0 else False
        atr_collapse = (atr / atr_prev) < 0.8 if atr_prev > 0 else False
        m5, m5_prev = row["m5"], prev["m5"]
        decelerating = (abs(m5) < abs(m5_prev)) if not np.isnan(m5_prev) else False

        if very_strong_trend and bull_stack:
            return MarketRegime.REGIME_TRENDING_BULL, min(adx, 100), 1.0
        if very_strong_trend and bear_stack:
            return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 100), -1.0
        if strong_trend and (bull_stack or mild_bull):
            return MarketRegime.REGIME_TRENDING_BULL, min(adx, 80), 0.7
        if strong_trend and (bear_stack or mild_bear):
            return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 80), -0.7
        if atr_expand and adx < 20:
            return MarketRegime.REGIME_VOLATILE, 50, 0
        if (atr_collapse and decelerating) or (adx > 20 and adx < 35 and decelerating):
            return MarketRegime.REGIME_EXHAUSTION, 40, (1 if m5 > 0 else -1)
        return MarketRegime.REGIME_RANGE, 30, 0

# ═══════════════════════════════════════════════════════════════════════════
#  SCORING & ADAPTIVE SIGNAL WEIGHTS
# ═══════════════════════════════════════════════════════════════════════════

class SignalWeights:
    def __init__(self):
        self.weights = {
            "ema_bull_stack": 30, "ema_mild_bull": 20, "ema_weak_bull": 12,
            "mom_strong": 25, "mom_moderate": 15,
            "macd_cross_up": 22, "macd_strengthen": 15,
            "orderflow_delta_bull": 25, "orderflow_buy_high": 15,
            "absorption_bull": 35, "orderbook_imbalance_bull": 20,
            "rsi_bull_flow": 15, "rsi_extreme_ob": 10,
            "ema_bear_stack": 30, "ema_mild_bear": 20, "ema_weak_bear": 12,
            "mom_strong_neg": 25, "mom_moderate_neg": 15,
            "macd_cross_down": 22, "macd_strengthen_neg": 15,
            "orderflow_delta_bear": 25, "orderflow_sell_high": 15,
            "absorption_bear": 35, "orderbook_imbalance_bear": 20,
            "rsi_bear_flow": 15, "rsi_extreme_os": 10,
        }
        self.history = defaultdict(list)
        self.adaptive_enabled = True

    def record_outcome(self, signals: List[str], won: bool):
        for sig in signals:
            base = sig.split('[')[0].strip()
            if base in self.weights:
                self.history[base].append(1 if won else 0)
                if len(self.history[base]) > LEARNING_WINDOW:
                    self.history[base] = self.history[base][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled:
            return self.weights.get(signal_name, 10)
        base = signal_name.split('[')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT:
            return self.weights.get(base, 10)
        return self.weights.get(base, 10) * max(0.5, min(1.5, 0.5 + sum(hist) / len(hist)))


class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

    def get_signal(self, df: pd.DataFrame, symbol: str = None) -> Tuple[Optional[str], int, List[str], float, str, float]:
        if df is None or len(df) < 55:
            return None, 0, [], 0.0, "UNKNOWN", 0.0

        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)
        atr = df["atr"].iloc[-2]
        bull_absorb, bear_absorb, _ = AbsorptionDetector.detect(df)

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10
            long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10
            short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        if regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        if regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        if regime == MarketRegime.REGIME_VOLATILE:
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        return None, 0, [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50:
            w = self.weights.get_adjusted_weight("ema_bull_stack"); score += w; signals.append(f"EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21:
            w = self.weights.get_adjusted_weight("ema_mild_bull"); score += w; signals.append(f"EMA4↑[{w:.0f}]")
        elif p > e5 > e9:
            w = self.weights.get_adjusted_weight("ema_weak_bull"); score += w; signals.append(f"EMA3↑[{w:.0f}]")

        if row["m5"] > 0.003:
            w = self.weights.get_adjusted_weight("mom_strong"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        elif row["m5"] > 0.0015:
            w = self.weights.get_adjusted_weight("mom_moderate"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")

        if prev["mh"] <= 0 and row["mh"] > 0:
            w = self.weights.get_adjusted_weight("macd_cross_up"); score += w; signals.append(f"MACD_X↑[{w:.0f}]")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]:
            w = self.weights.get_adjusted_weight("macd_strengthen"); score += w; signals.append(f"MACD↑↑[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio > 0.20:
            w = self.weights.get_adjusted_weight("orderflow_delta_bull"); score += w; signals.append(f"ΔBuy+{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio > 0.55:
            w = self.weights.get_adjusted_weight("orderflow_buy_high"); score += w; signals.append(f"TakerBuy{buy_ratio*100:.0f}%[{w:.0f}]")

        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs:
            w = self.weights.get_adjusted_weight("absorption_bull"); score += w; signals.append(f"BullAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bull"); score += w; signals.append(f"BAI+{imb*100:.0f}%[{w:.0f}]")

        if 48 <= row["rsi"] <= 68:
            w = self.weights.get_adjusted_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] > 68:
            w = self.weights.get_adjusted_weight("rsi_extreme_ob"); score += w; signals.append(f"RSI{row['rsi']:.0f}OB[{w:.0f}]")
        return score, signals

    def _score_short(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50:
            w = self.weights.get_adjusted_weight("ema_bear_stack"); score += w; signals.append(f"EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21:
            w = self.weights.get_adjusted_weight("ema_mild_bear"); score += w; signals.append(f"EMA4↓[{w:.0f}]")
        elif p < e5 < e9:
            w = self.weights.get_adjusted_weight("ema_weak_bear"); score += w; signals.append(f"EMA3↓[{w:.0f}]")

        if row["m5"] < -0.003:
            w = self.weights.get_adjusted_weight("mom_strong_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        elif row["m5"] < -0.0015:
            w = self.weights.get_adjusted_weight("mom_moderate_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")

        if prev["mh"] >= 0 and row["mh"] < 0:
            w = self.weights.get_adjusted_weight("macd_cross_down"); score += w; signals.append(f"MACD_X↓[{w:.0f}]")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]:
            w = self.weights.get_adjusted_weight("macd_strengthen_neg"); score += w; signals.append(f"MACD↓↓[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio < -0.20:
            w = self.weights.get_adjusted_weight("orderflow_delta_bear"); score += w; signals.append(f"ΔSell{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio < 0.45:
            w = self.weights.get_adjusted_weight("orderflow_sell_high"); score += w; signals.append(f"TakerSell{(1-buy_ratio)*100:.0f}%[{w:.0f}]")

        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs:
            w = self.weights.get_adjusted_weight("absorption_bear"); score += w; signals.append(f"BearAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bear"); score += w; signals.append(f"BAI{imb*100:.0f}%[{w:.0f}]")

        if 32 <= row["rsi"] <= 52:
            w = self.weights.get_adjusted_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] < 32:
            w = self.weights.get_adjusted_weight("rsi_extreme_os"); score += w; signals.append(f"RSI{row['rsi']:.0f}OS[{w:.0f}]")
        return score, signals

# ═══════════════════════════════════════════════════════════════════════════
#  TRADE RECORDS & LEARNING LAYER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    pnl: float
    won: bool
    regime: str
    signals: List[str]
    score: float
    atr_entry: float
    hold_seconds: float
    exit_reason: str
    peak_pct: float
    timestamp: float = field(default_factory=time.time)


class LearningLayer:
    def __init__(self, signal_weights: SignalWeights):
        self.signal_weights = signal_weights
        self.trades = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self.stats_by_symbol = defaultdict(lambda: {"wins": 0, "losses": 0})

    def add_trade(self, trade: TradeRecord):
        self.trades.append(trade)
        r = trade.regime
        self.stats_by_regime[r]["wins"] += 1 if trade.won else 0
        self.stats_by_regime[r]["losses"] += 0 if trade.won else 1
        self.stats_by_regime[r]["pnl"] += trade.pnl
        if trade.won:
            self.stats_by_regime[r].setdefault("peak_sum", 0.0)
            self.stats_by_regime[r]["peak_sum"] += trade.peak_pct
        self.stats_by_symbol[trade.symbol]["wins"] += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        self.signal_weights.record_outcome(trade.signals, trade.won)
        if len(self.trades) > 1000:
            self.trades = self.trades[-500:]

    def get_global_winrate(self) -> float:
        w = sum(s["wins"] for s in self.stats_by_regime.values())
        l = sum(s["losses"] for s in self.stats_by_regime.values())
        return w / (w + l) if (w + l) > 0 else 0.5

    def avg_win(self) -> float:
        wins = [t.pnl for t in self.trades if t.won]
        return sum(wins) / len(wins) if wins else 0.0

    def avg_loss(self) -> float:
        losses = [abs(t.pnl) for t in self.trades if not t.won]
        return sum(losses) / len(losses) if losses else 0.0

    def avg_peak_win(self) -> float:
        peaks = [t.peak_pct for t in self.trades if t.won]
        return sum(peaks) / len(peaks) if peaks else 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ticker_cache = {}
_ticker_ts = 0
_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q = queue.Queue()
_hot_syms = deque(maxlen=30)

_ws_mark_price = {}
_kline_cache = {}
_kline_lock = threading.Lock()
_ws_ticker_cache = {}
_ws_ticker_ts = 0
_ws_last_msg_ts = time.time()
WS_STALE_SEC = 30
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "regime_block": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "absorb_entries": 0,
    "hist": deque(maxlen=200), "start": time.time(),
    "sl_ban_count": 0, "sl_cascade_closes": 0,
    "cascade_ban_count": 0, "time_limit_ban_count": 0,
    "profit_guard_count": 0, "signal_flip_exits": 0,
    "time_grace_entries": 0,
    "time_grace_exits": 0,
}

live_positions = {}
cooldown_list = {}
trade_log = []
signal_weights = SignalWeights()
scorer = SignalScorer(signal_weights)
learning = LearningLayer(signal_weights)

_last_err_print = defaultdict(float)
_api_fail_streak = 0
_api_ok_last = time.time()
_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}
_leverage_done = set()
_order_state_uncertain = False
_sl_ban_lock = threading.Lock()
_sl_ban_until = 0.0
_sl_ban_reason = ""
_sl_ban_trigger = ""
_cascade_ban_until = 0.0
_cascade_ban_trigger = ""
_time_limit_ban_until = 0.0
_time_limit_ban_trigger = ""
_profit_guard_until = 0.0
_profit_guard_trigger = ""
_profit_guard_triggered_ath = 0.0
_profit_guard_in_progress = False
_sl_cascade_close_count = 0


def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now


def _log_warn(tag, msg, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {msg}")
        _last_err_print[tag] = now


def _api_ok():
    global _api_fail_streak, _api_ok_last
    _api_fail_streak = 0
    _api_ok_last = time.time()


def _api_fail(tag):
    global _api_fail_streak
    _api_fail_streak += 1
    if _api_fail_streak in (20, 100, 300) or _api_fail_streak % 1000 == 0:
        idle = time.time() - _api_ok_last
        print(f"  🚨 API GAGAL BERUNTUN {_api_fail_streak}x (idle {idle:.0f}s) — trigger: {tag}")


def _rest_call(tag, fn, *args, retries=1, **kwargs):
    """Rate-limited REST gate for Binance Futures DEMO market/account/order calls."""
    global _rest_last_ts, _rest_block_until

    last_exc = None
    for attempt in range(retries + 1):
        with _rest_lock:
            wait = max(0.0, _rest_block_until - time.time())
            if wait > 0:
                time.sleep(wait)
            gap = time.time() - _rest_last_ts
            if gap < REST_MIN_INTERVAL:
                time.sleep(REST_MIN_INTERVAL - gap)
            _rest_last_ts = time.time()

        try:
            result = fn(*args, **kwargs)
            _api_ok()
            return result
        except Exception as e:
            last_exc = e
            msg = str(e).upper()
            now = time.time()

            if "403" in msg or "REQUEST BLOCKED" in msg or "CLOUDFRONT" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_403_COOLDOWN)
                _log_warn("REST_403", f"[{tag}] WAF 403 — REST pause {REST_403_COOLDOWN:.0f}s", cooldown=30)
                _api_fail(f"{tag}_403")
                break
            if "418" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_418_COOLDOWN)
                _log_warn("REST_418", f"[{tag}] IP ban 418 — REST pause {REST_418_COOLDOWN:.0f}s", cooldown=30)
                _api_fail(f"{tag}_418")
                break
            if "429" in msg or "TOO MANY REQUESTS" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_429_COOLDOWN)
                _log_warn("REST_429", f"[{tag}] rate limit 429 — backoff {REST_429_COOLDOWN:.0f}s", cooldown=30)
                _api_fail(f"{tag}_429")
                break

            _api_fail(tag)
            if attempt < retries:
                time.sleep(min(2.0, 0.5 * (2 ** attempt)))

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"REST call failed: {tag}")


def get_precision(symbol):
    if symbol in _precision_cache:
        return _precision_cache[symbol]
    try:
        info = _rest_call("futures_exchange_info", client.futures_exchange_info)
        for item in info["symbols"]:
            if item["symbol"] == symbol:
                prec = int(item.get("quantityPrecision", 8))
                _precision_cache[symbol] = prec
                return prec
    except Exception as e:
        _log_err("get_precision", e)
    return 8


_symbol_rules_cache = {}


def _get_symbol_rules(symbol):
    """Return LOT_SIZE / MARKET_LOT_SIZE rules from DEMO exchangeInfo."""
    cached = _symbol_rules_cache.get(symbol)
    if cached:
        return cached
    info = _rest_call("futures_exchange_info_rules", client.futures_exchange_info)
    for item in info.get("symbols", []):
        if item.get("symbol") != symbol:
            continue
        filters = {f.get("filterType"): f for f in item.get("filters", [])}
        lot = filters.get("MARKET_LOT_SIZE") or filters.get("LOT_SIZE") or {}
        min_qty = float(lot.get("minQty", 0) or 0)
        step = float(lot.get("stepSize", 0) or 0)
        rules = {
            "min_qty": min_qty,
            "step_size": step,
            "precision": int(item.get("quantityPrecision", 8)),
        }
        _symbol_rules_cache[symbol] = rules
        return rules
    raise RuntimeError(f"Symbol {symbol} tidak ditemukan di DEMO exchangeInfo")


def qty(symbol, price):
    """Calculate order quantity and round DOWN to Binance DEMO lot-size step."""
    if price <= 0:
        return 0.0
    raw = (ORDER_USDT * LEVERAGE) / price
    rules = _get_symbol_rules(symbol)
    step = rules["step_size"]
    min_qty = rules["min_qty"]
    precision = rules["precision"]

    if step > 0:
        raw = math.floor((raw + 1e-15) / step) * step

    q_val = round(raw, precision)
    if min_qty > 0 and q_val < min_qty:
        return 0.0
    return q_val


def _fmt_qty(symbol, q_val):
    precision = get_precision(symbol)
    return f"{q_val:.{precision}f}"


def _demo_position(symbol):
    """Read the real DEMO Futures position for one symbol."""
    rows = _rest_call(
        f"demo_position_{symbol}",
        client.futures_position_information,
        symbol=symbol,
        retries=0,
    )
    for p in rows or []:
        if p.get("symbol") == symbol:
            amt = float(p.get("positionAmt", 0) or 0)
            if abs(amt) > 0:
                return p
    return None


def _demo_set_leverage(symbol):
    if symbol in _leverage_done:
        return True
    try:
        _rest_call(
            f"demo_set_leverage_{symbol}",
            client.futures_change_leverage,
            symbol=symbol,
            leverage=LEVERAGE,
            retries=0,
        )
        _leverage_done.add(symbol)
        return True
    except Exception as e:
        _log_err(f"demo_set_leverage_{symbol}", e, cooldown=30)
        return False


def _demo_market_open(symbol, side, quantity):
    """Send and verify a REAL MARKET entry on Binance Futures DEMO."""
    global _order_state_uncertain
    if not DEMO_TRADING:
        raise RuntimeError("DEMO_TRADING must remain True")

    order_side = "BUY" if side == "LONG" else "SELL"
    q_str = _fmt_qty(symbol, quantity)

    try:
        response = _rest_call(
            f"demo_entry_{symbol}",
            client.futures_create_order,
            symbol=symbol,
            side=order_side,
            type="MARKET",
            quantity=q_str,
            newOrderRespType="RESULT",
            recvWindow=5000,
            retries=0,
        )

        # Verify that Binance DEMO actually created the position.
        pos = None
        for _ in range(4):
            time.sleep(0.15)
            pos = _demo_position(symbol)
            if pos is not None:
                break
        if pos is None:
            _order_state_uncertain = True
            raise RuntimeError(
                f"ORDER SUBMITTED tetapi posisi DEMO belum terverifikasi: response={response}"
            )

        actual_amt = float(pos.get("positionAmt", 0) or 0)
        expected_sign = 1 if side == "LONG" else -1
        if actual_amt * expected_sign <= 0:
            _order_state_uncertain = True
            raise RuntimeError(
                f"Posisi DEMO terverifikasi tetapi arah tidak cocok: expected={side}, positionAmt={actual_amt}"
            )

        _order_state_uncertain = False
        avg_price = float(response.get("avgPrice", 0) or 0)
        if avg_price <= 0:
            avg_price = float(pos.get("entryPrice", 0) or 0)
        actual_qty = abs(actual_amt)

        print(
            f"  🟢 [BINANCE DEMO ENTRY FILLED] {symbol} {side} "
            f"qty:{actual_qty:.8g} avg:{avg_price:.8g} orderId:{response.get('orderId')}"
        )
        return response, pos, avg_price, actual_qty

    except Exception:
        # Never fabricate a local position after an uncertain order.
        _order_state_uncertain = True
        raise


def _demo_market_close(symbol, side, quantity):
    """Send a REAL reduceOnly MARKET close on Binance Futures DEMO."""
    global _order_state_uncertain
    order_side = "SELL" if side == "LONG" else "BUY"
    q_str = _fmt_qty(symbol, quantity)

    try:
        response = _rest_call(
            f"demo_exit_{symbol}",
            client.futures_create_order,
            symbol=symbol,
            side=order_side,
            type="MARKET",
            quantity=q_str,
            reduceOnly="true",
            newOrderRespType="RESULT",
            recvWindow=5000,
            retries=0,
        )

        remaining = None
        for _ in range(4):
            time.sleep(0.15)
            remaining = _demo_position(symbol)
            if remaining is None:
                break
        if remaining is not None:
            _order_state_uncertain = True
            raise RuntimeError(
                f"Close order terkirim tetapi posisi DEMO masih terbuka: "
                f"positionAmt={remaining.get('positionAmt')}"
            )

        _order_state_uncertain = False
        avg_price = float(response.get("avgPrice", 0) or 0)
        print(
            f"  🔵 [BINANCE DEMO EXIT FILLED] {symbol} {side} "
            f"qty:{quantity:.8g} avg:{avg_price:.8g} orderId:{response.get('orderId')}"
        )
        return response, avg_price

    except Exception:
        _order_state_uncertain = True
        raise


def price_live(symbol):
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if px > 0 and (time.time() - ts) < MARKPRICE_FRESH_SEC:
            return px

    now = time.time()
    old = _rest_price_cache.get(symbol)
    if old and (now - old[1]) < 1.0:
        return old[0]

    try:
        px = float(_rest_call(f"price_live_{symbol}", client.futures_symbol_ticker, symbol=symbol)["price"])
        _rest_price_cache[symbol] = (px, time.time())
        _log_warn(f"price_live_ws_miss_{symbol}", "fallback REST — mark price WS kosong/basi", cooldown=30)
        return px
    except Exception as e:
        _log_err(f"price_live_{symbol}", e)
        _api_fail(f"price_live_{symbol}")
        return 0.0


def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if _ws_ticker_cache and (now - _ws_ticker_ts) < 15:
        return _ws_ticker_cache
    if _ticker_cache and (now - _ticker_ts) < 15:
        return _ticker_cache
    try:
        raw = _rest_call("futures_ticker", client.futures_ticker, retries=1)
        _ticker_cache = {
            t["symbol"]: {
                "pct": float(t["priceChangePercent"]),
                "vol": float(t["quoteVolume"]),
                "last": float(t["lastPrice"]),
            }
            for t in raw
        }
        _ticker_ts = now
        _log_warn("tickers_all_ws_miss", "fallback REST — ticker WS kosong/basi", cooldown=30)
    except Exception as e:
        _log_err("tickers_all", e)
        _api_fail("tickers_all")
    return _ticker_cache


def _compute_indicators(df):
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"].replace(0, 1e-9)
    tbbase = df["tbbase"]

    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    df["mh"] = ta.trend.MACD(close, 12, 26, 9).macd_diff()
    df["e5"] = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"] = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()
    df["vm"] = volume.rolling(20).mean()
    df["vr"] = volume / df["vm"].replace(0, 1e-9)

    taker_buy = tbbase
    taker_sell = (volume - taker_buy).clip(lower=0)
    df["delta"] = taker_buy - taker_sell
    df["delta_ratio"] = df["delta"] / volume
    df["br"] = taker_buy / volume
    df["cvd"] = df["delta"].rolling(10).sum()

    df["rng"] = (high - low).replace(0, 1e-9)
    df["upper_wick"] = high - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - low
    df["body"] = (close - df["open"]).abs()
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]
    df["br2"] = df["body"] / df["rng"]
    df["m5"] = (close - close.shift(5)) / close.shift(5)
    df["m3"] = (close - close.shift(3)) / close.shift(3)
    return df


def run_ta(df):
    if "delta_ratio" not in df.columns or "rsi" not in df.columns:
        df = _compute_indicators(df)
    return df


def _bootstrap_klines(symbol, interval, limit=100):
    try:
        kl = _rest_call(
            f"bootstrap_klines_{symbol}",
            client.futures_klines,
            symbol=symbol,
            interval=interval,
            limit=limit,
        )
        df = pd.DataFrame(kl, columns=["time", "open", "high", "low", "close", "volume", "ct", "qv", "trades", "tbbase", "tbquote", "ignore"])
        for c in ["open", "high", "low", "close", "volume", "tbbase", "tbquote"]:
            df[c] = df[c].astype(float)
        df = _compute_indicators(df)
        with _kline_lock:
            _kline_cache[symbol] = df
        _api_ok()
        return df
    except Exception as e:
        _log_err(f"bootstrap_klines_{symbol}", e)
        _api_fail(f"bootstrap_klines_{symbol}")
        return None


def _append_kline_from_ws(symbol, k):
    try:
        base_cols = ["time", "open", "high", "low", "close", "volume", "ct", "qv", "trades", "tbbase", "tbquote", "ignore"]
        new_row = {
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
            df = _kline_cache.get(symbol)
            if df is None:
                return
            if len(df) > 0 and int(df.iloc[-1]["time"]) == new_row["time"]:
                df = df.iloc[:-1]
            df_base = df[base_cols] if all(c in df.columns for c in base_cols) else df
            df_base = pd.concat([df_base, pd.DataFrame([new_row])], ignore_index=True)
            if len(df_base) > 300:
                df_base = df_base.iloc[-300:].reset_index(drop=True)
            _kline_cache[symbol] = _compute_indicators(df_base)
    except Exception as e:
        _log_err(f"append_kline_{symbol}", e)


def bootstrap_all_klines(syms):
    """Bootstrap 5m history for all paper-trading symbols using public market data only."""
    print(f"  📥 Bootstrap history awal ({len(syms)} simbol) via LIVE REST — paced/anti-403...")
    ok = 0
    for i, s in enumerate(syms, 1):
        try:
            if _bootstrap_klines(s, Client.KLINE_INTERVAL_5MINUTE, 100) is not None:
                ok += 1
        except Exception as e:
            _log_err(f"bootstrap_all_{s}", e, cooldown=30)
        if i < len(syms):
            time.sleep(REST_MIN_INTERVAL)
    print(f"  ✅ Bootstrap selesai: {ok}/{len(syms)} simbol siap dipantau")


def ohlcv(symbol, interval, limit=100):
    with _kline_lock:
        df = _kline_cache.get(symbol)
    if df is not None:
        return df
    return _bootstrap_klines(symbol, interval, limit)


def _fmt_ban(label: str, remaining: float) -> str:
    if remaining <= 0:
        return ""
    h = int(remaining // 3600)
    m = int((remaining % 3600) // 60)
    sec = int(remaining % 60)
    return f"{label} {h:02d}:{m:02d}:{sec:02d}"


def _sl_ban_remaining():
    with _sl_ban_lock:
        rem = _sl_ban_until - time.time()
        return max(0.0, rem)


def _circuit_snapshot():
    now = time.time()
    with _sl_ban_lock:
        items = [
            ("SL_BAN", max(0.0, _sl_ban_until - now)),
            ("CASCADE_BAN", max(0.0, _cascade_ban_until - now)),
            ("TIME_BAN", max(0.0, _time_limit_ban_until - now)),
            ("PROFIT_GUARD", max(0.0, _profit_guard_until - now)),
        ]
    active = [(name, rem) for name, rem in items if rem > 0]
    if not active:
        return "", 0.0
    name, rem = max(active, key=lambda x: x[1])
    return _fmt_ban(name, rem), rem


def _sl_ban_status():
    rem = _sl_ban_remaining()
    return _fmt_ban("SL_BAN", rem)


def _activate_aux_ban(kind: str, seconds: float, trigger_sym: str):
    global _cascade_ban_until, _cascade_ban_trigger
    global _time_limit_ban_until, _time_limit_ban_trigger
    global _profit_guard_until, _profit_guard_trigger

    until = time.time() + seconds
    with _sl_ban_lock:
        if kind == "CASCADE":
            _cascade_ban_until = max(_cascade_ban_until, until)
            _cascade_ban_trigger = trigger_sym
            _stats["cascade_ban_count"] += 1
            label = "CASCADE_BAN"
        elif kind == "TIME_LIMIT":
            _time_limit_ban_until = max(_time_limit_ban_until, until)
            _time_limit_ban_trigger = trigger_sym
            _stats["time_limit_ban_count"] += 1
            label = "TIME_BAN"
        elif kind == "PROFIT_GUARD":
            _profit_guard_until = max(_profit_guard_until, until)
            _profit_guard_trigger = trigger_sym
            _stats["profit_guard_count"] += 1
            label = "PROFIT_GUARD"
        else:
            return
        active_until = {
            "CASCADE_BAN": _cascade_ban_until,
            "TIME_BAN": _time_limit_ban_until,
            "PROFIT_GUARD": _profit_guard_until,
        }[label]

    print(f"  🛑 [{label}] {trigger_sym} — DEMO entry baru diblokir sampai {time.strftime('%H:%M:%S', time.localtime(active_until))}")


def _estimate_floating_pnl(pos, price):
    try:
        entry = float(pos.get("entry", 0) or 0)
        q_val = float(pos.get("qty", 0) or 0)
        side = pos.get("side")
        if entry <= 0 or price <= 0 or q_val <= 0 or side not in ("LONG", "SHORT"):
            return 0.0
        gross = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
        fee_rate = 0.0005
        est_fee = (entry * q_val + price * q_val) * fee_rate
        return gross - est_fee
    except Exception:
        return 0.0


def _mark_price_only(sym):
    cached = _ws_mark_price.get(sym)
    if cached:
        px, ts = cached
        if px > 0 and (time.time() - ts) < MARKPRICE_FRESH_SEC:
            return px
    return 0.0


def _liquidate_losing_positions(reason: str, exclude=None):
    exclude = set(exclude or [])
    candidates = []

    for sym in list(live_positions.keys()):
        if sym in exclude:
            continue
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue

        px = _mark_price_only(sym)
        if px <= 0:
            try:
                px = price_live(sym)
            except Exception:
                px = 0.0
        if px <= 0:
            print(f"  ⚠️ [{reason}] {sym}: harga floating tidak tersedia — DEMO position dipertahankan")
            continue

        fpnl = _estimate_floating_pnl(pos, px)
        if fpnl < 0:
            candidates.append((sym, px, fpnl))
        else:
            print(f"  ✅ [{reason}] {sym} {pos.get('side', '?')} dipertahankan | floating:{fpnl:+.5f}U")

    for sym, px, fpnl in candidates:
        print(f"  🔻 [{reason}] {sym} ditutup | floating:{fpnl:+.5f}U")
        before = sym in live_positions
        live_close(sym, reason, px)
        if reason == "CASCADE_AFTER_SL" and before and sym not in live_positions:
            global _sl_cascade_close_count
            _sl_cascade_close_count += 1
            _stats["sl_cascade_closes"] += 1

    return len(candidates)


def _activate_sl_ban_and_liquidate(trigger_sym):
    global _sl_ban_until, _sl_ban_reason, _sl_ban_trigger
    now = time.time()
    with _sl_ban_lock:
        _sl_ban_until = max(_sl_ban_until, now + SL_BAN_SECONDS)
        _sl_ban_reason = "SL"
        _sl_ban_trigger = trigger_sym
        _stats["sl_ban_count"] += 1
        ban_until_local = _sl_ban_until

    print(f"\n  🛑 [SL CIRCUIT BAN] {trigger_sym} kena SL — DEMO entry dikunci 3 JAM sampai {time.strftime('%H:%M:%S', time.localtime(ban_until_local))}")
    if SL_LIQUIDATE_LOSERS:
        _liquidate_losing_positions("CASCADE_AFTER_SL", exclude={trigger_sym})


def _maybe_activate_profit_guard():
    global _profit_guard_triggered_ath, _profit_guard_in_progress

    if not PROFIT_GUARD_ENABLED:
        return
    pnl = _stats["pnl"]
    ath = _stats["ath_pnl"]
    if ath < PROFIT_GUARD_ARM_PNL:
        return

    if ath < 2.50:
        giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_LOW
    elif ath < 5.00:
        giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_MID
    elif ath < 10.00:
        giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_HIGH
    else:
        giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_MAX

    giveback = max(PROFIT_GUARD_GIVEBACK_MIN, ath * giveback_pct)
    floor = ath - giveback
    if pnl > floor:
        return
    if ath <= _profit_guard_triggered_ath + 1e-9:
        return
    if _profit_guard_in_progress:
        return

    _profit_guard_triggered_ath = ath
    _profit_guard_in_progress = True
    try:
        _activate_aux_ban("PROFIT_GUARD", PROFIT_GUARD_BAN_SECONDS, "ATH_GIVEBACK")
        print(f"  🧱 [PROFIT GUARD] PnL {pnl:+.4f}U turun dari ATH {ath:+.4f}U melewati floor {floor:+.4f}U | giveback:{ath-pnl:+.4f}U")
        if PROFIT_GUARD_CLOSE_LOSERS:
            _liquidate_losing_positions("PROFIT_GUARD")
    finally:
        _profit_guard_in_progress = False


def ks_check():
    k, now = _ks, time.time()
    circuit, remaining = _circuit_snapshot()
    if remaining > 0:
        return True, circuit
    if _order_state_uncertain:
        return True, "ORDER_STATE_UNKNOWN — paper engine dihentikan sementara"
    if k["active"] and now >= k["resume"]:
        k["active"], k["consec"] = False, 0
    if k["active"]:
        return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"], k["day_reset"] = 0.0, day
    if k["daily"] <= DAILY_LOSS:
        k["active"], k["reason"], k["resume"] = True, f"daily({k['daily']:.2f})", day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"], k["reason"], k["resume"] = True, f"consec({k['consec']})", now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""


def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════════════════════════
#  5. PAPER EXECUTION & POSITION MONITORING
# ═══════════════════════════════════════════════════════════════════════════


def live_open(orig_direction, score, sigs, price, atr, regime, bias, sym, risk_profile):
    """Open a REAL position on Binance Futures DEMO and mirror the verified fill locally."""
    if not DEMO_TRADING:
        raise RuntimeError("DEMO_TRADING must remain True")
    if orig_direction not in ("LONG", "SHORT"):
        return
    # INVERSE ENTRY: signal LONG -> actual SHORT, signal SHORT -> actual LONG.
    execution_side = "SHORT" if orig_direction == "LONG" else "LONG"
    if _order_state_uncertain:
        print(f"  ⛔ [{sym}] ENTRY DIBLOKIR: ORDER_STATE_UNKNOWN")
        return

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}

    try:
        px_now = price_live(sym)
        if px_now > 0:
            price = px_now
        if price <= 0:
            raise ValueError("Harga entry tidak tersedia")

        if not _demo_set_leverage(sym):
            raise RuntimeError(f"Gagal set leverage DEMO {LEVERAGE}x untuk {sym}")

        q_val = qty(sym, price)
        if q_val <= 0:
            raise ValueError(
                f"Quantity DEMO <= 0 setelah LOT_SIZE rounding untuk {sym}. "
                f"ORDER_USDT={ORDER_USDT}, leverage={LEVERAGE}"
            )

        response, account_pos, fill_price, actual_qty = _demo_market_open(
            sym, execution_side, q_val
        )

        if fill_price <= 0:
            raise RuntimeError(f"Fill price DEMO tidak valid: {fill_price}")

        new_risk = DynamicRiskManager.calculate_levels(
            fill_price, execution_side, atr
        )
        now = time.time()

        pos = {
            "side": execution_side,
            "orig_signal": orig_direction,
            "entry": fill_price,
            "qty": actual_qty,
            "open_time": now,
            "score": score,
            "sigs": sigs,
            "atr": atr,
            "regime": regime,
            "bias": bias,
            "tp_pct": new_risk["tp_pct"],
            "sl_pct": new_risk["sl_pct"],
            "tp_price": new_risk["tp_price"],
            "sl_price": new_risk["sl_price"],
            "peak_price": fill_price,
            "order_id": response.get("orderId"),
            "account_entry_price": float(account_pos.get("entryPrice", fill_price) or fill_price),
            "time_stage": 1,
            "stage1_deadline": now + TIME_LIMIT_STAGE1_SECONDS,
            "grace_until": None,
            "grace_start_pnl": None,
            "_time_grace_warned": False,
        }

        with _lock:
            live_positions[sym] = pos

        print(
            f"\n  🚀 [DEMO REAL ENTRY] {sym} SIGNAL:{orig_direction} -> ENTRY:{execution_side} @ {fill_price:.8g} "
            f"| qty:{actual_qty:.8g} | leverage:{LEVERAGE}x "
            f"| TP:{new_risk['tp_pct']*100:.2f}% | SL:{new_risk['sl_pct']*100:.2f}%"
        )
        print(f"         Signals: {' | '.join(sigs[:6])}")
        _stats["trades"] += 1
        if any("Absorb" in s for s in sigs):
            _stats["absorb_entries"] += 1

    except Exception as e:
        with _lock:
            live_positions.pop(sym, None)
        _log_err(f"DEMO_ENTRY_{sym}", e, cooldown=2)
        print(f"  ❌ [DEMO ENTRY GAGAL] {sym} SIGNAL:{orig_direction} -> ENTRY:{execution_side}: {e}")


def live_close(sym, reason, price=None):
    """Close the REAL Binance Futures DEMO position, then record the verified fill locally."""
    global _order_state_uncertain

    if not DEMO_TRADING:
        raise RuntimeError("DEMO_TRADING must remain True")

    with _lock:
        pos = live_positions.get(sym)
    if pos is None or pos.get("_r"):
        return

    # Always use the actual exchange position size, not a simulated quantity.
    try:
        account_pos = _demo_position(sym)
    except Exception as e:
        _order_state_uncertain = True
        _log_err(f"DEMO_POSITION_BEFORE_CLOSE_{sym}", e, cooldown=2)
        return

    if account_pos is None:
        # Position may already have been closed outside this process.
        print(f"  ⚠️ [DEMO SYNC] {sym}: posisi lokal ada tetapi posisi DEMO sudah tidak ada.")
        with _lock:
            live_positions.pop(sym, None)
        _order_state_uncertain = False
        return

    actual_amt = float(account_pos.get("positionAmt", 0) or 0)
    if abs(actual_amt) <= 0:
        with _lock:
            live_positions.pop(sym, None)
        return

    side = "LONG" if actual_amt > 0 else "SHORT"
    q_val = abs(actual_amt)
    entry = float(account_pos.get("entryPrice", pos.get("entry", 0)) or pos.get("entry", 0))

    # Price is only used as fallback for local accounting; exchange fill is preferred.
    if price is None or price <= 0:
        price = price_live(sym)

    try:
        response, fill_price = _demo_market_close(sym, side, q_val)
        if fill_price <= 0:
            fill_price = price_live(sym)
        if fill_price <= 0:
            fill_price = price
        if fill_price <= 0:
            raise RuntimeError("Harga fill exit DEMO tidak tersedia")

    except Exception as e:
        # Keep local position because the exchange position could still be open.
        with _lock:
            live_positions[sym] = pos
        _log_err(f"DEMO_EXIT_{sym}", e, cooldown=2)
        print(f"  ❌ [DEMO EXIT GAGAL] {sym} {side}: {e}")
        return

    with _lock:
        live_positions.pop(sym, None)

    gross_pnl = (fill_price - entry) * q_val if side == "LONG" else (entry - fill_price) * q_val
    fee_rate = 0.0005
    total_fee = (entry * q_val + fill_price * q_val) * fee_rate
    pnl = gross_pnl - total_fee
    pct = (fill_price - entry) / entry * 100 if side == "LONG" else (entry - fill_price) / entry * 100
    hold = time.time() - pos["open_time"]
    won = pnl >= 0
    e_icon = "🟢" if won else "🔴"

    peak_px = pos.get("peak_price", entry)
    peak_pct = (peak_px - entry) / entry if side == "LONG" else (entry - peak_px) / entry

    print(
        f"  {e_icon} [BINANCE DEMO EXIT] {sym} {side} CLOSE — {reason} "
        f"| peak:{peak_pct*100:+.3f}%"
    )
    print(
        f"     {entry:.8g}→{fill_price:.8g} ({pct:+.3f}%) hold:{hold:.0f}s "
        f"| PnL:{pnl:+.5f}U"
    )

    trade = TradeRecord(
        symbol=sym,
        direction=side,
        entry_price=entry,
        exit_price=fill_price,
        pnl=pnl,
        won=won,
        regime=pos.get("regime", "UNKNOWN"),
        signals=pos.get("sigs", []),
        score=pos.get("score", 0),
        atr_entry=pos.get("atr", 0),
        hold_seconds=hold,
        exit_reason=reason,
        peak_pct=peak_pct,
    )
    learning.add_trade(trade)

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    if _stats["pnl"] > _stats["ath_pnl"]:
        _stats["ath_pnl"] = _stats["pnl"]

    ks_upd(pnl)

    if won:
        _stats["wins"] += 1
        if pnl > _stats["best"]:
            _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]:
            _stats["worst"] = pnl

    if reason == "SL":
        _stats["hard_sl"] += 1
    elif reason == "TP":
        _stats["tp_exit"] += 1
    elif reason == "TIME_LIMIT_GRACE_DRAWDOWN":
        _stats["time_grace_exits"] += 1

    trade_log.append({
        "sym": sym,
        "side": side,
        "entry": round(entry, 7),
        "exit": round(fill_price, 7),
        "pnl": round(pnl, 5),
        "reason": reason,
        "hold": int(hold),
        "order_id": response.get("orderId"),
    })

    if reason == "SL":
        _activate_sl_ban_and_liquidate(sym)
    elif reason == "CASCADE_AFTER_SL":
        _activate_aux_ban("CASCADE", CASCADE_BAN_SECONDS, sym)
    elif reason == "TIME_LIMIT" and pos.get("_time_limit_ban", False):
        # Hanya TIME_LIMIT yang benar-benar LOSS pada menit ke-30
        # yang mengaktifkan ban entry 1 jam. Profit-at-time-limit tidak ban.
        _activate_aux_ban("TIME_LIMIT", TIME_LIMIT_BAN_SECONDS, sym)

    _maybe_activate_profit_guard()

    with _lock:
        cooldown_list[sym] = time.time() + COOLDOWN_SEC
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()



def _check_time_limit_stage(sym, pos, px):
    """Hard 30-minute max hold. Loss at timeout gets 1h entry ban; profit does not."""
    hold_time = time.time() - pos["open_time"]
    if hold_time < TIME_LIMIT_STAGE1_SECONDS:
        return False

    floating_pnl = _estimate_floating_pnl(pos, px)
    # Mark the exact timeout state before live_close() records the trade.
    # This prevents a profitable TIME_LIMIT close from receiving a TIME_BAN.
    pos["_time_limit_ban"] = floating_pnl < 0

    if floating_pnl < 0:
        print(
            f"  ⏰ {sym}: HARD TIME_LIMIT {hold_time/60:.1f}m | "
            f"Float:{floating_pnl:+.5f}U < 0 → close + TIME_BAN 1 jam"
        )
    else:
        print(
            f"  ⏰ {sym}: HARD TIME_LIMIT {hold_time/60:.1f}m | "
            f"Float:{floating_pnl:+.5f}U >= 0 → close TANPA TIME_BAN"
        )

    live_close(sym, "TIME_LIMIT", px)
    return True


def _check_signal_flip_exit(sym, pos):
    if not SIGNAL_FLIP_EXIT_ENABLED:
        return False
    if time.time() - pos.get("open_time", time.time()) < SIGNAL_FLIP_MIN_HOLD_SECONDS:
        return False

    with _kline_lock:
        df = _kline_cache.get(sym)
    if df is None or len(df) < 55:
        return False

    try:
        candle_key = int(df.iloc[-2]["time"])
    except Exception:
        return False

    if candle_key == pos.get("_signal_checked_candle"):
        return False
    pos["_signal_checked_candle"] = candle_key

    try:
        df_ta = run_ta(df.copy())
        signal, score, _, _, regime, _ = scorer.get_signal(df_ta, sym)
    except Exception as e:
        _log_err(f"signal_flip_{sym}", e, cooldown=30)
        return False

    side = pos.get("side")
    opposite = "SHORT" if side == "LONG" else "LONG"

    if signal == opposite and score >= SIGNAL_FLIP_MIN_SCORE:
        last_dir = pos.get("_flip_candidate")
        count = pos.get("_flip_count", 0) + 1 if last_dir == signal else 1
        pos["_flip_candidate"] = signal
        pos["_flip_count"] = count

        print(f"  🔄 [SIGNAL FLIP] {sym} {side} -> {signal} | score:{score:.0f} | regime:{regime} | confirm:{count}/{SIGNAL_FLIP_CONFIRM_CANDLES}")
        if count >= SIGNAL_FLIP_CONFIRM_CANDLES:
            _stats["signal_flip_exits"] += 1
            live_close(sym, "SIGNAL_FLIP")
            return True
    else:
        pos["_flip_candidate"] = None
        pos["_flip_count"] = 0
    return False


def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue

        px = price_live(sym)
        if px == 0:
            pos["_fail_count"] = pos.get("_fail_count", 0) + 1
            fc = pos["_fail_count"]
            if fc in (5, 20, 60) or fc % 300 == 0:
                print(f"  ⚠️ {sym}: price_live gagal {fc}x — TP/SL/Time monitoring tertunda")
            continue
        pos["_fail_count"] = 0

        side, tp_px, sl_px = pos["side"], pos["tp_price"], pos["sl_price"]

        if side == "LONG":
            if px > pos["peak_price"]:
                pos["peak_price"] = px
        else:
            if px < pos["peak_price"]:
                pos["peak_price"] = px

        # TP/SL take precedence at every moment, including exactly at 30m.
        if side == "LONG":
            if px >= tp_px:
                live_close(sym, "TP", tp_px)
                continue
            if px <= sl_px:
                live_close(sym, "SL", sl_px)
                continue
        else:
            if px <= tp_px:
                live_close(sym, "TP", tp_px)
                continue
            if px >= sl_px:
                live_close(sym, "SL", sl_px)
                continue

        # New two-stage time engine.
        if _check_time_limit_stage(sym, pos, px):
            continue

        # Cut only on a strong closed 5m opposite signal.
        _check_signal_flip_exit(sym, pos)

# ═══════════════════════════════════════════════════════════════════════════
#  6. SCANNER THREAD & HARD VETO FILTERS
# ═══════════════════════════════════════════════════════════════════════════


def scan_one(sym):
    try:
        time.sleep(0.002)
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None:
            return None
        df_ta = run_ta(df.copy())
        px_candle, atr_val = df_ta["close"].iloc[-2], df_ta["atr"].iloc[-2]
        if px_candle == 0 or np.isnan(atr_val):
            return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df_ta, sym)
        if orig_direction is None:
            return None
        if orig_direction not in ("LONG", "SHORT"):
            return None
        # INVERSE ENTRY MODE: hasil analisa sengaja dieksekusi berlawanan.
        # Analisa tetap dicatat sebagai orig_direction, tetapi order/veto/risk
        # memakai execution_side yang sudah dibalik.
        execution_side = "SHORT" if orig_direction == "LONG" else "LONG"

        px_live = price_live(sym)
        if px_live == 0:
            return None

        btc_vetoed, btc_reason = btc_macro.check_veto(execution_side)
        if btc_vetoed:
            _stats["btc_breaker_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED by BTC Circuit Breaker: {btc_reason}")
            return None

        has_wall, wall_type, wall_px, wall_qty, wall_mult = order_book.check_walls(sym, px_live, execution_side)
        if has_wall:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED by {wall_type} @ {wall_px:.6g} (qty:{wall_qty:.1f}, {wall_mult:.1f}x avg depth)")
            return None

        is_spoof, spoof_reason = order_book.detect_spoofing(sym, execution_side)
        if is_spoof:
            _stats["spoof_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED: Spoofing detected ({spoof_reason})")
            return None

        imb = order_book.get_imbalance(sym)
        if execution_side == "LONG" and imb < -0.40:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] LONG VETOED: Heavy Ask Queue Imbalance ({imb*100:.0f}%)")
            return None
        if execution_side == "SHORT" and imb > 0.40:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] SHORT VETOED: Heavy Bid Queue Imbalance ({imb*100:.0f}%)")
            return None

        risk_profile = DynamicRiskManager.calculate_levels(px_live, execution_side, atr_val)
        return (sym, orig_direction, score, sigs, px_live, atr_val, regime, bias, risk_profile)
    except Exception as e:
        _log_err(f"scan_one_{sym}", e)
        return None


def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    for f in as_completed(fut, timeout=5):
        try:
            r = f.result(timeout=1)
            if r:
                res.append(r)
        except Exception:
            pass
    return res


def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]


def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    aw = learning.avg_win()
    avg_pk = learning.avg_peak_win()
    e = "💚" if pnl >= 0 else "🔴"
    print(f"       ┌ [DEMO ENGINE v22] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U)")
    circuit, _ = _circuit_snapshot()
    print(f"       └ TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} Absorb:{_stats['absorb_entries']} | Circuit:{circuit or 'READY'} | Cascade:{_stats['sl_cascade_closes']} | FlipExit:{_stats['signal_flip_exits']} | Grace:{_stats['time_grace_entries']} | AvgWin:{aw:+.4f}U | Peak:{avg_pk*100:.3f}%")


def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    sess = (time.time() - _stats["start"]) / 3600
    tph = n / sess if sess > 0 else 0
    e = "💚" if pnl >= 0 else "🔴"
    aw, al = learning.avg_win(), learning.avg_loss()
    bep = al / (al + aw) * 100 if (al + aw) > 0 else 50

    print(f"\n  {'─'*72}")
    print(f"    🔔 INSTITUTIONAL SCALPING v22 DEMO — LIVE MARKET DATA")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} ({tph:.1f}T/hr)")
    print(f"    {e} PnL Net:{pnl:+.5f}U | ATH PnL:{_stats['ath_pnl']:+.5f}U | Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    print(f"    📈 Exit: TP:{_stats['tp_exit']} | SL:{_stats['hard_sl']} | Grace:{_stats['time_grace_entries']} | GraceDrawdown:{_stats['time_grace_exits']}")

    circuit, _ = _circuit_snapshot()
    if _stats["ath_pnl"] >= PROFIT_GUARD_ARM_PNL:
        ath = _stats["ath_pnl"]
        if ath < 2.50:
            giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_LOW
        elif ath < 5.00:
            giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_MID
        elif ath < 10.00:
            giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_HIGH
        else:
            giveback_pct = PROFIT_GUARD_GIVEBACK_PCT_MAX
        giveback = max(PROFIT_GUARD_GIVEBACK_MIN, ath * giveback_pct)
        profit_floor = ath - giveback
        guard_info = f" | Floor:{profit_floor:+.3f}U ({giveback_pct:.0%} GB)"
    else:
        guard_info = ""

    print(f"    🛑 Circuit: {circuit if circuit else 'READY'} | SL:{_stats['sl_ban_count']} | CascadeBan:{_stats['cascade_ban_count']} | TimeBan:{_stats['time_limit_ban_count']}")
    print(f"    🧱 ProfitGuard:{_stats['profit_guard_count']} | FlipExit:{_stats['signal_flip_exits']} | Cascade Close:{_stats['sl_cascade_closes']} | ATH{guard_info}")
    print(f"    🕐 TIME ENGINE: HARD MAX 30m | Loss@30m -> BAN 1h | Profit@30m -> NO BAN")
    print(f"    🛡️ Veto Stats: Wall Veto:{_stats['wall_veto']} | BTC Breaker:{_stats['btc_breaker_veto']} | Spoof:{_stats['spoof_veto']}")
    print(f"    ⚡ Absorption Entries: {_stats['absorb_entries']} | BEP WR:{bep:.1f}%")

    if trade_log:
        print(f"    {'─'*62}\n    📋 Last 5:")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] > 0 else "🔴"
            print(f"       {em} {t['sym']:<16} {t['side']} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']}")
    print(f"  {'─'*72}")


def t_monitor():
    while True:
        try:
            if live_positions:
                monitor_positions()
        except Exception as e:
            _log_err("t_monitor", e)
        time.sleep(MONITOR_INT)


def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT)
                continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and (s not in cooldown_list or now > cooldown_list[s])]

            hot = [s for s in _hot_syms if s in valid_syms]
            mv = top_movers(valid_syms, 30)
            bs = scan_idx * BATCH_SIZE
            reg = [s for s in valid_syms[bs:bs+BATCH_SIZE] if s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            scan_list = list(dict.fromkeys(hot[:5] + mv[:20] + reg[:15]))[:BATCH_SIZE]

            if not scan_list:
                time.sleep(SLOT_FILL_INT)
                continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS:
                        break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except Exception as e:
            _log_err("t_slot_filler", e)
        time.sleep(SLOT_FILL_INT)


def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and (s not in cooldown_list or now > cooldown_list[s])]

            hot = [s for s in _hot_syms if s in valid_syms]
            rest = [s for s in valid_syms if s not in hot]
            res = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS:
                        break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except Exception as e:
            _log_err("t_rescan", e, cooldown=15)


def t_macro():
    while True:
        try:
            df_btc = ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80)
            if df_btc is not None and len(df_btc) >= 55:
                regime, strength, bias = MarketRegime.detect(df_btc)
                _macro["btc"] = regime
                _btc_macro["regime"] = regime
                row = df_btc.iloc[-2]
                _btc_macro["m5"] = row.get("m5", 0.0)
                _btc_macro["delta_ratio"] = row.get("delta_ratio", 0.0)
                _btc_macro["cvd"] = row.get("cvd", 0.0)
        except Exception as e:
            _log_err("t_macro", e)
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  7. WEBSOCKET HANDLERS & WATCHDOG
# ═══════════════════════════════════════════════════════════════════════════


def handle_all_ticker(msg):
    global _ws_last_msg_ts, _ws_ticker_cache, _ws_ticker_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        arr = data if isinstance(data, list) else [data]
        cache = {}
        for d in arr:
            if not isinstance(d, dict):
                continue
            sym = d.get("s")
            if not sym:
                continue
            try:
                cache[sym] = {
                    "pct": float(d.get("P", 0)),
                    "vol": float(d.get("q", 0)),
                    "last": float(d.get("c", 0)),
                }
            except (TypeError, ValueError):
                continue
        if cache:
            _ws_ticker_cache = cache
            _ws_ticker_ts = time.time()
    except Exception as e:
        _log_err("handle_all_ticker", e)


def handle_mark_price(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        if isinstance(msg, dict) and "data" in msg:
            data = msg.get("data")
            arr = data if isinstance(data, list) else [data]
        elif isinstance(msg, list):
            arr = msg
        else:
            arr = [msg]

        now = time.time()
        for d in arr:
            if not isinstance(d, dict):
                continue
            sym, px = d.get("s"), d.get("p")
            if sym and px:
                pf = float(px)
                if pf > 0:
                    _ws_mark_price[sym] = (pf, now)
    except Exception as e:
        _log_err("handle_mark_price", e)


def handle_kline_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        k = data.get("k")
        if not k:
            return
        sym = data.get("s") or k.get("s")
        # Cache only CLOSED 5m candles for stable signal calculations.
        if sym and k.get("x"):
            _append_kline_from_ws(sym, k)
    except Exception as e:
        _log_err("handle_kline_multiplex", e)


def handle_btc_aggtrade(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict):
            return
        p = data.get("p")
        t = data.get("T")
        if p is not None:
            ts = float(t) / 1000.0 if t else time.time()
            btc_macro.update_tick(float(p), ts)
    except Exception as e:
        _log_err("handle_btc_aggtrade", e)


def handle_depth_multiplex(msg):
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
            bids = data.get("b", [])
            asks = data.get("a", [])
            order_book.update(sym, bids, asks)
    except Exception as e:
        _log_err("handle_depth_multiplex", e)


def t_ws_watchdog():
    while True:
        idle = time.time() - _ws_last_msg_ts
        if idle > WS_STALE_SEC:
            print(f"  🚨 WEBSOCKET DIAM {idle:.0f}s — REST fallback dibatasi rate limiter/backoff")
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  8. BOT LAUNCHER & MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════


def demo_preflight_account(syms):
    """Verify API credentials are valid for Binance Futures DEMO and no bot position is already open."""
    global _order_state_uncertain

    if not DEMO_TRADING:
        raise RuntimeError("DEMO_TRADING harus True")

    if "demo-fapi.binance.com" not in str(getattr(client, "FUTURES_URL", "")):
        raise RuntimeError(
            f"SAFETY ROUTING FAILED: Futures URL bukan DEMO: {getattr(client, 'FUTURES_URL', None)}"
        )

    try:
        account = _rest_call(
            "demo_account_preflight",
            client.futures_account,
            retries=0,
        )
        if not isinstance(account, dict):
            raise RuntimeError("response futures_account DEMO tidak valid")

        print(
            f"  ✅ DEMO API terhubung | canTrade:{account.get('canTrade')} "
            f"| availableBalance:{account.get('availableBalance', 'n/a')} USDT"
        )
    except Exception as e:
        raise RuntimeError(
            "API_KEY/API_SECRET tidak valid untuk Binance Futures DEMO, "
            f"atau endpoint DEMO tidak dapat diakses: {e}"
        ) from e

    try:
        mode = _rest_call(
            "demo_position_mode",
            client.futures_get_position_mode,
            retries=0,
        )
        if bool(mode.get("dualSidePosition")):
            raise RuntimeError(
                "Akun DEMO memakai Hedge Mode. Bot ini memakai One-Way Mode; "
                "ubah Position Mode Binance DEMO ke One-Way."
            )
    except AttributeError:
        print("  ⚠️ futures_get_position_mode tidak tersedia pada versi python-binance ini; lanjut.")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Gagal membaca Position Mode DEMO: {e}") from e

    try:
        positions = _rest_call(
            "demo_positions_preflight",
            client.futures_position_information,
            retries=0,
        )
        wanted = set(syms)
        active = []
        for p in positions or []:
            sym = p.get("symbol")
            amt = float(p.get("positionAmt", 0) or 0)
            if sym in wanted and abs(amt) > 0:
                active.append((sym, amt))
        if active:
            raise RuntimeError(
                f"Masih ada posisi DEMO terbuka pada symbol bot: {active}. "
                "Tutup/sinkronkan dulu agar state lokal tidak berbeda dari akun DEMO."
            )
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Gagal membaca posisi DEMO: {e}") from e

    _order_state_uncertain = False
    print("  🟢 DEMO ORDER ROUTING: AKTIF — MARKET ENTRY/EXIT benar-benar dikirim ke Binance DEMO")


def run_bot():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  💎 BOT SCALPING v22.0 — BINANCE FUTURES DEMO EXECUTION         ║")
    print("║  1. Signal LONG -> REAL DEMO MARKET SELL (INVERSE)             ║")
    print("║  2. Signal SHORT -> REAL DEMO MARKET BUY (INVERSE)             ║")
    print("║  3. EXIT -> REAL DEMO reduceOnly MARKET                         ║")
    print("║  4. TP = 2.5–3.5% | SL = 1.5–2.5%                              ║")
    print("║  5. SL BAN 3 JAM + CLOSE POSISI LAIN YANG LOSS                  ║")
    print("║  6. TIME: HARD MAX 30m | LOSS->BAN 1h | PROFIT->NO BAN         ║")
    print("║  7. Profit Guard + Signal Flip aktif                            ║")
    print("║  8. PRIVATE ORDER ROUTE: demo-fapi.binance.com ONLY            ║")
    print("╚════════════════════════════════════════════════════════════════════╝")

    try:
        valid = {
            s["symbol"]
            for s in _rest_call(
                "startup_exchange_info",
                client.futures_exchange_info,
                retries=0,
            )["symbols"]
            if s["status"] == "TRADING"
        }
    except Exception as e:
        raise RuntimeError(f"Gagal membaca exchangeInfo Binance DEMO: {e}") from e

    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    if not syms:
        raise RuntimeError("Tidak ada symbol bot yang valid pada Binance Futures DEMO")

    print(f"  ✅ DEMO Futures REST: {DEMO_FUTURES_BASE_URL} | Symbols: {len(syms)}")
    demo_preflight_account(syms)

    bootstrap_all_klines(syms)

    # Public market streams are used for signal/price data.
    # Private execution remains hard-pinned to demo-fapi REST.
    twm.start()
    twm.start_all_mark_price_socket(callback=handle_mark_price, fast=MARK_PRICE_FAST)
    twm.start_futures_multiplex_socket(
        callback=handle_all_ticker,
        streams=["!ticker@arr"],
    )

    kline_streams = [f"{s.lower()}@kline_5m" for s in syms]
    twm.start_futures_multiplex_socket(
        callback=handle_kline_multiplex,
        streams=kline_streams,
    )
    twm.start_futures_multiplex_socket(
        callback=handle_btc_aggtrade,
        streams=["btcusdt@aggtrade"],
    )

    for i in range(0, len(syms), DEPTH_SOCKET_CHUNK):
        chunk = syms[i:i + DEPTH_SOCKET_CHUNK]
        depth_streams = [f"{s.lower()}@depth10" for s in chunk]
        twm.start_futures_multiplex_socket(
            callback=handle_depth_multiplex,
            streams=depth_streams,
        )
        time.sleep(0.15)

    threading.Thread(target=t_ws_watchdog, daemon=True).start()
    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    time.sleep(2)
    tickers_all()

    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        print(f"\n{'═'*68}")
        api_flag = f" | ⚠️API_FAIL:{_api_fail_streak}" if _api_fail_streak >= 20 else ""
        ws_idle = time.time() - _ws_last_msg_ts
        ws_flag = f" | ⚠️WS_IDLE:{ws_idle:.0f}s" if ws_idle > WS_STALE_SEC else ""

        btc_status = btc_macro.get_status_str()
        veto_summary = (
            f"Veto[Wall:{_stats['wall_veto']}|"
            f"BTC:{_stats['btc_breaker_veto']}|"
            f"Spoof:{_stats['spoof_veto']}]"
        )
        circuit_status, _ = _circuit_snapshot()
        circuit_flag = f" | 🛑 {circuit_status}" if circuit_status else ""

        print(
            f"  #{cycle} {time.strftime('%H:%M:%S')} "
            f"BINANCE-DEMO BTC_5M:{_macro['btc']} "
            f"({len(live_positions)}/{MAX_POSITIONS}) "
            f"PnL:{_stats['pnl']:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U) | "
            f"{veto_summary}{circuit_flag}{api_flag}{ws_flag}"
        )
        print(f"        ↳ {btc_status}")

        if (k := ks_check())[0]:
            print(f"  🚨 KS:{k[1]}")
        elif slots == 0:
            print("  ✅ Slots full — monitoring REAL DEMO positions + TP/SL + TIME ENGINE + SIGNAL FLIP")
        else:
            print(f"  🔍 {slots} slot kosong — scanning untuk REAL DEMO entry...")

        if cycle % 30 == 0:
            print_full()
        time.sleep(SCAN_INTERVAL)



if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot Binance DEMO dihentikan manual.")
    except Exception as e:
        print(f"\n❌ BOT STARTUP STOP: {type(e).__name__}: {e}")
