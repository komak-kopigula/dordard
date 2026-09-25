"""
Bot Scalping v22.0 DEMO — INSTITUTIONAL QUANT ENGINE (Binance Futures)
====================================================================
MODE NORMAL + CAPITAL PROTECTION
- Signal Asli LONG  -> Eksekusi LONG
- Signal Asli SHORT -> Eksekusi SHORT
- TP = 3.5x ATR (clamped 2.5%-3.5%)
- SL = 1.8x ATR (clamped 1.5%-2.5%)
- Trailing Stop Dihapus Sepenuhnya
- SL = ban 3 jam + liquidate posisi lain yang sedang floating loss
- CASCADE_AFTER_SL = ban tambahan 1 jam (tidak memperpanjang SL 3 jam)
- TIME_LIMIT = ban 1 jam
- Profit Guard dynamic berbasis ATH PnL; semakin besar ATH, semakin ketat proteksi giveback
- Signal Flip Exit: cut setelah candle 5m closed mengonfirmasi arah berlawanan kuat
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

try:
    client = Client(api_key, api_secret)
except Exception:
    client = Client(api_key, api_secret)
# BINANCE FUTURES DEMO / TESTNET — HARD SAFETY LOCK
# Semua REST Futures diarahkan ke environment demo, bukan akun real.
BINANCE_DEMO = True
if BINANCE_DEMO:
    client.FUTURES_URL = "https://demo-fapi.binance.com/fapi"
else:
    client.FUTURES_URL = "https://fapi.binance.com/fapi"

# WebSocket tuning. python-binance versions that support max_queue_size get a
# larger per-socket queue; older versions are handled without breaking startup.
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
MAX_POSITIONS = 3

# ── LOSS CIRCUIT / LOSS LIQUIDATION ────────────────────────────────────────
SL_BAN_SECONDS = 3 * 60 * 60          # 3 jam setelah SL asli
CASCADE_BAN_SECONDS = 1 * 60 * 60      # 1 jam setelah CASCADE_AFTER_SL
TIME_LIMIT_BAN_SECONDS = 30 * 60        # 30 menit setelah TIME_LIMIT; lebih ringan dari SL
SL_LIQUIDATE_LOSERS = True             # Saat SL: tutup posisi lain yang floating loss

# ── PROFIT GUARD / ATH GIVEBACK PROTECTION ──────────────────────────────────
PROFIT_GUARD_ENABLED = True
PROFIT_GUARD_ARM_PNL = 1.50             # Mulai melindungi profit setelah ATH mencapai +1.50U
# Dynamic ATH giveback: semakin besar ATH, semakin kecil persentase profit
# yang boleh dikembalikan. Tidak ada hard ceiling; guard terus mengikuti ATH.
PROFIT_GUARD_GIVEBACK_PCT_LOW = 0.40    # ATH 1.50–2.50U -> toleransi giveback 40%
PROFIT_GUARD_GIVEBACK_PCT_MID = 0.35    # ATH 2.50–5.00U  -> toleransi giveback 35%
PROFIT_GUARD_GIVEBACK_PCT_HIGH = 0.30   # ATH 5.00–10.00U -> toleransi giveback 30%
PROFIT_GUARD_GIVEBACK_PCT_MAX = 0.20    # ATH >=10U -> toleransi giveback 20%
PROFIT_GUARD_GIVEBACK_MIN = 0.50        # Batas minimum giveback nominal; bukan lagi $1 tetap
PROFIT_GUARD_BAN_SECONDS = 3 * 60 * 60  # 3 jam blok entry baru setelah guard trigger
PROFIT_GUARD_CLOSE_LOSERS = True       # Saat guard aktif, close posisi lain yang floating loss

# ── SIGNAL FLIP EXIT ────────────────────────────────────────────────────────
SIGNAL_FLIP_EXIT_ENABLED = True
SIGNAL_FLIP_MIN_SCORE = 65              # Harus lebih kuat dari MIN_SCORE normal
SIGNAL_FLIP_CONFIRM_CANDLES = 1         # 1 candle 5m closed berlawanan sudah cukup
SIGNAL_FLIP_MIN_HOLD_SECONDS = 90       # Hindari cut akibat noise tepat setelah entry

# Scanning & Concurrency
# STRATEGY BASELINE = PAPER VERSION YANG TERBUKTI +2.95U
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01
COOLDOWN_SEC  = 300   # 5 Menit jeda per simbol setelah close

# ── REST API SAFETY / ANTI-403 ───────────────────────────────────────────────
REST_MIN_INTERVAL = 0.20       # jeda minimum antar request REST dari proses ini
REST_403_COOLDOWN = 300.0      # jangan spam API setelah WAF 403
REST_429_COOLDOWN = 60.0       # backoff setelah rate-limit 429
REST_418_COOLDOWN = 900.0      # backoff konservatif setelah auto-ban 418
REST_RETRIES = 2

# Scoring & Filter
MIN_SCORE      = 55
SLIPPAGE_GUARD = 0.0015
TTL_5M         = 2

# ── Dynamic Volatility Risk Management (ATR Multipliers) ───────────────────
# Risk management normal berbasis ATR.
# TP = 3.5x ATR dan SL = 1.8x ATR, dibatasi oleh rentang persentase.
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8

MIN_TP_PCT        = 0.025
MAX_TP_PCT        = 0.035
MIN_SL_PCT        = 0.015
MAX_SL_PCT        = 0.025
MAX_HOLD_SECONDS  = 6120   # hard limit 102 menit (batas absolut)
TIME_LIMIT_SOFT_SECONDS = 45 * 60
TIME_LIMIT_PROFIT_SECONDS = 75 * 60
TIME_LIMIT_LOSS_PNL = -0.12
TIME_LIMIT_SMALL_PROFIT_PNL = 0.10
PROFIT_GUARD_EMERGENCY_GAP = 0.25
# ──────────────────────────────────────────────────────────────────────────

# ── Institutional Microstructure (Order Book Depth) ───────────────────────
WALL_RATIO_THRESHOLD  = 2.5   # Volume level antrean >= 2.5x rata-rata 10 level
WALL_DEPTH_PCT        = 0.35  # Atau >= 35% dari total volume sisi tersebut
WALL_PROXIMITY_PCT    = 0.005 # Dalam radius 0.5% dari harga pasar saat ini
IMBALANCE_STRONG_BULL = 0.25  # BAI > +0.25
IMBALANCE_STRONG_BEAR = -0.25 # BAI < -0.25
SPOOF_DROP_THRESHOLD  = 0.40  # Penurunan likuiditas mendadak > 40% dalam < 2.5 detik
# ──────────────────────────────────────────────────────────────────────────

# ── Macro BTC Correlation & Flash Crash Engine ────────────────────────────
BTC_CRASH_THRESHOLD  = -0.003 # -0.3% dalam sliding window detik
BTC_PUMP_THRESHOLD   = 0.003  # +0.3% dalam sliding window detik
BTC_WINDOW_SEC       = 8.0    # Window lookback perbandingan harga BTC
BTC_BREAKER_COOLDOWN = 120.0  # 2 Menit circuit breaker blokir sinyal Altcoin berlawanan
# ──────────────────────────────────────────────────────────────────────────

# Kill Switch
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

# Learning
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
#  1. ORDER BOOK ENGINE (MICRO-STRUCTURE & DEPTH)
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
                    "bids": bids,
                    "asks": asks,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "imbalance": imbalance,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
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
        """
        Deteksi apakah ada Limit Wall tebal yang menghalangi pergerakan harga.
        Returns: (has_wall, wall_type, wall_price, wall_qty, wall_multiplier)
        """
        book = self.get_book(symbol)
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0

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
        """
        Deteksi apakah ada penarikan likuiditas mendadak (spoofing trap).
        """
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if len(hist) < 3: return False, ""
        
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
                    print(f"\n  🚨 [BTC FLASH CRASH DETECTED] Drop: {delta*100:+.2f}% in {ts - cutoff:.1f}s | Price: {price:.1f} | Altcoin LONGs LOCKED for {BTC_BREAKER_COOLDOWN:.0f}s!")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {
                        "active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚀 [BTC FLASH PUMP DETECTED] Surge: {delta*100:+.2f}% in {ts - cutoff:.1f}s | Price: {price:.1f} | Altcoin SHORTs LOCKED for {BTC_BREAKER_COOLDOWN:.0f}s!")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]
                    delta = self.breaker["delta"]
                    if b_type == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash active ({rem:.0f}s left, drop {delta*100:+.2f}%)"
                    elif b_type == "PUMP" and side == "SHORT":
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
        """
        Deteksi Institutional Absorption:
        - Bullish: Seller volume meledak tapi harga tertahan di support/membuat lower wick panjang.
        - Bearish: Buyer volume meledak tapi harga tertahan di resistance/membuat upper wick panjang.
        """
        if df is None or len(df) < 25: return False, False, ""
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

        # Bullish Absorption
        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        wick_bull = lw_ratio >= 0.38
        close_held_bull = close >= (low + 0.45 * rng)
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)

        # Bearish Absorption
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
#  4. VOLATILITY-ADJUSTED RISK MANAGEMENT (NORMAL TP/SL, NO TRAILING)
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
        else: # SHORT
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "atr_pct": atr_pct
        }

# ═══════════════════════════════════════════════════════════════════════════
#  MARKET REGIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════

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
        atr, atr_prev = row["atr"], prev["atr"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull  = close > e9 > e21
        mild_bear  = close < e9 < e21
        strong_trend      = adx > 25
        very_strong_trend = adx > 35
        atr_expand   = (atr / atr_prev) > 1.2 if atr_prev > 0 else False
        atr_collapse = (atr / atr_prev) < 0.8 if atr_prev > 0 else False
        m5, m5_prev = row["m5"], prev["m5"]
        decelerating = (abs(m5) < abs(m5_prev)) if not np.isnan(m5_prev) else False

        if very_strong_trend and bull_stack: return MarketRegime.REGIME_TRENDING_BULL, min(adx, 100), 1.0
        elif very_strong_trend and bear_stack: return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 100), -1.0
        elif strong_trend and (bull_stack or mild_bull): return MarketRegime.REGIME_TRENDING_BULL, min(adx, 80), 0.7
        elif strong_trend and (bear_stack or mild_bear): return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 80), -0.7
        elif atr_expand and adx < 20: return MarketRegime.REGIME_VOLATILE, 50, 0
        elif (atr_collapse and decelerating) or (adx > 20 and adx < 35 and decelerating): return MarketRegime.REGIME_EXHAUSTION, 40, (1 if m5 > 0 else -1)
        else: return MarketRegime.REGIME_RANGE, 30, 0

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
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        base = signal_name.split('[')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT: return self.weights.get(base, 10)
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
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, regime, bias
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        elif regime == MarketRegime.REGIME_VOLATILE:
            _stats["regime_block"] += 1
            return None, max(long_score, short_score), [], atr, regime, bias

        return None, 0, [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50: w = self.weights.get_adjusted_weight("ema_bull_stack"); score += w; signals.append(f"EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21: w = self.weights.get_adjusted_weight("ema_mild_bull"); score += w; signals.append(f"EMA4↑[{w:.0f}]")
        elif p > e5 > e9: w = self.weights.get_adjusted_weight("ema_weak_bull"); score += w; signals.append(f"EMA3↑[{w:.0f}]")

        if row["m5"] > 0.003: w = self.weights.get_adjusted_weight("mom_strong"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        elif row["m5"] > 0.0015: w = self.weights.get_adjusted_weight("mom_moderate"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")

        if prev["mh"] <= 0 and row["mh"] > 0: w = self.weights.get_adjusted_weight("macd_cross_up"); score += w; signals.append(f"MACD_X↑[{w:.0f}]")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen"); score += w; signals.append(f"MACD↑↑[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio > 0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bull"); score += w; signals.append(f"ΔBuy+{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio > 0.55: w = self.weights.get_adjusted_weight("orderflow_buy_high"); score += w; signals.append(f"TakerBuy{buy_ratio*100:.0f}%[{w:.0f}]")

        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs:
            w = self.weights.get_adjusted_weight("absorption_bull"); score += w; signals.append(f"BullAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bull"); score += w; signals.append(f"BAI+{imb*100:.0f}%[{w:.0f}]")

        if 48 <= row["rsi"] <= 68: w = self.weights.get_adjusted_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] > 68: w = self.weights.get_adjusted_weight("rsi_extreme_ob"); score += w; signals.append(f"RSI{row['rsi']:.0f}OB[{w:.0f}]")

        return score, signals

    def _score_short(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50: w = self.weights.get_adjusted_weight("ema_bear_stack"); score += w; signals.append(f"EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21: w = self.weights.get_adjusted_weight("ema_mild_bear"); score += w; signals.append(f"EMA4↓[{w:.0f}]")
        elif p < e5 < e9: w = self.weights.get_adjusted_weight("ema_weak_bear"); score += w; signals.append(f"EMA3↓[{w:.0f}]")

        if row["m5"] < -0.003: w = self.weights.get_adjusted_weight("mom_strong_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        elif row["m5"] < -0.0015: w = self.weights.get_adjusted_weight("mom_moderate_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")

        if prev["mh"] >= 0 and row["mh"] < 0: w = self.weights.get_adjusted_weight("macd_cross_down"); score += w; signals.append(f"MACD_X↓[{w:.0f}]")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen_neg"); score += w; signals.append(f"MACD↓↓[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio < -0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bear"); score += w; signals.append(f"ΔSell{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio < 0.45: w = self.weights.get_adjusted_weight("orderflow_sell_high"); score += w; signals.append(f"TakerSell{(1-buy_ratio)*100:.0f}%[{w:.0f}]")

        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs:
            w = self.weights.get_adjusted_weight("absorption_bear"); score += w; signals.append(f"BearAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bear"); score += w; signals.append(f"BAI{imb*100:.0f}%[{w:.0f}]")

        if 32 <= row["rsi"] <= 52: w = self.weights.get_adjusted_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] < 32: w = self.weights.get_adjusted_weight("rsi_extreme_os"); score += w; signals.append(f"RSI{row['rsi']:.0f}OS[{w:.0f}]")

        return score, signals

# ═══════════════════════════════════════════════════════════════════════════
#  TRADE RECORDS & LEARNING LAYER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:       str
    direction:    str
    entry_price:  float
    exit_price:   float
    pnl:          float
    won:          bool
    regime:       str
    signals:      List[str]
    score:        float
    atr_entry:    float
    hold_seconds: float
    exit_reason:  str
    peak_pct:     float
    timestamp:    float = field(default_factory=time.time)

class LearningLayer:
    def __init__(self, signal_weights: SignalWeights):
        self.signal_weights  = signal_weights
        self.trades          = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self.stats_by_symbol = defaultdict(lambda: {"wins": 0, "losses": 0})

    def add_trade(self, trade: TradeRecord):
        self.trades.append(trade)
        r = trade.regime
        self.stats_by_regime[r]["wins"]   += 1 if trade.won else 0
        self.stats_by_regime[r]["losses"] += 0 if trade.won else 1
        self.stats_by_regime[r]["pnl"]    += trade.pnl
        if trade.won:
            self.stats_by_regime[r].setdefault("peak_sum", 0.0)
            self.stats_by_regime[r]["peak_sum"] += trade.peak_pct
        self.stats_by_symbol[trade.symbol]["wins"]   += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        self.signal_weights.record_outcome(trade.signals, trade.won)
        if len(self.trades) > 1000: self.trades = self.trades[-500:]

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
WS_STALE_SEC     = 30
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0, # TRAILING STOP REMOVED: ath_pnl ditambahkan
    "hard_sl": 0, "tp_exit": 0, "regime_block": 0, # TRAILING STOP REMOVED: trail_exit dihapus
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "absorb_entries": 0,
    "hist": deque(maxlen=200), "start": time.time(),
    "sl_ban_count": 0, "sl_cascade_closes": 0,
    "cascade_ban_count": 0, "time_limit_ban_count": 0,
    "profit_guard_count": 0, "signal_flip_exits": 0,
}

live_positions = {}
cooldown_list  = {}
trade_log      = []
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)
learning       = LearningLayer(signal_weights)

_last_err_print   = defaultdict(float)
_api_fail_streak = 0
_api_ok_last      = time.time()
_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}
_rest_ticker_stale_until = 0.0
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
_profit_guard_last_floor = 0.0
_profit_guard_last_trigger_ts = 0.0
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
    """REST gate: serialize requests, honor WAF/rate-limit backoff, avoid bursts."""
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

def _new_client_order_id(prefix, sym):
    return f"IV22_{prefix}_{sym}_{int(time.time()*1000)%1000000000}"[:32]

def _create_market_order_safe(sym, side, quantity, reduce_only=False):
    """Tidak me-retry POST order; gunakan clientOrderId untuk verifikasi bila perlu."""
    global _order_state_uncertain
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
        return _rest_call(f"create_order_{sym}", client.futures_create_order, retries=0, **kwargs)
    except Exception as first_exc:
        text = str(first_exc).upper()
        # 403/429/418 are rejected by the API/WAF layer; do not duplicate the POST.
        if "403" in text or "429" in text or "418" in text or "CLOUDFRONT" in text:
            raise

        # For ambiguous network/5xx errors, query by clientOrderId exactly once.
        try:
            time.sleep(0.5)
            found = _rest_call(
                f"verify_order_{sym}",
                client.futures_get_order,
                symbol=sym,
                origClientOrderId=cid,
                retries=0,
            )
            if found:
                return found
        except Exception:
            pass

        _order_state_uncertain = True
        raise RuntimeError(f"ORDER STATUS UNKNOWN {sym} clientOrderId={cid}: {first_exc}") from first_exc

def _get_order_fill(order):
    try:
        filled = float(order.get("executedQty", 0) or 0)
        avg_px = float(order.get("avgPrice", 0) or 0)
        return filled, avg_px
    except Exception:
        return 0.0, 0.0

def get_precision(symbol):
    if symbol in _precision_cache: return _precision_cache[symbol]
    try:
        info = _rest_call("futures_exchange_info", client.futures_exchange_info)
        for s in info['symbols']:
            if s['symbol'] == symbol:
                prec = int(s['quantityPrecision'])
                _precision_cache[symbol] = prec
                return prec
    except Exception as e:
        _log_err("get_precision", e)
    return 2

def qty(symbol, price):
    raw = (ORDER_USDT * LEVERAGE) / price
    return round(raw, get_precision(symbol))

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
    # REST fallback dibatasi; strategi tetap sama saat WS sehat.
    if _ticker_cache and (now - _ticker_ts) < 15:
        return _ticker_cache
    try:
        raw = _rest_call("futures_ticker", client.futures_ticker, retries=1)
        _ticker_cache = {
            t["symbol"]: {
                "pct": float(t["priceChangePercent"]),
                "vol": float(t["quoteVolume"]),
                "last": float(t["lastPrice"])
            } for t in raw
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
    df["mh"]  = ta.trend.MACD(close, 12, 26, 9).macd_diff()
    df["e5"]  = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"]  = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()
    
    df["vm"]  = volume.rolling(20).mean()
    df["vr"]  = volume / df["vm"].replace(0, 1e-9)

    taker_buy = tbbase
    taker_sell = (volume - taker_buy).clip(lower=0)
    df["delta"] = taker_buy - taker_sell
    df["delta_ratio"] = df["delta"] / volume
    df["br"]  = taker_buy / volume
    df["cvd"] = df["delta"].rolling(10).sum()

    df["rng"] = (high - low).replace(0, 1e-9)
    df["upper_wick"] = high - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - low
    df["body"] = (close - df["open"]).abs()
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]
    df["br2"]  = df["body"] / df["rng"]

    df["m5"]   = (close - close.shift(5)) / close.shift(5)
    df["m3"]   = (close - close.shift(3)) / close.shift(3)
    return df

def run_ta(df):
    if "delta_ratio" not in df.columns or "rsi" not in df.columns:
        df = _compute_indicators(df)
    return df

def _bootstrap_klines(symbol, interval, limit=100):
    try:
        kl = _rest_call(f"bootstrap_klines_{symbol}", client.futures_klines, symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]: df[c] = df[c].astype(float)
        df = _compute_indicators(df)
        with _kline_lock: _kline_cache[symbol] = df
        _api_ok()
        return df
    except Exception as e:
        _log_err(f"bootstrap_klines_{symbol}", e)
        _api_fail(f"bootstrap_klines_{symbol}")
        return None

def _append_kline_from_ws(symbol, k):
    try:
        base_cols = ["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"]
        new_row = {
            "time": int(k["t"]), "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]),
            "close": float(k["c"]), "volume": float(k["v"]), "ct": int(k["T"]), "qv": float(k.get("q", 0)),
            "trades": int(k.get("n", 0)), "tbbase": float(k.get("V", 0)), "tbquote": float(k.get("Q", 0)),
            "ignore": 0,
        }
        with _kline_lock:
            df = _kline_cache.get(symbol)
            if df is None: return
            if len(df) > 0 and int(df.iloc[-1]["time"]) == new_row["time"]:
                df = df.iloc[:-1]
            df_base = df[base_cols] if all(c in df.columns for c in base_cols) else df
            df_base = pd.concat([df_base, pd.DataFrame([new_row])], ignore_index=True)
            if len(df_base) > 300: df_base = df_base.iloc[-300:].reset_index(drop=True)
            _kline_cache[symbol] = _compute_indicators(df_base)
    except Exception as e:
        _log_err(f"append_kline_{symbol}", e)

def ohlcv(symbol, interval, limit=100):
    with _kline_lock:
        df = _kline_cache.get(symbol)
    if df is not None: return df
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

    print(f"  🛑 [{label}] {trigger_sym} — entry baru diblokir sampai {time.strftime('%H:%M:%S', time.localtime(active_until))}")

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
            print(f"  ⚠️ [{reason}] {sym}: harga floating tidak tersedia — posisi TIDAK dipaksa close")
            continue

        fpnl = _estimate_floating_pnl(pos, px)
        if fpnl < 0:
            candidates.append((sym, px, fpnl))
        else:
            side = pos.get("side", "?")
            print(f"  ✅ [{reason}] {sym} {side} dipertahankan | floating:{fpnl:+.5f}U")

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
    """Aktifkan ban 3 jam setelah SL dan tutup hanya posisi lain yang floating loss."""
    global _sl_ban_until, _sl_ban_reason, _sl_ban_trigger

    now = time.time()
    with _sl_ban_lock:
        _sl_ban_until = max(_sl_ban_until, now + SL_BAN_SECONDS)
        _sl_ban_reason = "SL"
        _sl_ban_trigger = trigger_sym
        _stats["sl_ban_count"] += 1
        ban_until_local = _sl_ban_until

    print(f"\n  🛑 [SL CIRCUIT BAN] {trigger_sym} kena SL — trading baru DIKUNCI 3 JAM sampai {time.strftime('%H:%M:%S', time.localtime(ban_until_local))}")

    if SL_LIQUIDATE_LOSERS:
        _liquidate_losing_positions("CASCADE_AFTER_SL", exclude={trigger_sym})

def _profit_guard_metrics():
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
    return ath, ath - giveback, giveback_pct

def _maybe_activate_profit_guard():
    global _profit_guard_triggered_ath, _profit_guard_in_progress
    global _profit_guard_last_floor, _profit_guard_last_trigger_ts

    if not PROFIT_GUARD_ENABLED:
        return
    pnl = _stats["pnl"]
    ath = _stats["ath_pnl"]
    if ath < PROFIT_GUARD_ARM_PNL:
        return

    _, floor, giveback_pct = _profit_guard_metrics()
    _profit_guard_last_floor = floor

    trigger_new_ath = pnl <= floor and ath > _profit_guard_triggered_ath + 1e-9
    trigger_emergency = (
        _profit_guard_triggered_ath > 0
        and pnl <= floor - PROFIT_GUARD_EMERGENCY_GAP
        and time.time() - _profit_guard_last_trigger_ts >= 300
    )
    if not (trigger_new_ath or trigger_emergency) or _profit_guard_in_progress:
        return

    if trigger_new_ath:
        _profit_guard_triggered_ath = ath
    _profit_guard_in_progress = True
    _profit_guard_last_trigger_ts = time.time()
    try:
        _activate_aux_ban("PROFIT_GUARD", PROFIT_GUARD_BAN_SECONDS, "ATH_GIVEBACK")
        mode = "EMERGENCY" if trigger_emergency else "TRIGGER"
        print(f"  🧱 [PROFIT GUARD {mode}] PnL {pnl:+.4f}U | ATH {ath:+.4f}U | floor {floor:+.4f}U | giveback:{ath-pnl:+.4f}U ({giveback_pct:.0%})")
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
        return True, "ORDER_STATE_UNKNOWN — entry baru dihentikan"
    if k["active"] and now >= k["resume"]: k["active"], k["consec"] = False, 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"], k["day_reset"] = 0.0, day
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

def get_real_fill_price(sym, order_resp):
    try:
        cum_quote = float(order_resp.get('cumQuote', 0))
        exec_qty = float(order_resp.get('executedQty', 0))
        if exec_qty > 0 and cum_quote > 0:
            return cum_quote / exec_qty
        avg_px = float(order_resp.get('avgPrice', 0))
        if avg_px > 0:
            return avg_px
        order_id = order_resp.get('orderId')
        if order_id:
            for _ in range(2):
                time.sleep(0.5)
                info = _rest_call(f"get_order_{sym}", client.futures_get_order, symbol=sym, orderId=order_id)
                c_quote = float(info.get('cumQuote', 0))
                e_qty = float(info.get('executedQty', 0))
                if e_qty > 0 and c_quote > 0:
                    return c_quote / e_qty
                a_px = float(info.get('avgPrice', 0))
                if a_px > 0:
                    return a_px
    except Exception:
        pass
    return 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  5. CORE EXECUTION & POSITION MONITORING
# ═══════════════════════════════════════════════════════════════════════════

def live_open(orig_direction, score, sigs, price, atr, regime, bias, sym, risk_profile):
    # EKSEKUSI NORMAL: arah order mengikuti hasil analisis asli.
    if orig_direction not in ("LONG", "SHORT"):
        return
    execution_side = orig_direction

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS: return
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    if px_now > 0:
        price = px_now

    try:
        q_val = qty(sym, price)
        if q_val <= 0:
            raise ValueError("quantity <= 0")
    except Exception as e:
        _log_err(f"qty_{sym}", e)
        with _lock: live_positions.pop(sym, None)
        return

    tp_pct   = risk_profile["tp_pct"]
    sl_pct   = risk_profile["sl_pct"]
    tp_price = risk_profile["tp_price"]
    sl_price = risk_profile["sl_price"]

    pos = {
        "side": execution_side,
        "orig_signal": orig_direction,
        "entry": price, "qty": q_val,
        "open_time": time.time(), "score": score, "sigs": sigs,
        "atr": atr, "regime": regime, "bias": bias,
        "tp_pct": tp_pct, "sl_pct": sl_pct,
        "tp_price": tp_price, "sl_price": sl_price,
        "peak_price": price
    }
    with _lock: live_positions[sym] = pos

    # Set leverage sekali per symbol per proses.
    if sym not in _leverage_done:
        try:
            _rest_call(f"leverage_{sym}", client.futures_change_leverage, symbol=sym, leverage=LEVERAGE, retries=0)
            _leverage_done.add(sym)
        except Exception as e:
            _log_err(f"leverage_{sym}", e)
            with _lock: live_positions.pop(sym, None)
            return

    try:
        order = _create_market_order_safe(
            sym,
            'BUY' if execution_side == 'LONG' else 'SELL',
            q_val,
            reduce_only=False,
        )
        filled_qty, real_px = _get_order_fill(order)
        if filled_qty <= 0:
            raise RuntimeError(f"ORDER TIDAK FILLED: {sym} status={order.get('status')}")
        if real_px <= 0:
            real_px = price

        price = real_px
        new_risk = DynamicRiskManager.calculate_levels(price, execution_side, atr)
        with _lock:
            if sym in live_positions:
                live_positions[sym].update({
                    "entry": price,
                    "qty": filled_qty,
                    "tp_pct": new_risk["tp_pct"],
                    "sl_pct": new_risk["sl_pct"],
                    "tp_price": new_risk["tp_price"],
                    "sl_price": new_risk["sl_price"],
                    "peak_price": price,
                    "peak_floating_pnl": 0.0
                })

        print(f"         ✅ ORDER #{order.get('orderId')} | {execution_side} | fill:{price:.6g} | qty:{filled_qty:.8g}")
    except Exception as e:
        print(f"  ❌ ORDER GAGAL {sym}: {e}")
        with _lock: live_positions.pop(sym, None)
        return

    d = "🟢" if execution_side == "LONG" else "🔴"
    imb_str = f" | BAI:{order_book.get_imbalance(sym)*100:+.0f}%" if order_book.get_book(sym) else ""
    print(f"\n  {d} [LIVE PAPER-BASELINE NORMAL v22] {sym} EXEC:{execution_side} (Signal:{orig_direction}) @{price:.6g} | TP:{tp_pct*100:.2f}% | SL:{sl_pct*100:.2f}%{imb_str} | Regime:{regime}")
    print(f"         Signals: {' | '.join(sigs[:6])}")
    _stats["trades"] += 1
    if any("Absorb" in s for s in sigs):
        _stats["absorb_entries"] += 1

def live_close(sym, reason, price=None):
    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"): return

    if price is None:
        price = price_live(sym)

    side, entry, q_val = pos["side"], pos["entry"], pos["qty"]
    if q_val <= 0:
        with _lock: live_positions[sym] = pos
        return

    try:
        close_order = _create_market_order_safe(
            sym,
            'SELL' if side == 'LONG' else 'BUY',
            q_val,
            reduce_only=True,
        )
        filled_qty, real_px = _get_order_fill(close_order)
        if real_px > 0:
            price = real_px
        if price <= 0:
            price = entry
        if filled_qty > 0 and filled_qty < q_val:
            q_val = filled_qty
        print(f"         ✅ CLOSE ORDER #{close_order.get('orderId')} | fill:{price:.6g} | qty:{q_val:.8g}")
    except Exception as e:
        _log_err(f"close_order_{sym}", e, cooldown=5)
        print(f"  ⚠️ CLOSE ORDER GAGAL {sym}: {e}")
        with _lock: live_positions[sym] = pos
        return

    gross_pnl  = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    fee_rate   = 0.0005
    total_fee  = (entry * q_val + price * q_val) * fee_rate
    pnl        = gross_pnl - total_fee
    pct        = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
    hold       = time.time() - pos["open_time"]
    won        = pnl >= 0
    e_icon     = "🟢" if won else "🔴"

    peak_px  = pos.get("peak_price", entry)
    peak_pct = (peak_px - entry) / entry if side == "LONG" else (entry - peak_px) / entry

    print(f"  {e_icon} [NORMAL ENGINE v22] {sym} {side} CLOSE — {reason} | peak:{peak_pct*100:+.3f}%")
    print(f"     {entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U")

    trade = TradeRecord(
        symbol=sym, direction=side, entry_price=entry, exit_price=price,
        pnl=pnl, won=won, regime=pos.get("regime", "UNKNOWN"),
        signals=pos.get("sigs", []), score=pos.get("score", 0),
        atr_entry=pos.get("atr", 0), hold_seconds=hold, exit_reason=reason, peak_pct=peak_pct,
    )
    learning.add_trade(trade)

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    
    # TRACKING ATH PNL: Update PnL Tertinggi / ATH jika PnL kumulatif mencapai puncaknya
    if _stats["pnl"] > _stats["ath_pnl"]:
        _stats["ath_pnl"] = _stats["pnl"]

    ks_upd(pnl)

    if won:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    # Exit counters dipisahkan agar CASCADE_AFTER_SL tidak dihitung sebagai SL baru.
    if reason == "SL": _stats["hard_sl"] += 1
    elif reason == "TP": _stats["tp_exit"] += 1

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
        "pnl": round(pnl, 5), "reason": reason, "hold": int(hold),
    })

    # Circuit actions berdasarkan jenis exit.
    if reason == "SL":
        _activate_sl_ban_and_liquidate(sym)
    elif reason == "CASCADE_AFTER_SL":
        _activate_aux_ban("CASCADE", CASCADE_BAN_SECONDS, sym)
    elif reason == "TIME_LIMIT":
        _activate_aux_ban("TIME_LIMIT", TIME_LIMIT_BAN_SECONDS, sym)

    _maybe_activate_profit_guard()

    with _lock: cooldown_list[sym] = time.time() + COOLDOWN_SEC
    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

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
        if pos is None or pos.get("_r"): continue

        hold_time = time.time() - pos["open_time"]
        px = price_live(sym)
        if px == 0:
            pos["_fail_count"] = pos.get("_fail_count", 0) + 1
            fc = pos["_fail_count"]
            if fc in (5, 20, 60) or fc % 300 == 0:
                print(f"  ⚠️ {sym}: price_live gagal {fc}x — SL/TP monitoring tertunda")
            continue
        pos["_fail_count"] = 0

        floating_pnl = _estimate_floating_pnl(pos, px)
        peak_fpnl = max(pos.get("peak_floating_pnl", floating_pnl), floating_pnl)
        pos["peak_floating_pnl"] = peak_fpnl

        if hold_time >= TIME_LIMIT_SOFT_SECONDS and floating_pnl <= TIME_LIMIT_LOSS_PNL:
            print(f"  ⏰ {sym}: TIME_LIMIT EARLY-LOSS | hold:{hold_time:.0f}s floating:{floating_pnl:+.4f}U peak:{peak_fpnl:+.4f}U")
            live_close(sym, "TIME_LIMIT")
            continue

        if hold_time >= TIME_LIMIT_PROFIT_SECONDS and floating_pnl <= TIME_LIMIT_SMALL_PROFIT_PNL:
            print(f"  ⏰ {sym}: TIME_LIMIT STAGNANT | hold:{hold_time:.0f}s floating:{floating_pnl:+.4f}U peak:{peak_fpnl:+.4f}U")
            live_close(sym, "TIME_LIMIT")
            continue

        if hold_time > MAX_HOLD_SECONDS:
            print(f"  ⏰ {sym}: HARD MAX_HOLD {MAX_HOLD_SECONDS}s | floating:{floating_pnl:+.4f}U — TIME_LIMIT")
            live_close(sym, "TIME_LIMIT")
            continue

        side, tp_px, sl_px = pos["side"], pos["tp_price"], pos["sl_price"]

        if side == "LONG":
            if px > pos["peak_price"]: pos["peak_price"] = px
        else:
            if px < pos["peak_price"]: pos["peak_price"] = px

        # RESTORED TP/SL & NO TRAILING: Monitoring HANYA menggunakan TP Baru, SL Baru, dan Time Limit
        if side == "LONG":
            if px >= tp_px: live_close(sym, "TP", tp_px); continue
            if px <= sl_px: live_close(sym, "SL", sl_px); continue
        elif side == "SHORT":
            if px <= tp_px: live_close(sym, "TP", tp_px); continue
            if px >= sl_px: live_close(sym, "SL", sl_px); continue

        # Cut hanya jika candle 5m CLOSED memberikan sinyal lawan yang kuat.
        _check_signal_flip_exit(sym, pos)

# ═══════════════════════════════════════════════════════════════════════════
#  6. SCANNER THREAD & HARD VETO FILTERS
# ═══════════════════════════════════════════════════════════════════════════

def scan_one(sym):
    try:
        time.sleep(0.002)
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None: return None
        df_ta = run_ta(df.copy())
        px_candle, atr_val = df_ta["close"].iloc[-2], df_ta["atr"].iloc[-2]
        if px_candle == 0 or np.isnan(atr_val): return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df_ta, sym)
        if orig_direction is None: return None

        # EKSEKUSI NORMAL: arah order mengikuti hasil analisis asli.
        if orig_direction not in ("LONG", "SHORT"):
            return None
        execution_side = orig_direction

        px_live = price_live(sym)
        if px_live == 0: return None

        # Filter Veto dijalankan berbasis execution_side (arah aktual yang dieksekusi)
        # ── VETO FILTER 1: BTC Flash Crash / Pump Circuit Breaker ────────────
        btc_vetoed, btc_reason = btc_macro.check_veto(execution_side)
        if btc_vetoed:
            _stats["btc_breaker_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED by BTC Circuit Breaker: {btc_reason}")
            return None

        # ── VETO FILTER 2: Order Book Wall Detection ──────────────────────────
        has_wall, wall_type, wall_px, wall_qty, wall_mult = order_book.check_walls(sym, px_live, execution_side)
        if has_wall:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED by {wall_type} @ {wall_px:.6g} (qty:{wall_qty:.1f}, {wall_mult:.1f}x avg depth)")
            return None

        # ── VETO FILTER 3: Spoofing & Liquidity Pull Detection ────────────────
        is_spoof, spoof_reason = order_book.detect_spoofing(sym, execution_side)
        if is_spoof:
            _stats["spoof_veto"] += 1
            print(f"  ⛔ [{sym}] {execution_side} VETOED: Spoofing detected ({spoof_reason})")
            return None

        # ── VETO FILTER 4: Order Book Imbalance Guard ─────────────────────────
        imb = order_book.get_imbalance(sym)
        if execution_side == "LONG" and imb < -0.40:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] LONG VETOED: Heavy Ask Queue Imbalance ({imb*100:.0f}%)")
            return None
        elif execution_side == "SHORT" and imb > 0.40:
            _stats["wall_veto"] += 1
            print(f"  ⛔ [{sym}] SHORT VETOED: Heavy Bid Queue Imbalance ({imb*100:.0f}%)")
            return None

        # ── Hitung Dynamic Risk Profile Berbasis Execution Side ─────────────────────
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
            if r: res.append(r)
        except: pass
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
    # TRAILING STOP REMOVED: Tampilan log ringkas diperbarui
    print(f"       ┌ [NORMAL ENGINE v22] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} {e}PnL:{pnl:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U)")
    circuit, _ = _circuit_snapshot()
    print(f"       └ TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} Absorb:{_stats['absorb_entries']} | Circuit:{circuit or 'READY'} | Cascade:{_stats['sl_cascade_closes']} | FlipExit:{_stats['signal_flip_exits']} | AvgWin:{aw:+.4f}U | Peak:{avg_pk*100:.3f}%")

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
    print(f"    🔔 INSTITUTIONAL SCALPING v22 LIVE DASHBOARD (NORMAL MODE)")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} ({tph:.1f}T/hr)")
    # ADDED ATH PNL: Menampilkan PnL Kumulatif Tertinggi (ATH PnL)
    print(f"    {e} PnL Net:{pnl:+.5f}U | ATH PnL:{_stats['ath_pnl']:+.5f}U | Best:{_stats['best']:+.5f} Worst:{_stats['worst']:+.5f}")
    # TRAILING STOP REMOVED: Log exit dashboard tanpa trailing stop
    print(f"    📈 Exit: TP:{_stats['tp_exit']} | SL:{_stats['hard_sl']}")
    circuit, _ = _circuit_snapshot()
    if _stats['ath_pnl'] >= PROFIT_GUARD_ARM_PNL:
        ath, profit_floor, giveback_pct = _profit_guard_metrics()
        gap = _stats['pnl'] - profit_floor
        guard_state = "TRIGGERED" if _stats['profit_guard_count'] > 0 and gap <= 0 else "ARMED"
        guard_info = f" | {guard_state} | Floor:{profit_floor:+.3f}U ({giveback_pct:.0%} GB) | Gap:{gap:+.3f}U"
    else:
        guard_info = ""
    print(f"    🛑 Circuit: {circuit if circuit else 'READY'} | SL:{_stats['sl_ban_count']} | CascadeBan:{_stats['cascade_ban_count']} | TimeBan:{_stats['time_limit_ban_count']}")
    print(f"    🧱 ProfitGuard:{_stats['profit_guard_count']} | FlipExit:{_stats['signal_flip_exits']} | Cascade Close:{_stats['sl_cascade_closes']} | ATH{guard_info}")
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
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0]:
                time.sleep(SLOT_FILL_INT); continue

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
                time.sleep(SLOT_FILL_INT); continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
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
            if slots <= 0 or ks_check()[0]: continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and (s not in cooldown_list or now > cooldown_list[s])]

            hot = [s for s in _hot_syms if s in valid_syms]
            rest = [s for s in valid_syms if s not in hot]
            res = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except: pass

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
            if not isinstance(d, dict): continue
            sym = d.get("s")
            if not sym: continue
            try:
                cache[sym] = {"pct": float(d.get("P", 0)), "vol": float(d.get("q", 0)), "last": float(d.get("c", 0))}
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
        if not k: return
        sym = data.get("s") or k.get("s")
        if sym and k.get("x"):
            _append_kline_from_ws(sym, k)
    except Exception as e:
        _log_err("handle_kline_multiplex", e)

def handle_btc_aggtrade(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        if not isinstance(data, dict): return
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
        if not isinstance(data, dict): return
        sym = data.get("s")
        if not sym:
            stream = msg.get("stream", "")
            if "@depth" in stream:
                sym = stream.split("@")[0].upper()
        if sym:
            bids = data.get("b", [])
            asks = data.get("a", [])
            order_book.update(sym, bids, asks)
    except Exception as e:
        _log_err("handle_depth_multiplex", e)

def handle_user_data(msg):
    try:
        etype = msg.get("e")
        if etype == "ORDER_TRADE_UPDATE":
            o = msg.get("o", {})
            if o.get("X") in ("FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED"):
                print(f"  📡 [WS ORDER] {o.get('s')} {o.get('S')} {o.get('X')} qty={o.get('z')} avgPx={o.get('ap')}")
    except Exception as e:
        _log_err("handle_user_data", e)

def preflight_account(syms):
    """Validasi dasar agar bot tidak start dalam kondisi yang mudah menyebabkan mismatch posisi."""
    try:
        info = _rest_call("preflight_account", client.futures_account, retries=0)
        if not isinstance(info, dict):
            raise RuntimeError("response futures_account tidak valid")
    except Exception as e:
        raise RuntimeError(f"API/account check gagal: {e}") from e

    try:
        positions = _rest_call("preflight_positions", client.futures_position_information, retries=0)
        active = []
        wanted = set(syms)
        for p in positions or []:
            sym = p.get("symbol")
            amt = float(p.get("positionAmt", 0) or 0)
            if sym in wanted and abs(amt) > 0:
                active.append((sym, amt))
        if active:
            raise RuntimeError(f"Masih ada posisi Futures terbuka pada bot symbols: {active}. Tutup/sinkronkan dulu sebelum start.")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"position check gagal: {e}") from e

def bootstrap_all_klines(syms):
    print(f"  📥 Bootstrap history awal ({len(syms)} simbol) via REST — paced/anti-403...")
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

def t_ws_watchdog():
    while True:
        idle = time.time() - _ws_last_msg_ts
        if idle > WS_STALE_SEC:
            print(f"  🚨 WEBSOCKET DIAM {idle:.0f}s — tidak ada data masuk. REST fallback dibatasi oleh rate limiter/backoff.")
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  8. BOT LAUNCHER & MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def run_bot():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print(f"║  WS protection: queue={WS_MAX_QUEUE_SIZE} | depth chunks={DEPTH_SOCKET_CHUNK} | mark fast={MARK_PRICE_FAST} ║")
    print("║  💎 BOT SCALPING v22.0 LIVE — NORMAL TRADING MODE                ║")
    print("║  1. Signal LONG -> LONG | Signal SHORT -> SHORT                  ║")
    print("║  2. TP = 2.5–3.5% (3.5x ATR capped)                              ║")
    print("║  3. SL = 1.5–2.5% (1.8x ATR capped)                              ║")
    print("║  4. SL = BAN 3 JAM + CLOSE POSISI LAIN YANG SEDANG LOSS          ║")
    print("║  5. CASCADE=1 JAM / TIME_LIMIT=30 MENIT | PROFIT GUARD aktif           ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    try:
        valid = {s["symbol"] for s in _rest_call("startup_exchange_info", client.futures_exchange_info, retries=0)["symbols"] if s["status"] == "TRADING"}
    except Exception as e:
        raise RuntimeError(f"Gagal membaca exchangeInfo REAL: {e}") from e
    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    preflight_account(syms)

    bootstrap_all_klines(syms)

    twm.start()

    # Mark price: gunakan mode normal/lebih ringan. fast=True menghasilkan jauh
    # lebih banyak pesan untuk semua simbol dan dapat memenuhi queue terlalu cepat.
    twm.start_all_mark_price_socket(callback=handle_mark_price, fast=MARK_PRICE_FAST)

    twm.start_futures_multiplex_socket(callback=handle_all_ticker, streams=["!ticker@arr"])

    # Candle 5m sangat ringan, tetap satu multiplex socket.
    kline_streams = [f"{s.lower()}@kline_5m" for s in syms]
    twm.start_futures_multiplex_socket(callback=handle_kline_multiplex, streams=kline_streams)

    # BTC aggTrade dipisah sendiri agar tidak ikut antrean depth/mark-price.
    twm.start_futures_multiplex_socket(callback=handle_btc_aggtrade, streams=["btcusdt@aggtrade"])

    # Depth adalah sumber pesan terbesar. Pecah menjadi beberapa multiplex socket
    # agar satu queue tidak menerima seluruh 40 simbol sekaligus.
    for i in range(0, len(syms), DEPTH_SOCKET_CHUNK):
        chunk = syms[i:i + DEPTH_SOCKET_CHUNK]
        depth_streams = [f"{s.lower()}@depth10" for s in chunk]
        twm.start_futures_multiplex_socket(callback=handle_depth_multiplex, streams=depth_streams)
        time.sleep(0.15)

    try:
        twm.start_futures_user_socket(callback=handle_user_data)
    except Exception as e:
        _log_err("user_data_stream_start", e, cooldown=0)

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
        veto_summary = f"Veto[Wall:{_stats['wall_veto']}|BTC:{_stats['btc_breaker_veto']}|Spoof:{_stats['spoof_veto']}]"

        circuit_status, _ = _circuit_snapshot()
        circuit_flag = f" | 🛑 {circuit_status}" if circuit_status else ""
        print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC_5M:{_macro['btc']} ({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U (ATH:{_stats['ath_pnl']:+.4f}U) | {veto_summary}{circuit_flag}{api_flag}{ws_flag}")
        print(f"        ↳ {btc_status}")

        if (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}")
        elif slots == 0: print(f"  ✅ Slots full — monitoring posisi terbuka (TP/SL Only)")
        else: print(f"  🔍 {slots} slot kosong — scanning order book & flow...")
        if cycle % 30 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot dihentikan manual.")
    except Exception as e:
        print(f"\n❌ BOT STARTUP STOP: {type(e).__name__}: {e}")
