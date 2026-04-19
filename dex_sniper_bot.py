import sys
sys.stdout.reconfigure(line_buffering=True)

"""
╔══════════════════════════════════════════════════════════════════╗
║              DEX SNIPER BOT — PAPER TRADING ENGINE              ║
║         Adaptive · Selective · Momentum-Driven · Safe           ║
║                   Built for Pydroid 3 / Android                 ║
╚══════════════════════════════════════════════════════════════════╝

Author  : Xlintinsights x Claude
Version : 1.0.0 — FINAL LOCKED SYSTEM
Status  : Paper Trading Only (no live execution)

Dependencies: requests, time, random, json, os, collections, math
"""

import time
import random
import json
import os
import math
import requests
from collections import deque

# ═══════════════════════════════════════════════════════════════════
#  CONFIG BLOCK — ALL TUNABLE PARAMETERS IN ONE PLACE
# ═══════════════════════════════════════════════════════════════════

CONFIG = {
    # ── Chain & Market ──────────────────────────────────────────────
    "chain": "bsc",               # "solana" | "bsc" | "ethereum" | "base"

    # ── Adaptive Mode Score Thresholds ──────────────────────────────
    "score_threshold": {
        "QUIET":  65,
        "ACTIVE": 70,
        "HOT":    75
    },

    # ── Liquidity & Spread ──────────────────────────────────────────
    "min_liquidity_usd":    50_000,
    "max_spread_pct":       0.01,    # 1% max spread

    # ── Overextension Guard ─────────────────────────────────────────
    "overextension_soft":   0.05,    # 5%  → conditional allow
    "overextension_hard":   0.07,    # 7%  → blacklist
    "blacklist_duration":   600,     # 10 minutes

    # ── Trade Sizing (paper) ────────────────────────────────────────
    "trade_size_usd":       100,

    # ── Stop Loss per Coin Type ─────────────────────────────────────
    "stop_loss": {
        "memecoin":  0.030,
        "mid_cap":   0.020,
        "large_cap": 0.015
    },

    # ── Trailing Stop per Coin Type ─────────────────────────────────
    "trailing_stop": {
        "memecoin":  0.020,
        "mid_cap":   0.010,
        "large_cap": 0.007
    },

    # ── Profit Tiers ────────────────────────────────────────────────
    "tp1_pct": {
        "memecoin":  0.08,
        "mid_cap":   0.04,
        "large_cap": 0.02
    },
    "tp2_pct": {
        "memecoin":  0.20,
        "mid_cap":   0.10,
        "large_cap": 0.04
    },
    "tp1_sell_ratio": 0.40,          # sell 40% at TP1
    "tp2_sell_ratio": 0.35,          # sell 35% at TP2
    # remaining 25% rides trailing stop

    # ── Hold Ceilings (seconds) ─────────────────────────────────────
    "max_hold_seconds": {
        "memecoin":   900,           # 15 min
        "mid_cap":   14400,          # 4 hours
        "large_cap": 43200           # 12 hours
    },

    # ── Risk Management ─────────────────────────────────────────────
    "max_trades_per_day":       5,
    "max_consecutive_losses":   2,
    "daily_loss_limit_pct":    -3.0,
    "cooldown_after_trade":     90,  # seconds after close
    "duplicate_entry_cooldown": 60,  # seconds after entry

    # ── Timing ──────────────────────────────────────────────────────
    "monitor_interval_min":   7.5,
    "monitor_interval_max":   9.5,
    "scan_interval_base":    30,     # seconds between scan cycles
    "scan_jitter":            5,     # ± random jitter
    "warmup_seconds":        25,
    "reconfirm_min":          3,
    "reconfirm_max":          5,

    # ── Circuit Breaker ─────────────────────────────────────────────
    "btc_drop_trigger_pct":    -2.0,
    "dex_volume_spike_x":       5.0,
    "circuit_breaker_min":    600,
    "circuit_breaker_max":    900,

    # ── API Stability ───────────────────────────────────────────────
    "min_api_delay":           2.0,
    "backoff_sequence":       [2, 4, 8],
    "max_backoff":            60,
    "api_failure_pause_min":  60,
    "api_failure_pause_max": 120,
    "max_consecutive_api_failures": 3,

    # ── Slippage & Fill ─────────────────────────────────────────────
    "entry_slippage":  1.005,
    "exit_slippage":   0.995,
    "fill_factor_min": 0.90,
    "fill_factor_max": 1.00,

    # ── Memory ──────────────────────────────────────────────────────
    "max_trade_history": 100,
    "volume_buffer_size":  4,        # rolling avg window
    "btc_buffer_size":    60,        # ~8 min of 8s snapshots
    "dex_vol_buffer_size": 30,

    # ── Filters ─────────────────────────────────────────────────────
    "min_dex_volume_ratio":    0.0, # 10% of rolling avg
    "min_trades_per_5min":     1,
    "price_glitch_pct":        0.12, # 12% jump without volume = glitch
    "invalidation_window_sec": 25,
    "invalidation_min_move":   0.005,# 0.5% min move in first 25s

    # ── Entry Scoring Components ─────────────────────────────────────
    "min_green_candles":       3,
    "volume_spike_multiplier": 3.0,
    "momentum_pct_min":        0.02, # +2% price momentum

    # ── Modes ───────────────────────────────────────────────────────
    "DRY_RUN":  True,               # True = paper only, never real orders
    "VERBOSE":  True,              # True = detailed debug logs
    "RUNNING":  True,               # False = manual kill switch

    # ── State File ──────────────────────────────────────────────────
    "state_file": "bot_state.json",

    # ── Heartbeat ───────────────────────────────────────────────────
    "heartbeat_interval": 90,

    # ── Daily Reset Hour (UTC) ──────────────────────────────────────
    "daily_reset_hour": 0,

    # ── CoinGecko Classification Thresholds ─────────────────────────
    "large_cap_mcap":  1_000_000_000,   # $1B+
    "mid_cap_mcap":     50_000_000,     # $50M–$1B
    # below $50M or not on CoinGecko → memecoin
}

# ═══════════════════════════════════════════════════════════════════
#  GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════

state = {
    # Market mode
    "current_mode":        "QUIET",
    "last_candidate_mode": None,
    "last_mode_check_time": 0,

    # Trade tracking
    "active_trade":        None,
    "trade_history":       deque(maxlen=CONFIG["max_trade_history"]),
    "trade_count_today":   0,
    "consecutive_losses":  0,
    "daily_pnl":           0.0,
    "last_trade_entry_time":  0,
    "last_trade_close_time":  0,

    # API health
    "consecutive_api_failures": 0,
    "api_paused_until":         0,
    "last_api_call_time":       0,

    # Circuit breaker
    "circuit_breaker_until": 0,

    # Blacklist: token_address → {blacklisted_at, peak_price}
    "blacklist": {},

    # BTC price buffer: deque of (timestamp, price)
    "btc_price_buffer": deque(maxlen=CONFIG["btc_buffer_size"]),

    # DEX global volume buffer: deque of floats
    "dex_volume_buffer": deque(maxlen=CONFIG["dex_vol_buffer_size"]),

    # Daily reset tracking
    "last_reset_day": -1,

    # Heartbeat
    "last_heartbeat_time": 0,

    # Watchdog
    "last_loop_time": 0,

    # Startup
    "startup_time": 0,
}


# ═══════════════════════════════════════════════════════════════════
#  LOGGER
# ═══════════════════════════════════════════════════════════════════

def log(msg, level="INFO", force=False):
    """Structured logger. Respects VERBOSE flag."""
    if level == "DEBUG" and not CONFIG["VERBOSE"] and not force:
        return
    ts = time.strftime("%H:%M:%S")
    prefix = {
        "INFO":    "  ",
        "TRADE":   "💰",
        "SIGNAL":  "📡",
        "WARN":    "⚠️ ",
        "ERROR":   "❌",
        "MODE":    "🔄",
        "HEART":   "💗",
        "SYSTEM":  "🔧",
        "DEBUG":   "🔍",
        "SUCCESS": "✅",
        "EXIT":    "🚪",
    }.get(level, "  ")
    print(f"[{ts}] {prefix} {msg}")


def log_separator(char="─", width=56):
    print(char * width)


# ═══════════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════════════

def save_state():
    """Persist critical state to JSON for restart recovery."""
    try:
        persist = {
            "saved_at":             time.time(),
            "current_mode":         state["current_mode"],
            "daily_pnl":            state["daily_pnl"],
            "trade_count_today":    state["trade_count_today"],
            "consecutive_losses":   state["consecutive_losses"],
            "last_trade_entry_time": state["last_trade_entry_time"],
            "last_trade_close_time": state["last_trade_close_time"],
            "last_reset_day":       state["last_reset_day"],
            "blacklist": {
                k: v for k, v in state["blacklist"].items()
            },
            "active_trade": state["active_trade"],
        }
        with open(CONFIG["state_file"], "w") as f:
            json.dump(persist, f, indent=2)
        log("State saved.", "DEBUG")
    except Exception as e:
        log(f"State save failed: {e}", "WARN")


def load_state():
    """Load persisted state on startup."""
    if not os.path.exists(CONFIG["state_file"]):
        log("No state file found. Starting fresh.", "SYSTEM")
        return

    try:
        with open(CONFIG["state_file"], "r") as f:
            data = json.load(f)

        saved_at = data.get("saved_at", 0)
        age = time.time() - saved_at

        # State older than 24h → ignore, start fresh
        if age > 86400:
            log("State file too old (>24h). Starting fresh.", "SYSTEM")
            return

        state["current_mode"]         = data.get("current_mode", "QUIET")
        state["daily_pnl"]            = data.get("daily_pnl", 0.0)
        state["trade_count_today"]    = data.get("trade_count_today", 0)
        state["consecutive_losses"]   = data.get("consecutive_losses", 0)
        state["last_trade_entry_time"] = data.get("last_trade_entry_time", 0)
        state["last_trade_close_time"] = data.get("last_trade_close_time", 0)
        state["last_reset_day"]       = data.get("last_reset_day", -1)
        state["blacklist"]            = data.get("blacklist", {})

        # Handle open trade — DO NOT blindly resume
        # Instead flag it for safe evaluation on first scan
        open_trade = data.get("active_trade")
        if open_trade:
            log(f"Open trade found in state: {open_trade.get('symbol')}. "
                f"Will evaluate for safe close on startup.", "WARN")
            state["active_trade"] = open_trade
            state["active_trade"]["needs_eval_on_startup"] = True

        log(f"State loaded. Mode={state['current_mode']} | "
            f"PnL={state['daily_pnl']:.2f}% | "
            f"Trades={state['trade_count_today']}", "SYSTEM")

    except Exception as e:
        log(f"State load failed: {e}. Starting fresh.", "WARN")


# ═══════════════════════════════════════════════════════════════════
#  DAILY RESET
# ═══════════════════════════════════════════════════════════════════

def check_daily_reset():
    """Reset daily counters at midnight UTC."""
    now_struct = time.gmtime()
    today = now_struct.tm_yday  # day of year

    if today != state["last_reset_day"]:
        if state["last_reset_day"] != -1:
            log("Midnight reset. Clearing daily stats.", "SYSTEM")
            log(f"Yesterday: {state['trade_count_today']} trades | "
                f"PnL {state['daily_pnl']:+.2f}%", "SYSTEM")

        state["daily_pnl"]          = 0.0
        state["trade_count_today"]  = 0
        state["consecutive_losses"] = 0
        state["last_reset_day"]     = today

        # Clean expired blacklist entries
        clean_blacklist()
        save_state()


# ═══════════════════════════════════════════════════════════════════
#  API LAYER — RATE LIMITING + EXPONENTIAL BACKOFF
# ═══════════════════════════════════════════════════════════════════

def enforce_rate_limit():
    """Ensure minimum delay between API calls."""
    elapsed = time.time() - state["last_api_call_time"]
    if elapsed < CONFIG["min_api_delay"]:
        time.sleep(CONFIG["min_api_delay"] - elapsed)
    state["last_api_call_time"] = time.time()


def api_get(url, params=None, timeout=10):
    """
    Single API call with rate limiting.
    Returns (data_dict, success_bool).
    Does NOT handle retries — caller uses safe_api_call for that.
    """
    enforce_rate_limit()
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        return data, True
    except requests.exceptions.Timeout:
        return None, False
    except requests.exceptions.HTTPError as e:
        log(f"HTTP error {e}", "DEBUG")
        return None, False
    except Exception as e:
        log(f"API error: {e}", "DEBUG")
        return None, False


def safe_api_call(url, params=None, timeout=10):
    """
    Wraps api_get with exponential backoff and failure tracking.
    Returns data dict on success, None on failure.
    """
    # Check if we're in an API pause
    if time.time() < state["api_paused_until"]:
        remaining = state["api_paused_until"] - time.time()
        log(f"API paused. {remaining:.0f}s remaining.", "DEBUG")
        return None

    backoff_seq = CONFIG["backoff_sequence"]
    attempt = 0

    while attempt <= len(backoff_seq):
        data, success = api_get(url, params=params, timeout=timeout)

        if success and data is not None:
            # Reset failure counter on success
            state["consecutive_api_failures"] = 0
            return data

        # Failed
        state["consecutive_api_failures"] += 1
        log(f"API call failed. Consecutive: {state['consecutive_api_failures']}", "DEBUG")

        # Check if we've hit the pause threshold
        if state["consecutive_api_failures"] >= CONFIG["max_consecutive_api_failures"]:
            pause = random.randint(
                CONFIG["api_failure_pause_min"],
                CONFIG["api_failure_pause_max"]
            )
            state["api_paused_until"] = time.time() + pause
            log(f"3+ consecutive API failures. Pausing {pause}s.", "WARN")
            return None

        # Exponential backoff
        if attempt < len(backoff_seq):
            delay = min(backoff_seq[attempt], CONFIG["max_backoff"])
            log(f"Retrying in {delay}s...", "DEBUG")
            time.sleep(delay)

        attempt += 1

    return None


# ═══════════════════════════════════════════════════════════════════
#  DATA INTEGRITY VALIDATOR
# ═══════════════════════════════════════════════════════════════════

def deep_get(d, *keys):
    """Safely traverse nested dict. Returns None if any key missing."""
    for key in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(key)
    return d


def validate_data(pair):
    """
    Validate all critical fields before use.
    Returns (is_valid: bool, reason: str)
    """
    if pair is None or not isinstance(pair, dict):
        return False, "pair is None or not dict"

    # Price
    price = deep_get(pair, "priceUsd")
    if price is None:
        return False, "missing priceUsd"
    try:
        price = float(price)
    except (ValueError, TypeError):
        return False, "priceUsd not numeric"
    if not (0.0000000001 <= price <= 10_000_000):
        return False, f"priceUsd out of range: {price}"
    if price != price:  # NaN
        return False, "priceUsd is NaN"

    # Volume
    vol = deep_get(pair, "volume", "h24")
    if vol is None:
        return False, "missing volume.h24"
    try:
        vol = float(vol)
    except (ValueError, TypeError):
        return False, "volume not numeric"
    if vol < 0:
        return False, f"negative volume: {vol}"

    # Liquidity
    liq = deep_get(pair, "liquidity", "usd")
    if liq is None:
        return False, "missing liquidity.usd"
    try:
        liq = float(liq)
    except (ValueError, TypeError):
        return False, "liquidity not numeric"

    # Price change
    pc = deep_get(pair, "priceChange", "m5")
    if pc is not None:
        try:
            pc = float(pc)
            if not (-99 <= pc <= 10000):
                return False, f"priceChange.m5 out of range: {pc}"
        except (ValueError, TypeError):
            pass  # non-critical, continue

    # Activity check — at least some txns in last 5 min
    buys  = deep_get(pair, "txns", "m5", "buys")  or 0
    sells = deep_get(pair, "txns", "m5", "sells") or 0
    total_txns = int(buys) + int(sells)
    if total_txns < CONFIG["min_trades_per_5min"]:
        return False, f"low activity: {total_txns} txns in 5m"

    return True, "ok"


# ═══════════════════════════════════════════════════════════════════
#  FETCH PAIRS — DEX SCREENER
# ═══════════════════════════════════════════════════════════════════

def fetch_pairs():
    """
    Fetch top pairs from DEX Screener for configured chain.
    Returns list of pair dicts, or empty list on failure.
    """


    chain = CONFIG["chain"]
    queries = [
    "https://api.dexscreener.com/latest/dex/search/?q=USDC%20bsc",
    "https://api.dexscreener.com/latest/dex/search/?q=BNB%20meme",
    "https://api.dexscreener.com/latest/dex/search/?q=BNB%20token",
]
    search_url = random.choice(queries)
    data = safe_api_call(search_url)
    if data is None:
        log("fetch_pairs: DEX Screener returned None.", "WARN")
        return []

    pairs = data.get("pairs", [])
    if not isinstance(pairs, list):
        log("fetch_pairs: unexpected response format.", "WARN")
        return []

    # Filter to configured chain
    chain_pairs = [
        p for p in pairs
        if isinstance(p, dict) and p.get("chainId", "").lower() == chain.lower()
    ]

    log(f"Fetched {len(chain_pairs)} pairs on {chain}.", "DEBUG")
    return chain_pairs


def fetch_pair_data(pair_address):
    """Fetch fresh data for a specific pair address."""
    url = f"https://api.dexscreener.com/latest/dex/pairs/{CONFIG['chain']}/{pair_address}"
    data = safe_api_call(url)
    if data is None:
        return None
    pairs = data.get("pairs", [])
    if pairs and len(pairs) > 0:
        return pairs[0]
    return None


# ═══════════════════════════════════════════════════════════════════
#  FETCH OHLCV — DERIVED FROM DEX SCREENER DATA
# ═══════════════════════════════════════════════════════════════════

def fetch_ohlcv(pair):
    """
    DEX Screener free tier doesn't provide full OHLCV candles.
    We derive a synthetic OHLCV snapshot from available fields.
    Returns dict with derived metrics, or None if insufficient data.
    """
    try:
        price_now   = float(pair.get("priceUsd", 0))
        pc_m5       = float(deep_get(pair, "priceChange", "m5")  or 0)
        pc_h1       = float(deep_get(pair, "priceChange", "h1")  or 0)
        pc_h6       = float(deep_get(pair, "priceChange", "h6")  or 0)
        pc_h24      = float(deep_get(pair, "priceChange", "h24") or 0)
        vol_h24     = float(deep_get(pair, "volume", "h24") or 0)
        vol_h6      = float(deep_get(pair, "volume", "h6")  or 0)
        vol_h1      = float(deep_get(pair, "volume", "h1")  or 0)
        vol_m5      = float(deep_get(pair, "volume", "m5")  or 0)
        buys_m5     = int(deep_get(pair, "txns", "m5", "buys")  or 0)
        sells_m5    = int(deep_get(pair, "txns", "m5", "sells") or 0)
        buys_h1     = int(deep_get(pair, "txns", "h1", "buys")  or 0)
        sells_h1    = int(deep_get(pair, "txns", "h1", "sells") or 0)

        if price_now <= 0:
            return None

        # Derive prices from % changes
        price_5min_ago = price_now / (1 + pc_m5 / 100) if pc_m5 != -100 else price_now
        price_1hr_ago  = price_now / (1 + pc_h1 / 100) if pc_h1 != -100 else price_now

        # Estimate average volume per 5-min slot over last hour
        vol_per_5min_avg = vol_h1 / 12 if vol_h1 > 0 else 0

        # Estimate candle direction from recent price changes
        # Positive m5 = last candle likely green, etc.
        candles_green = 0
        if pc_m5  > 0: candles_green += 1
        if pc_h1  > 0: candles_green += 1
        if pc_h6  > 0: candles_green += 1
        if pc_h24 > 0: candles_green += 1

        # Buy pressure ratio in last 5 min
        total_m5 = buys_m5 + sells_m5
        buy_ratio_m5 = buys_m5 / total_m5 if total_m5 > 0 else 0.5

        return {
            "price_now":         price_now,
            "price_5min_ago":    price_5min_ago,
            "price_1hr_ago":     price_1hr_ago,
            "pc_m5":             pc_m5,
            "pc_h1":             pc_h1,
            "vol_m5":            vol_m5,
            "vol_h1":            vol_h1,
            "vol_h24":           vol_h24,
            "vol_per_5min_avg":  vol_per_5min_avg,
            "buys_m5":           buys_m5,
            "sells_m5":          sells_m5,
            "buy_ratio_m5":      buy_ratio_m5,
            "candles_green":     candles_green,
            "total_txns_m5":     total_m5,
            "total_txns_h1":     buys_h1 + sells_h1,
        }
    except Exception as e:
        log(f"fetch_ohlcv error: {e}", "DEBUG")
        return None


# ═══════════════════════════════════════════════════════════════════
#  BTC PRICE BUFFER HELPERS
# ═══════════════════════════════════════════════════════════════════

def find_closest_price(buffer, target_time):
    """
    Given deque of (timestamp, price), find price closest to target_time.
    Returns price float or None.
    """
    if not buffer:
        return None
    closest = min(buffer, key=lambda x: abs(x[0] - target_time))
    # Only use if within 3 minutes of target
    if abs(closest[0] - target_time) > 180:
        return None
    return closest[1]


def update_btc_buffer(btc_price):
    """Add current BTC price with timestamp to buffer."""
    state["btc_price_buffer"].append((time.time(), btc_price))


# ═══════════════════════════════════════════════════════════════════
#  MARKET CONDITIONS — BTC TREND + DEX VOLUME
# ═══════════════════════════════════════════════════════════════════

def fetch_btc_price():
    """Fetch BTC price from OKX public API."""
    url = "https://www.okx.com/api/v5/market/ticker"
    params = {"instId": "BTC-USDT"}
    data = safe_api_call(url, params=params)
    if data is None:
        return None
    try:
        return float(data["data"][0]["last"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None


def get_btc_trend():
    """
    Determine BTC trend using price buffer.
    Returns: "UP" | "FLAT" | "DOWN"
    """
    buf = state["btc_price_buffer"]
    if len(buf) < 4:
        return "FLAT"  # Not enough data yet

    current_time  = time.time()
    current_price = buf[-1][1]

    # Price ~5 minutes ago
    price_5m = find_closest_price(buf, current_time - 300)
    # Price ~15 minutes ago
    price_15m = find_closest_price(buf, current_time - 900)

    if price_5m is None or price_15m is None:
        return "FLAT"

    change_5m  = (current_price - price_5m)  / price_5m  * 100
    change_15m = (current_price - price_15m) / price_15m * 100

    # Circuit breaker: BTC drops >2% in 5 min
    if change_5m <= CONFIG["btc_drop_trigger_pct"]:
        pause = random.randint(
            CONFIG["circuit_breaker_min"],
            CONFIG["circuit_breaker_max"]
        )
        state["circuit_breaker_until"] = time.time() + pause
        log(f"CIRCUIT BREAKER: BTC {change_5m:.2f}% in 5min. "
            f"Pausing {pause}s.", "WARN", force=True)
        return "DOWN"

    if change_15m > 1.5:
        return "UP"
    elif change_15m < -1.5:
        return "DOWN"
    else:
        return "FLAT"


def fetch_dex_global_volume():
    """
    Estimate global DEX volume from aggregate of fetched pairs.
    Returns total USD volume of scanned pairs, or None.
    """
    # We use the pairs we already fetched — no extra API call
    # This is called after fetch_pairs(), passing vol data
    # Returns the sum from state buffer
    buf = state["dex_volume_buffer"]
    if len(buf) == 0:
        return None
    return buf[-1] if buf else None


def check_market_conditions():
    """
    Determine adaptive trading mode.
    Uses 2-consecutive-check stability filter.
    Returns mode string: "DORMANT" | "QUIET" | "ACTIVE" | "HOT"
    Also updates BTC buffer and checks circuit breaker.
    """
    now = time.time()

    # Respect minimum check interval (2 min)
    if now - state["last_mode_check_time"] < 120:
        return state["current_mode"]

    state["last_mode_check_time"] = now

    # Fetch BTC price
    btc_price = fetch_btc_price()
    if btc_price is not None:
        update_btc_buffer(btc_price)

    btc_trend = get_btc_trend()

    # DEX volume check
    dex_buf = state["dex_volume_buffer"]
    dex_low = False
    if len(dex_buf) >= 10:
        rolling_avg = sum(dex_buf) / len(dex_buf)
        current_vol = dex_buf[-1]
        if rolling_avg > 0:
            vol_ratio = current_vol / rolling_avg
            # Circuit breaker: volume spikes 3x (market chaos)
            if vol_ratio >= CONFIG["dex_volume_spike_x"]:
                pause = random.randint(
                    CONFIG["circuit_breaker_min"],
                    CONFIG["circuit_breaker_max"]
                )
                state["circuit_breaker_until"] = now + pause
                log(f"CIRCUIT BREAKER: DEX vol spike {vol_ratio:.1f}x. "
                    f"Pausing {pause}s.", "WARN", force=True)
                return "DORMANT"
            dex_low = vol_ratio < CONFIG["min_dex_volume_ratio"]

    # Mode determination matrix
    if btc_trend == "DOWN":
        raw_mode = "DORMANT"
    elif btc_trend == "UP" and not dex_low:
        raw_mode = "HOT"
    elif btc_trend == "UP" and dex_low:
        raw_mode = "ACTIVE"
    elif btc_trend == "FLAT" and not dex_low:
        raw_mode = "ACTIVE"
    else:
        raw_mode = "QUIET"

    # Stability filter: require 2 consecutive same readings
    if raw_mode == state["last_candidate_mode"]:
        # Confirmed — apply mode change
        if raw_mode != state["current_mode"]:
            log(f"Mode change: {state['current_mode']} → {raw_mode}", "MODE", force=True)
        state["current_mode"] = raw_mode
        state["last_candidate_mode"] = None
    else:
        # Not yet confirmed — store as candidate
        state["last_candidate_mode"] = raw_mode
        log(f"Mode candidate: {raw_mode} (needs 1 more confirmation)", "DEBUG")

    return state["current_mode"]


# ═══════════════════════════════════════════════════════════════════
#  TOKEN CLASSIFICATION — COINGECKO
# ═══════════════════════════════════════════════════════════════════

_coingecko_cache = {}   # symbol → (coin_type, timestamp)
_CACHE_TTL = 3600       # 1 hour

def classify_token(pair):
    """
    Classify token as 'memecoin', 'mid_cap', or 'large_cap'
    using CoinGecko market cap data. Falls back to 'memecoin' on failure.
    """
    base_token = deep_get(pair, "baseToken", "symbol") or ""
    base_addr  = deep_get(pair, "baseToken", "address") or ""
    symbol = base_token.lower()

    # Known large caps — skip API call
    large_cap_known = {
        "btc", "eth", "sol", "bnb", "xrp", "usdc", "usdt",
        "ada", "avax", "dot", "matic", "link", "uni", "ltc"
    }
    if symbol in large_cap_known:
        return "large_cap"

    # Check cache
    now = time.time()
    if symbol in _coingecko_cache:
        cached_type, cached_time = _coingecko_cache[symbol]
        if now - cached_time < _CACHE_TTL:
            return cached_type

    # Query CoinGecko
    url = "https://api.coingecko.com/api/v3/search"
    params = {"query": symbol}
    data = safe_api_call(url, params=params)

    if data is None:
        _coingecko_cache[symbol] = ("memecoin", now)
        return "memecoin"

    coins = data.get("coins", [])
    if not coins:
        _coingecko_cache[symbol] = ("memecoin", now)
        return "memecoin"

    # Take the first result's market cap rank as a proxy
    top_coin = coins[0]
    mcap_rank = top_coin.get("market_cap_rank")

    if mcap_rank is None:
        coin_type = "memecoin"
    elif mcap_rank <= 50:
        coin_type = "large_cap"
    elif mcap_rank <= 300:
        coin_type = "mid_cap"
    else:
        coin_type = "memecoin"

    _coingecko_cache[symbol] = (coin_type, now)
    log(f"Classified {symbol.upper()} → {coin_type} (rank #{mcap_rank})", "DEBUG")
    return coin_type


# ═══════════════════════════════════════════════════════════════════
#  TOKEN VALIDATION
# ═══════════════════════════════════════════════════════════════════

def validate_token(pair, ohlcv):
    """
    Run all pre-filters before scoring.
    Returns (is_valid: bool, reason: str)
    """
    # 1. Liquidity check
    liq = float(deep_get(pair, "liquidity", "usd") or 0)
    if liq < CONFIG["min_liquidity_usd"]:
        return False, f"liquidity too low: ${liq:,.0f}"

    # 2. Spread check
    spread = estimate_spread(pair, ohlcv)
    if spread is not None and spread > CONFIG["max_spread_pct"]:
        return False, f"spread too wide: {spread*100:.2f}%"

    # 3. Activity check (already in validate_data, but double check)
    if ohlcv["total_txns_m5"] < CONFIG["min_trades_per_5min"]:
        return False, f"insufficient activity: {ohlcv['total_txns_m5']} txns/5m"

    # 4. Price must be moving upward (basic bullish gate)
    if ohlcv["pc_m5"] <= 0:
        return False, f"price not rising: {ohlcv['pc_m5']:.2f}% in 5m"

    # 5. Volume must be positive
    if ohlcv["vol_m5"] <= 0:
        return False, "zero volume in last 5m"

    # 6. Price glitch filter
    if abs(ohlcv["pc_m5"]) > CONFIG["price_glitch_pct"] * 100:
        # Only flag as glitch if volume doesn't confirm
        if ohlcv["vol_m5"] < ohlcv["vol_per_5min_avg"] * 0.5:
            return False, f"price glitch: {ohlcv['pc_m5']:.1f}% with low volume"

    return True, "ok"


def estimate_spread(pair, ohlcv):
    """
    Estimate bid/ask spread.
    DEX Screener doesn't provide bid/ask directly.
    We estimate from price impact proxies.
    """
    try:
        price = ohlcv["price_now"]
        # Rough estimate: 0.3% base for liquid pairs, up to 2% for thin ones
        liq = float(deep_get(pair, "liquidity", "usd") or 0)
        if liq <= 0:
            return 0.02  # assume 2% if no liquidity data

        # Spread inversely proportional to liquidity (rough model)
        if liq >= 1_000_000:
            return 0.002    # 0.2% — very liquid
        elif liq >= 200_000:
            return 0.005    # 0.5%
        elif liq >= 50_000:
            return 0.008    # 0.8%
        else:
            return 0.015    # 1.5% — thin
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
#  OVEREXTENSION FILTER
# ═══════════════════════════════════════════════════════════════════

def check_overextension(ohlcv, score_tier):
    """
    Check if token has already moved too much.
    Returns (allow: bool, reason: str)
    score_tier: "weak" | "medium" | "strong"
    """
    recent_move = ohlcv["pc_m5"] / 100  # convert % to decimal

    if recent_move > CONFIG["overextension_hard"]:
        return False, f"overextended (hard): {ohlcv['pc_m5']:.1f}% move"

    if recent_move > CONFIG["overextension_soft"]:
        # Smart exception: allow only if strong breakout + volume increasing
        if score_tier == "strong" and ohlcv["buy_ratio_m5"] > 0.6:
            log(f"Overextension exception granted: strong breakout + "
                f"buy ratio {ohlcv['buy_ratio_m5']:.0%}", "DEBUG")
            return True, "overextension exception: strong confirmed"
        return False, f"overextended (soft): {ohlcv['pc_m5']:.1f}% move"

    return True, "ok"


# ═══════════════════════════════════════════════════════════════════
#  BLACKLIST MANAGER
# ═══════════════════════════════════════════════════════════════════

def blacklist_token(token_address, peak_price, duration=None):
    """Add token to blacklist."""
    if duration is None:
        duration = CONFIG["blacklist_duration"]
    state["blacklist"][token_address] = {
        "blacklisted_at": time.time(),
        "peak_price":     peak_price,
        "duration":       duration
    }
    log(f"Blacklisted {token_address[:8]}... for {duration}s", "DEBUG")


def is_blacklisted(token_address, current_price):
    """
    Check if token is blacklisted.
    Allows re-entry if:
      - Minimum duration served AND
      - Price has broken above previous peak × 1.01
    """
    if token_address not in state["blacklist"]:
        return False

    entry = state["blacklist"][token_address]
    elapsed = time.time() - entry["blacklisted_at"]

    # Must serve minimum duration
    if elapsed < entry["duration"]:
        return True

    # After minimum: check for structural breakout
    peak = entry["peak_price"]
    if current_price > peak * 1.01:
        # Structural breakout confirmed — remove from blacklist
        del state["blacklist"][token_address]
        log(f"Blacklist cleared: structural breakout above peak.", "DEBUG")
        return False

    # Time served but no breakout — stays blacklisted
    return True


def clean_blacklist():
    """Remove very old blacklist entries (>2x duration)."""
    now = time.time()
    to_remove = []
    for addr, entry in state["blacklist"].items():
        if now - entry["blacklisted_at"] > entry["duration"] * 2:
            to_remove.append(addr)
    for addr in to_remove:
        del state["blacklist"][addr]
    if to_remove:
        log(f"Cleaned {len(to_remove)} expired blacklist entries.", "DEBUG")


# ═══════════════════════════════════════════════════════════════════
#  SCORING ENGINE — 0 to 100
# ═══════════════════════════════════════════════════════════════════

def calculate_score(ohlcv, pair):
    """
    Full 0–100 scoring engine. Every component explicitly calculated.
    Returns (score: int, breakdown: dict)
    """
    score = 0
    breakdown = {}

    price_now        = ohlcv["price_now"]
    vol_m5           = ohlcv["vol_m5"]
    vol_per_5min_avg = ohlcv["vol_per_5min_avg"]
    pc_m5            = ohlcv["pc_m5"]
    candles_green    = ohlcv["candles_green"]
    buys_m5          = ohlcv["buys_m5"]
    sells_m5         = ohlcv["sells_m5"]
    total_txns_m5    = ohlcv["total_txns_m5"]
    buy_ratio        = ohlcv["buy_ratio_m5"]

    # ── +25: Volume spike ≥ 3x average ──────────────────────────────
    vol_spike_pts = 0
    vol_spike_x = 0
    if vol_per_5min_avg > 0:
        vol_spike_x = vol_m5 / vol_per_5min_avg
        if vol_spike_x >= CONFIG["volume_spike_multiplier"]:
            vol_spike_pts = 25
    score += vol_spike_pts
    breakdown["volume_spike"] = {
        "pts": vol_spike_pts,
        "value": f"{vol_spike_x:.1f}x"
    }

    # ── +20: Price momentum ≥ +2% in last 5m ────────────────────────
    momentum_pts = 0
    if pc_m5 >= CONFIG["momentum_pct_min"] * 100:
        momentum_pts = 20
    elif pc_m5 > 0:
        # Partial credit for positive but sub-threshold momentum
        momentum_pts = int(pc_m5 / (CONFIG["momentum_pct_min"] * 100) * 20)
    score += momentum_pts
    breakdown["momentum"] = {
        "pts": momentum_pts,
        "value": f"{pc_m5:.2f}%"
    }

    # ── +15: 3–5 consecutive green candles ──────────────────────────
    green_pts = 0
    if candles_green >= CONFIG["min_green_candles"]:
        green_pts = 15
    elif candles_green == 2:
        green_pts = 8
    score += green_pts
    breakdown["green_candles"] = {
        "pts": green_pts,
        "value": f"{candles_green} green"
    }

    # ── +15: Breakout of recent high ────────────────────────────────
    # Proxy: price_now > price_1hr_ago AND pc_m5 > 0 AND pc_h1 > 0
    breakout_pts = 0
    if ohlcv["pc_h1"] > 0 and ohlcv["pc_m5"] > 0:
        # Price trending up across both timeframes
        breakout_pts = 15
    score += breakout_pts
    breakdown["breakout"] = {
        "pts": breakout_pts,
        "value": f"h1={ohlcv['pc_h1']:.1f}% m5={pc_m5:.1f}%"
    }

    # ── +10: Liquidity above threshold ──────────────────────────────
    liq_pts = 0
    liq = float(deep_get(pair, "liquidity", "usd") or 0)
    if liq >= CONFIG["min_liquidity_usd"]:
        liq_pts = 10
    score += liq_pts
    breakdown["liquidity"] = {
        "pts": liq_pts,
        "value": f"${liq:,.0f}"
    }

    # ── +10: No long upper wicks ─────────────────────────────────────
    # Proxy: buy_ratio > 0.5 (more buyers than sellers = no rejection)
    wick_pts = 0
    if buy_ratio > 0.55:
        wick_pts = 10
    elif buy_ratio > 0.45:
        wick_pts = 5
    score += wick_pts
    breakdown["no_wick"] = {
        "pts": wick_pts,
        "value": f"buy ratio {buy_ratio:.0%}"
    }

    # ── +5: Increasing trade frequency ──────────────────────────────
    freq_pts = 0
    h1_per_5min = ohlcv["total_txns_h1"] / 12 if ohlcv["total_txns_h1"] > 0 else 0
    if h1_per_5min > 0 and total_txns_m5 > h1_per_5min * 1.2:
        freq_pts = 5
    score += freq_pts
    breakdown["trade_freq"] = {
        "pts": freq_pts,
        "value": f"{total_txns_m5} vs avg {h1_per_5min:.0f}"
    }

    # ── +10: Micro pullback before breakout ─────────────────────────
    # Proxy: h1 positive, m5 positive but < h1 rate (pulled back then resumed)
    pullback_pts = 0
    if ohlcv["pc_h1"] > 2 and 0 < pc_m5 < ohlcv["pc_h1"]:
        pullback_pts = 10
    score += pullback_pts
    breakdown["pullback"] = {
        "pts": pullback_pts,
        "value": f"h1={ohlcv['pc_h1']:.1f}% m5={pc_m5:.1f}%"
    }

    # ── -20 PENALTY: Volume spike WITHOUT price continuation ─────────
    penalty = 0
    if vol_spike_x >= CONFIG["volume_spike_multiplier"] and pc_m5 <= 0:
        penalty = -20
        score += penalty
    breakdown["penalty"] = {
        "pts": penalty,
        "value": "vol spike no price follow" if penalty < 0 else "none"
    }

    score = max(0, min(100, score))
    return score, breakdown


# ═══════════════════════════════════════════════════════════════════
#  MOMENTUM CLASSIFIER
# ═══════════════════════════════════════════════════════════════════

def classify_momentum(score, mode_threshold, ohlcv):
    """
    Classify entry momentum as weak / medium / strong.
    Determines profit-taking strategy.
    """
    margin = score - mode_threshold
    vol_spike_x = (ohlcv["vol_m5"] / ohlcv["vol_per_5min_avg"]
                   if ohlcv["vol_per_5min_avg"] > 0 else 1)

    if margin >= 12 and vol_spike_x >= 4 and ohlcv["pc_m5"] >= 3:
        return "strong"
    elif margin >= 6 and vol_spike_x >= 2.5:
        return "medium"
    else:
        return "weak"


# ═══════════════════════════════════════════════════════════════════
#  RISK MANAGER
# ═══════════════════════════════════════════════════════════════════

def can_trade():
    """
    Check all risk gates before allowing a new trade.
    Returns (allowed: bool, reason: str)
    """
    now = time.time()

    # Kill switch
    if not CONFIG["RUNNING"]:
        return False, "kill switch active"

    # Warmup period
    if now - state["startup_time"] < CONFIG["warmup_seconds"]:
        remaining = CONFIG["warmup_seconds"] - (now - state["startup_time"])
        return False, f"warmup: {remaining:.0f}s remaining"

    # Circuit breaker
    if now < state["circuit_breaker_until"]:
        remaining = state["circuit_breaker_until"] - now
        return False, f"circuit breaker: {remaining:.0f}s remaining"

    # API pause
    if now < state["api_paused_until"]:
        remaining = state["api_paused_until"] - now
        return False, f"API paused: {remaining:.0f}s remaining"

    # Market dormant
    if state["current_mode"] == "DORMANT":
        return False, "market dormant"

    # Active trade
    if state["active_trade"] is not None:
        return False, "trade already active"

    # Daily trade limit
    if state["trade_count_today"] >= CONFIG["max_trades_per_day"]:
        return False, f"daily limit reached ({CONFIG['max_trades_per_day']} trades)"

    # Daily loss limit
    if state["daily_pnl"] <= CONFIG["daily_loss_limit_pct"]:
        return False, f"daily loss limit hit ({state['daily_pnl']:.2f}%)"

    # Consecutive losses
    if state["consecutive_losses"] >= CONFIG["max_consecutive_losses"]:
        return False, f"{state['consecutive_losses']} consecutive losses"

    # Post-trade cooldown
    if now - state["last_trade_close_time"] < CONFIG["cooldown_after_trade"]:
        remaining = CONFIG["cooldown_after_trade"] - (now - state["last_trade_close_time"])
        return False, f"post-trade cooldown: {remaining:.0f}s"

    # Duplicate entry cooldown
    if now - state["last_trade_entry_time"] < CONFIG["duplicate_entry_cooldown"]:
        remaining = CONFIG["duplicate_entry_cooldown"] - (now - state["last_trade_entry_time"])
        return False, f"duplicate cooldown: {remaining:.0f}s"

    return True, "ok"


# ═══════════════════════════════════════════════════════════════════
#  ENTRY CONFIRMATION
# ═══════════════════════════════════════════════════════════════════

def confirm_entry(pair_address, initial_score, mode_threshold):
    """
    Wait reconfirmation window then re-check score.
    Returns (confirmed: bool, fresh_score: int, reason: str)
    """
    delay = random.uniform(CONFIG["reconfirm_min"], CONFIG["reconfirm_max"])
    log(f"Reconfirmation window: waiting {delay:.1f}s...", "DEBUG")
    time.sleep(delay)

    fresh_pair = fetch_pair_data(pair_address)
    if fresh_pair is None:
        return False, 0, "API failure during reconfirm"

    valid, reason = validate_data(fresh_pair)
    if not valid:
        return False, 0, f"data invalid on reconfirm: {reason}"

    fresh_ohlcv = fetch_ohlcv(fresh_pair)
    if fresh_ohlcv is None:
        return False, 0, "OHLCV failed on reconfirm"

    fresh_score, _ = calculate_score(fresh_ohlcv, fresh_pair)

    # Score decay check
    score_drop = initial_score - fresh_score
    if score_drop > 10:
        return False, fresh_score, f"score decayed {score_drop} pts ({initial_score}→{fresh_score})"

    # Still above threshold
    if fresh_score < mode_threshold:
        return False, fresh_score, f"score below threshold: {fresh_score} < {mode_threshold}"

    # Price still rising or holding
    if fresh_ohlcv["pc_m5"] < 0:
        return False, fresh_score, "price reversed during reconfirm"

    return True, fresh_score, "confirmed"


# ═══════════════════════════════════════════════════════════════════
#  TRADE SIMULATION ENGINE
# ═══════════════════════════════════════════════════════════════════

def open_trade(pair, ohlcv, coin_type, momentum_tier, score):
    """
    Simulate opening a trade. Applies slippage + fill factor.
    """
    market_price = ohlcv["price_now"]
    fill_factor  = random.uniform(CONFIG["fill_factor_min"], CONFIG["fill_factor_max"])
    entry_price  = market_price * CONFIG["entry_slippage"] * fill_factor

    symbol = (
        f"{deep_get(pair, 'baseToken', 'symbol') or '???'}"
        f"/{deep_get(pair, 'quoteToken', 'symbol') or 'USDT'}"
    )
    pair_address = deep_get(pair, "pairAddress") or ""

    # Trailing stop distance for this coin type
    trail_pct = CONFIG["trailing_stop"][coin_type]

    trade = {
        "symbol":          symbol,
        "pair_address":    pair_address,
        "coin_type":       coin_type,
        "momentum_tier":   momentum_tier,
        "entry_score":     score,
        "entry_price":     entry_price,
        "market_price_at_entry": market_price,
        "fill_factor":     fill_factor,
        "peak_price":      entry_price,
        "entry_time":      time.time(),
        "trade_size_usd":  CONFIG["trade_size_usd"],
        "tp1_price":       entry_price * (1 + CONFIG["tp1_pct"][coin_type]),
        "tp2_price":       entry_price * (1 + CONFIG["tp2_pct"][coin_type]),
        "stop_loss_price": entry_price * (1 - CONFIG["stop_loss"][coin_type]),
        "trailing_stop_pct": trail_pct,
        "trailing_stop_price": entry_price * (1 - trail_pct),
        "trailing_active": False,
        "tp1_hit":         False,
        "volume_buffer":   deque(maxlen=CONFIG["volume_buffer_size"]),
        "last_new_high_time": time.time(),
        "needs_eval_on_startup": False,
        # Quality tracking
        "max_runup_pct":   0.0,
        "max_drawdown_pct": 0.0,
        "entry_strength":  momentum_tier,
        "exit_type":       None,
    }

    # Seed volume buffer
    trade["volume_buffer"].append(ohlcv["vol_m5"])

    state["active_trade"] = trade
    state["last_trade_entry_time"] = time.time()
    state["trade_count_today"] += 1

    mode_emoji = {"QUIET": "🟡", "ACTIVE": "🟢", "HOT": "🔥"}.get(
        state["current_mode"], "⚪"
    )

    log_separator("═")
    log(f"TRADE OPENED {mode_emoji}", "TRADE", force=True)
    log(f"  Symbol    : {symbol}", "TRADE", force=True)
    log(f"  Type      : {coin_type} | Momentum: {momentum_tier}", "TRADE", force=True)
    log(f"  Score     : {score}/100", "TRADE", force=True)
    log(f"  Entry     : ${entry_price:.8f} (slip+fill applied)", "TRADE", force=True)
    log(f"  TP1       : ${trade['tp1_price']:.8f} (+{CONFIG['tp1_pct'][coin_type]*100:.0f}%)", "TRADE", force=True)
    log(f"  TP2       : ${trade['tp2_price']:.8f} (+{CONFIG['tp2_pct'][coin_type]*100:.0f}%)", "TRADE", force=True)
    log(f"  Stop Loss : ${trade['stop_loss_price']:.8f} (-{CONFIG['stop_loss'][coin_type]*100:.0f}%)", "TRADE", force=True)
    log(f"  Trail Stop: {trail_pct*100:.1f}% from peak", "TRADE", force=True)
    log_separator("═")

    save_state()
    return trade


def close_trade(trade, exit_price_raw, exit_type, reason=""):
    """
    Simulate closing a trade. Applies exit slippage + fill factor.
    Records full trade quality metrics.
    """
    fill_factor = random.uniform(CONFIG["fill_factor_min"], CONFIG["fill_factor_max"])
    exit_price  = exit_price_raw * CONFIG["exit_slippage"] * fill_factor

    entry_price = trade["entry_price"]
    pnl_pct     = (exit_price - entry_price) / entry_price * 100
    pnl_usd     = CONFIG["trade_size_usd"] * pnl_pct / 100
    hold_sec    = time.time() - trade["entry_time"]

    # Update state
    state["daily_pnl"] += pnl_pct
    state["active_trade"] = None
    state["last_trade_close_time"] = time.time()

    if pnl_pct >= 0:
        state["consecutive_losses"] = 0
        result_emoji = "✅ WIN"
    else:
        state["consecutive_losses"] += 1
        result_emoji = "❌ LOSS"

    # Build trade record
    record = {
        "symbol":          trade["symbol"],
        "coin_type":       trade["coin_type"],
        "entry_strength":  trade["entry_strength"],
        "exit_type":       exit_type,
        "entry_price":     entry_price,
        "exit_price":      exit_price,
        "pnl_pct":         round(pnl_pct, 4),
        "pnl_usd":         round(pnl_usd, 4),
        "hold_seconds":    round(hold_sec, 1),
        "max_runup_pct":   trade["max_runup_pct"],
        "max_drawdown_pct": trade["max_drawdown_pct"],
        "entry_score":     trade["entry_score"],
        "timestamp":       time.time(),
    }
    state["trade_history"].append(record)

    # Blacklist the token after exit
    blacklist_token(
        trade["pair_address"],
        peak_price=trade["peak_price"]
    )

    hold_str = (
        f"{hold_sec/60:.1f}m" if hold_sec >= 60
        else f"{hold_sec:.0f}s"
    )

    log_separator("═")
    log(f"TRADE CLOSED — {result_emoji}", "EXIT", force=True)
    log(f"  Symbol     : {trade['symbol']}", "EXIT", force=True)
    log(f"  Exit Type  : {exit_type}", "EXIT", force=True)
    log(f"  Entry      : ${entry_price:.8f}", "EXIT", force=True)
    log(f"  Exit       : ${exit_price:.8f}", "EXIT", force=True)
    log(f"  PnL        : {pnl_pct:+.2f}% (${pnl_usd:+.4f})", "EXIT", force=True)
    log(f"  Hold Time  : {hold_str}", "EXIT", force=True)
    log(f"  Max Runup  : +{trade['max_runup_pct']:.2f}%", "EXIT", force=True)
    log(f"  Max DD     : -{trade['max_drawdown_pct']:.2f}%", "EXIT", force=True)
    log(f"  Daily PnL  : {state['daily_pnl']:+.2f}%", "EXIT", force=True)
    if reason:
        log(f"  Reason     : {reason}", "EXIT", force=True)
    log_separator("═")

    save_state()
    return record


# ═══════════════════════════════════════════════════════════════════
#  TRADE MONITOR — RUNS EVERY 8s DURING ACTIVE TRADE
# ═══════════════════════════════════════════════════════════════════

def monitor_trade():
    """
    Called every 8–9.5 seconds while a trade is active.
    Checks exit conditions in strict priority order.
    Returns True if trade was closed, False if still open.
    """
    trade = state["active_trade"]
    if trade is None:
        return True

    now = time.time()

    # ── Handle startup re-evaluation ────────────────────────────────
    if trade.get("needs_eval_on_startup"):
        log("Evaluating open trade recovered from state file...", "WARN")
        fresh_pair = fetch_pair_data(trade["pair_address"])
        if fresh_pair is None:
            close_trade(trade, trade["entry_price"], "STARTUP_EVAL_FAIL",
                        "Could not fetch price on startup")
            return True
        valid, _ = validate_data(fresh_pair)
        if not valid:
            close_trade(trade, trade["entry_price"], "STARTUP_EVAL_FAIL",
                        "Invalid data on startup")
            return True
        ohlcv = fetch_ohlcv(fresh_pair)
        if ohlcv:
            close_trade(trade, ohlcv["price_now"], "STARTUP_SAFE_CLOSE",
                        "Safely closed open trade from previous session")
        trade["needs_eval_on_startup"] = False
        return True

    # ── Fetch current price ──────────────────────────────────────────
    fresh_pair = fetch_pair_data(trade["pair_address"])
    if fresh_pair is None:
        log("Monitor: API failure. Skipping this check.", "WARN")
        return False

    valid, reason = validate_data(fresh_pair)
    if not valid:
        log(f"Monitor: invalid data ({reason}). Skipping.", "WARN")
        return False

    ohlcv = fetch_ohlcv(fresh_pair)
    if ohlcv is None:
        return False

    current_price = ohlcv["price_now"]
    entry_price   = trade["entry_price"]
    peak_price    = trade["peak_price"]

    # Update peak price
    if current_price > peak_price:
        trade["peak_price"] = current_price
        trade["last_new_high_time"] = now
        # Activate trailing stop after +3% from entry
        if (current_price - entry_price) / entry_price >= 0.03:
            trade["trailing_active"] = True

    # Update trailing stop price
    trail_pct = trade["trailing_stop_pct"]
    trade["trailing_stop_price"] = trade["peak_price"] * (1 - trail_pct)

    # Track max runup and drawdown
    runup_pct = (current_price - entry_price) / entry_price * 100
    drawdown_from_peak = (peak_price - current_price) / peak_price * 100
    trade["max_runup_pct"]   = max(trade["max_runup_pct"], runup_pct)
    trade["max_drawdown_pct"] = max(trade["max_drawdown_pct"], drawdown_from_peak)

    # Update volume buffer (smoothed)
    trade["volume_buffer"].append(ohlcv["vol_m5"])
    smoothed_vol = sum(trade["volume_buffer"]) / len(trade["volume_buffer"])
    entry_vol = trade["volume_buffer"][0] if trade["volume_buffer"] else 1

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 1: TRAILING STOP (highest authority)
    # ════════════════════════════════════════════════════════════════
    if trade["trailing_active"]:
        if current_price <= trade["trailing_stop_price"]:
            close_trade(trade, current_price, "TRAILING_STOP",
                        f"price {current_price:.8f} ≤ trail {trade['trailing_stop_price']:.8f}")
            return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 2: HARD STOP LOSS
    # ════════════════════════════════════════════════════════════════
    if current_price <= trade["stop_loss_price"]:
        close_trade(trade, current_price, "STOP_LOSS",
                    f"price hit stop loss")
        return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 3: ENTRY INVALIDATION (first 20-30s only)
    # ════════════════════════════════════════════════════════════════
    time_in_trade = now - trade["entry_time"]
    if time_in_trade <= CONFIG["invalidation_window_sec"]:
        min_move = CONFIG["invalidation_min_move"]
        if current_price < entry_price * (1 + min_move):
            close_trade(trade, current_price, "INVALIDATION",
                        f"no +0.5% move in first {time_in_trade:.0f}s")
            return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 4: DAILY LOSS LIMIT
    # ════════════════════════════════════════════════════════════════
    unrealized_pnl = (current_price - entry_price) / entry_price * 100
    projected_daily = state["daily_pnl"] + unrealized_pnl
    if projected_daily <= CONFIG["daily_loss_limit_pct"]:
        close_trade(trade, current_price, "DAILY_LOSS_LIMIT",
                    f"projected daily PnL {projected_daily:.2f}% ≤ limit")
        return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 5: TAKE PROFIT TIERS
    # ════════════════════════════════════════════════════════════════
    if not trade["tp1_hit"] and current_price >= trade["tp1_price"]:
        trade["tp1_hit"] = True
        log(f"TP1 hit: +{(current_price/entry_price-1)*100:.1f}% | "
            f"Activating trailing stop", "TRADE", force=True)
        trade["trailing_active"] = True
        # Partial exit simulation — log only, main position tracked as whole
        # (simplified: full position closes at TP2 or trailing)

    if current_price >= trade["tp2_price"]:
        close_trade(trade, current_price, "TP2",
                    f"TP2 target reached")
        return True

    # Weak momentum: flat exit at smaller target
    if trade["momentum_tier"] == "weak":
        weak_target = entry_price * 1.02
        if current_price >= weak_target:
            close_trade(trade, current_price, "TP_WEAK",
                        "weak momentum flat exit +2%")
            return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 6: FLATLINE EXIT (time + volume BOTH required)
    # ════════════════════════════════════════════════════════════════
    time_since_high = now - trade["last_new_high_time"]
    vol_trending_down = (smoothed_vol < entry_vol * 0.75) if entry_vol > 0 else False

    if time_since_high >= 60 and vol_trending_down:
        close_trade(trade, current_price, "FLATLINE",
                    f"no new high for {time_since_high:.0f}s + volume declining")
        return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 7: PULLBACK TOLERANCE CHECK
    # ════════════════════════════════════════════════════════════════
    # With improved tolerance: stay in if within 0.5-1% of peak
    # Exit only if dropped more than trail distance (handled above)
    # Or if dropped 1% from peak AND volume < 60%
    drop_from_peak_pct = (peak_price - current_price) / peak_price * 100
    vol_ratio_to_entry = smoothed_vol / entry_vol if entry_vol > 0 else 1

    if drop_from_peak_pct > 1.0 and vol_ratio_to_entry < 0.60:
        close_trade(trade, current_price, "PULLBACK_FAIL",
                    f"-{drop_from_peak_pct:.2f}% from peak + low volume")
        return True

    # ════════════════════════════════════════════════════════════════
    # PRIORITY 8: HARD CEILING
    # ════════════════════════════════════════════════════════════════
    max_hold = CONFIG["max_hold_seconds"][trade["coin_type"]]
    if time_in_trade >= max_hold:
        close_trade(trade, current_price, "HARD_CEILING",
                    f"max hold {max_hold}s reached")
        return True

    # ── Still in trade ───────────────────────────────────────────────
    current_pnl = (current_price - entry_price) / entry_price * 100
    log(f"  Monitor: {trade['symbol']} | "
        f"PnL {current_pnl:+.2f}% | "
        f"Peak {(peak_price/entry_price-1)*100:+.2f}% | "
        f"Trail {'ON' if trade['trailing_active'] else 'OFF'} | "
        f"Vol ratio {vol_ratio_to_entry:.1%}", "DEBUG")

    return False


# ═══════════════════════════════════════════════════════════════════
#  HEARTBEAT + METRICS
# ═══════════════════════════════════════════════════════════════════

def print_heartbeat():
    """Print system status every 90 seconds."""
    now = time.time()
    if now - state["last_heartbeat_time"] < CONFIG["heartbeat_interval"]:
        return
    state["last_heartbeat_time"] = now

    mode_display = {
        "DORMANT": "🔴 DORMANT",
        "QUIET":   "🟡 QUIET",
        "ACTIVE":  "🟢 ACTIVE",
        "HOT":     "🔥 HOT",
    }.get(state["current_mode"], state["current_mode"])

    trade_info = "None"
    if state["active_trade"]:
        t = state["active_trade"]
        # Attempt to show current PnL estimate
        trade_info = (
            f"{t['symbol']} | Entry: ${t['entry_price']:.8f} | "
            f"Peak: +{t['max_runup_pct']:.1f}% | "
            f"Hold: {(now - t['entry_time']):.0f}s"
        )

    log_separator("─")
    log(f"HEARTBEAT", "HEART", force=True)
    log(f"  Mode      : {mode_display}", "HEART", force=True)
    log(f"  Daily PnL : {state['daily_pnl']:+.2f}%", "HEART", force=True)
    log(f"  Trades    : {state['trade_count_today']}/{CONFIG['max_trades_per_day']}", "HEART", force=True)
    log(f"  Con.Losses: {state['consecutive_losses']}", "HEART", force=True)
    log(f"  Active    : {trade_info}", "HEART", force=True)
    log_separator("─")


def print_metrics_summary():
    """Print metrics for last 10 trades."""
    history = list(state["trade_history"])
    if not history:
        return

    recent = history[-10:]
    wins   = [t for t in recent if t["pnl_pct"] > 0]
    losses = [t for t in recent if t["pnl_pct"] <= 0]

    win_rate = len(wins) / len(recent) * 100
    avg_win  = sum(t["pnl_pct"] for t in wins)  / len(wins)  if wins   else 0
    avg_loss = sum(t["pnl_pct"] for t in losses) / len(losses) if losses else 0

    # Exit type distribution
    exit_types = {}
    for t in recent:
        et = t["exit_type"] or "unknown"
        exit_types[et] = exit_types.get(et, 0) + 1

    log_separator("═")
    log("METRICS SUMMARY (Last 10 Trades)", "SYSTEM", force=True)
    log(f"  Win Rate   : {win_rate:.0f}% ({len(wins)}W / {len(losses)}L)", "SYSTEM", force=True)
    log(f"  Avg Win    : +{avg_win:.2f}%", "SYSTEM", force=True)
    log(f"  Avg Loss   : {avg_loss:.2f}%", "SYSTEM", force=True)
    if history:
        best  = max(recent, key=lambda t: t["pnl_pct"])
        worst = min(recent, key=lambda t: t["pnl_pct"])
        log(f"  Best Trade : {best['pnl_pct']:+.2f}% ({best['symbol']}, {best['exit_type']})", "SYSTEM", force=True)
        log(f"  Worst Trade: {worst['pnl_pct']:+.2f}% ({worst['symbol']}, {worst['exit_type']})", "SYSTEM", force=True)
    log("  Exit Distribution:", "SYSTEM", force=True)
    for etype, count in sorted(exit_types.items(), key=lambda x: -x[1]):
        pct = count / len(recent) * 100
        log(f"    {etype:<20} {count:2d} ({pct:.0f}%)", "SYSTEM", force=True)
    log_separator("═")


# ═══════════════════════════════════════════════════════════════════
#  MAIN SCAN LOOP
# ═══════════════════════════════════════════════════════════════════

def scan_and_trade():
    """
    One full scan cycle:
    1. Check market conditions
    2. Fetch pairs
    3. Filter, score, confirm
    4. Open trade if all gates pass
    """
    # Market mode check
    current_mode = check_market_conditions()
    if current_mode == "DORMANT":
        log("Market DORMANT. Skipping scan.", "DEBUG")
        return

    # Low volatility check
    dex_buf = state["dex_volume_buffer"]
    if len(dex_buf) >= 10:
        rolling_avg = sum(dex_buf) / len(dex_buf)
        if rolling_avg > 0 and dex_buf[-1] < rolling_avg * CONFIG["min_dex_volume_ratio"]:
            log("Global DEX volume too low. Skipping scan.", "DEBUG")
            return

    # Risk gates
    allowed, reason = can_trade()
    if not allowed:
        log(f"Trade blocked: {reason}", "DEBUG")
        return

    mode_threshold = CONFIG["score_threshold"][current_mode]

    # Fetch pairs
    pairs = fetch_pairs()
    if not pairs:
        log("No pairs fetched. Skipping cycle.", "WARN")
        return

    # Update DEX volume buffer with aggregate
    total_vol = sum(float(deep_get(p, "volume", "h24") or 0) for p in pairs)
    state["dex_volume_buffer"].append(total_vol)

    # Shuffle pairs slightly to avoid always processing same order
    random.shuffle(pairs)

    best_score    = 0
    best_candidate = None
    best_ohlcv    = None
    best_coin_type = None
    best_momentum = None

    for pair in pairs:
        # ── Data integrity ────────────────────────────────────────────
        valid, reason = validate_data(pair)
        if not valid:
            log(f"Skip: {reason}", "DEBUG")
            continue

        pair_address = deep_get(pair, "pairAddress") or ""
        base_symbol  = deep_get(pair, "baseToken", "symbol") or "?"

        # ── OHLCV ─────────────────────────────────────────────────────
        ohlcv = fetch_ohlcv(pair)
        if ohlcv is None:
            continue

        current_price = ohlcv["price_now"]

        # ── Blacklist check ───────────────────────────────────────────
        if is_blacklisted(pair_address, current_price):
            log(f"Skip {base_symbol}: blacklisted", "DEBUG")
            continue

        # ── Token validation ──────────────────────────────────────────
        token_valid, tv_reason = validate_token(pair, ohlcv)
        if not token_valid:
            log(f"Skip {base_symbol}: {tv_reason}", "DEBUG")
            continue

        # ── Classification ────────────────────────────────────────────
        coin_type = classify_token(pair)

        # ── Scoring ───────────────────────────────────────────────────
        score, breakdown = calculate_score(ohlcv, pair)

        if CONFIG["VERBOSE"]:
            log(f"{base_symbol} | Score: {score} | {coin_type}", "DEBUG")

        if score < mode_threshold:
            continue

        # ── Momentum classification ───────────────────────────────────
        momentum_tier = classify_momentum(score, mode_threshold, ohlcv)

        # ── Overextension check ───────────────────────────────────────
        ext_ok, ext_reason = check_overextension(ohlcv, momentum_tier)
        if not ext_ok:
            # Hard overextension → blacklist
            if ohlcv["pc_m5"] / 100 > CONFIG["overextension_hard"]:
                blacklist_token(pair_address, current_price, duration=900)
            log(f"Skip {base_symbol}: {ext_reason}", "DEBUG")
            continue

        # HOT mode extra safety
        if current_mode == "HOT":
            if ohlcv["buy_ratio_m5"] <= 0.5:
                log(f"HOT mode: skip {base_symbol} — buy ratio too low", "DEBUG")
                continue
            # No long wick proxy: buy ratio + positive momentum
            if ohlcv["pc_m5"] <= 0:
                continue

        # Track best candidate this cycle
        if score > best_score:
            best_score      = score
            best_candidate  = pair
            best_ohlcv      = ohlcv
            best_coin_type  = coin_type
            best_momentum   = momentum_tier

    # ── Process best candidate ────────────────────────────────────────
    if best_candidate is None:
        log("No qualifying tokens this cycle.", "DEBUG")
        return

    base_sym     = deep_get(best_candidate, "baseToken", "symbol") or "?"
    pair_address = deep_get(best_candidate, "pairAddress") or ""

    log(f"SIGNAL: {base_sym} | Score: {best_score} | "
        f"{best_coin_type} | {best_momentum} | Mode: {current_mode}",
        "SIGNAL", force=True)

    # ── Re-check can_trade (may have changed) ─────────────────────────
    allowed, reason = can_trade()
    if not allowed:
        log(f"Trade blocked at entry: {reason}", "DEBUG")
        return

    # ── Entry confirmation ────────────────────────────────────────────
    confirmed, fresh_score, conf_reason = confirm_entry(
        pair_address, best_score, mode_threshold
    )

    if not confirmed:
        log(f"Entry cancelled: {conf_reason}", "WARN")
        return

    # ── Final can_trade check ─────────────────────────────────────────
    allowed, reason = can_trade()
    if not allowed:
        log(f"Trade blocked after confirm: {reason}", "DEBUG")
        return

    # ── Open trade ────────────────────────────────────────────────────
    if CONFIG["DRY_RUN"]:
        log("DRY RUN mode — trade simulated (no real orders)", "SYSTEM")

    open_trade(best_candidate, best_ohlcv, best_coin_type,
               best_momentum, fresh_score)


# ═══════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════════════════

def main():
    """Main bot loop. Never crashes. Handles all exceptions."""
    log_separator("═")
    log("DEX SNIPER BOT — STARTING UP", "SYSTEM", force=True)
    log(f"Chain    : {CONFIG['chain'].upper()}", "SYSTEM", force=True)
    log(f"Dry Run  : {CONFIG['DRY_RUN']}", "SYSTEM", force=True)
    log(f"Verbose  : {CONFIG['VERBOSE']}", "SYSTEM", force=True)
    log_separator("═")

    # Load persisted state
    load_state()
    state["startup_time"] = time.time()
    state["last_loop_time"] = time.time()

    # Check daily reset on startup
    check_daily_reset()

    log(f"Warming up for {CONFIG['warmup_seconds']}s...", "SYSTEM", force=True)
    time.sleep(CONFIG["warmup_seconds"])
    log("Warm-up complete. Entering main loop.", "SYSTEM", force=True)

    last_scan_time     = 0
    last_metrics_time  = 0

    while CONFIG["RUNNING"]:
        try:
            now = time.time()

            # ── Watchdog: detect stalled loop ────────────────────────
            loop_gap = now - state["last_loop_time"]
            if loop_gap > 60:
                log(f"Watchdog: loop gap {loop_gap:.0f}s detected. Continuing.", "WARN")
            state["last_loop_time"] = now

            # ── Daily reset ──────────────────────────────────────────
            check_daily_reset()

            # ── Heartbeat ────────────────────────────────────────────
            print_heartbeat()

            # ── Metrics (every 10 trades or 30 min) ──────────────────
            if (now - last_metrics_time > 1800 or
                    len(state["trade_history"]) % 10 == 0 and
                    len(state["trade_history"]) > 0):
                if len(state["trade_history"]) > 0:
                    print_metrics_summary()
                last_metrics_time = now

            # ── Monitor active trade ─────────────────────────────────
            if state["active_trade"] is not None:
                monitor_trade()
                # Use shorter sleep interval during active trade
                interval = random.uniform(
                    CONFIG["monitor_interval_min"],
                    CONFIG["monitor_interval_max"]
                )
                time.sleep(interval)
                continue  # Skip scan while in trade

            # ── Scan cycle ───────────────────────────────────────────
            scan_interval = (CONFIG["scan_interval_base"] +
                             random.uniform(-CONFIG["scan_jitter"],
                                            CONFIG["scan_jitter"]))
            if now - last_scan_time >= scan_interval:
                last_scan_time = now
                scan_and_trade()

            # ── Clean blacklist periodically ─────────────────────────
            if now % 300 < 10:  # roughly every 5 min
                clean_blacklist()

            # ── Idle sleep ───────────────────────────────────────────
            time.sleep(random.uniform(2, 4))

        except KeyboardInterrupt:
            log("Manual stop detected (Ctrl+C).", "SYSTEM", force=True)
            CONFIG["RUNNING"] = False
            break

        except Exception as e:
            log(f"UNEXPECTED ERROR: {e}", "ERROR", force=True)
            log("Pausing 30s before resuming...", "ERROR", force=True)
            save_state()
            time.sleep(30)
            # Do NOT re-raise — never crash

    # ── Graceful shutdown ────────────────────────────────────────────
    log_separator("═")
    log("BOT SHUTTING DOWN", "SYSTEM", force=True)

    if state["active_trade"]:
        log("Active trade detected on shutdown — marking for safe close.", "WARN", force=True)
        save_state()

    print_metrics_summary()

    log(f"Final Daily PnL  : {state['daily_pnl']:+.2f}%", "SYSTEM", force=True)
    log(f"Total Trades     : {state['trade_count_today']}", "SYSTEM", force=True)
    log("State saved. Goodbye.", "SYSTEM", force=True)
    log_separator("═")


# ═══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
