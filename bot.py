#!/usr/bin/env python3
"""
SpreadOscillator Bot
- Full scan each loop, shortlist symbols with abs(spread) >= 0.2%
- Monitor shortlisted symbols continuously for 10 minutes (600s)
- Count number of times each symbol crosses ±0.5% (rising edge counts)
- After 10 minutes produce a report with oscillation counts, min/max spread, timestamps
- Loop continues
"""
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import traceback

# ============================= CONFIG =============================
# Fill these with your real values if you want Telegram messages.
TELEGRAM_TOKEN = "<PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE>"
TELEGRAM_CHAT_IDS = ["<CHAT_ID_1>", "<CHAT_ID_2>"]

# Scan & monitoring thresholds (user-specified)
SCAN_THRESHOLD = 0.2        # shortlist if abs(spread) >= 0.2%
OSCILLATION_THRESHOLD = 0.5 # count a crossing when abs(spread) >= 0.5%
MONITOR_DURATION = 600      # seconds (10 minutes)
MONITOR_POLL = 2            # seconds between polls while monitoring
MAX_WORKERS = 20

# Optional instant-alert settings (left conservative by default)
ALERT_THRESHOLD = 5.0       # instant alert threshold in % (kept high)
ALERT_COOLDOWN = 60         # seconds between instant alerts for same symbol
CONFIRM_RETRY_DELAY = 0.5
CONFIRM_RETRIES = 2
# ==================================================================

# API endpoints (futures/perpetual endpoints used in original file)
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
KUCOIN_ACTIVE_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
KUCOIN_TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"

# -------------------- Logging setup --------------------
logger = logging.getLogger("spread_oscillator")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

fh = RotatingFileHandler("spread_oscillator.log", maxBytes=8_000_000, backupCount=5)
fh.setLevel(logging.DEBUG)
fh_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -------------------- Telegram helper --------------------
def send_telegram(message):
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN.startswith("<"):
        logger.debug("Telegram token not set; skipping Telegram send.")
        return
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            resp = requests.get(url, params={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
            if resp.status_code != 200:
                logger.warning("Telegram non-200 response: %s %s", resp.status_code, resp.text[:200])
        except Exception:
            logger.exception("Failed to send Telegram message")

# -------------------- Utility / fetch functions --------------------
def normalize(sym):
    """Normalize symbol names to a comparable form."""
    if not sym:
        return sym
    s = sym.upper()
    if s.endswith("USDTM"):
        return s[:-1]
    if s.endswith("USDTP"):
        return s[:-1]
    if s.endswith("M"):
        return s[:-1]
    return s

def get_binance_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(BINANCE_INFO_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            syms = [s["symbol"] for s in data.get("symbols", [])
                    if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
            logger.debug("[BINANCE] fetched %d symbols", len(syms))
            return syms
        except Exception as e:
            logger.warning("[BINANCE] attempt %d error: %s", attempt, str(e))
            if attempt == retries:
                logger.exception("[BINANCE] final failure fetching symbols")
                return []
            time.sleep(0.7)

def get_kucoin_symbols(retries=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(KUCOIN_ACTIVE_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            raw = data.get("data", []) if isinstance(data, dict) else []
            syms = [s["symbol"] for s in raw if s.get("status", "").lower() == "open"]
            logger.debug("[KUCOIN] fetched %d symbols", len(syms))
            return syms
        except Exception as e:
            logger.warning("[KUCOIN] attempt %d error: %s", attempt, str(e))
            if attempt == retries:
                logger.exception("[KUCOIN] final failure fetching symbols")
                return []
            time.sleep(0.7)

def get_common_symbols():
    bin_syms = get_binance_symbols()
    ku_syms = get_kucoin_symbols()
    bin_set = {normalize(s) for s in bin_syms}
    ku_set = {normalize(s) for s in ku_syms}
    common = bin_set.intersection(ku_set)
    ku_map = {}
    for s in ku_syms:
        n = normalize(s)
        if n not in ku_map:
            ku_map[n] = s
    logger.info("Common symbols: %d", len(common))
    return common, ku_map

def get_binance_book(retries=1):
    for attempt in range(1, retries+1):
        try:
            r = requests.get(BINANCE_BOOK_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            out = {}
            for d in data:
                try:
                    out[d["symbol"]] = {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"])}
                except Exception:
                    continue
            logger.debug("[BINANCE_BOOK] entries: %d", len(out))
            return out
        except Exception:
            logger.exception("[BINANCE_BOOK] fetch error")
            if attempt == retries:
                return {}
            time.sleep(0.5)

def get_binance_price(symbol, session, retries=1):
    for attempt in range(1, retries+1):
        try:
            url = BINANCE_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                logger.debug("Binance ticker non-200 %s for %s: %s", r.status_code, symbol, r.text[:200])
                return None, None
            d = r.json()
            bid = float(d.get("bidPrice") or 0)
            ask = float(d.get("askPrice") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
            logger.debug("Binance price fetch failed for %s (attempt %d)", symbol, attempt)
            if attempt == retries:
                logger.exception("Binance price final failure for %s", symbol)
                return None, None
            time.sleep(0.2)

def get_kucoin_price_once(symbol, session, retries=1):
    for attempt in range(1, retries+1):
        try:
            url = KUCOIN_TICKER_URL.format(symbol=symbol)
            r = session.get(url, timeout=6)
            if r.status_code != 200:
                logger.debug("KuCoin ticker non-200 %s for %s: %s", r.status_code, symbol, r.text[:200])
                return None, None
            data = r.json()
            d = data.get("data", {}) if isinstance(data, dict) else {}
            bid = float(d.get("bestBidPrice") or d.get("bid") or 0)
            ask = float(d.get("bestAskPrice") or d.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                return None, None
            return bid, ask
        except Exception:
            logger.debug("KuCoin price fetch failed for %s (attempt %d)", symbol, attempt)
            if attempt == retries:
                logger.exception("KuCoin price final failure for %s", symbol)
                return None, None
            time.sleep(0.2)

def threaded_kucoin_prices(symbols):
    prices = {}
    if not symbols:
        return prices
    workers = min(MAX_WORKERS, max(4, len(symbols)))
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(get_kucoin_price_once, s, session): s for s in symbols}
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    bid, ask = fut.result()
                    if bid and ask:
                        prices[s] = {"bid": bid, "ask": ask}
                except Exception:
                    logger.exception("threaded_kucoin_prices: future error for %s", s)
    logger.debug("[KUCOIN_BATCH] fetched %d/%d", len(prices), len(symbols))
    return prices

# -------------------- Spread calculation --------------------
def calculate_spread(bin_bid, bin_ask, ku_bid, ku_ask):
    """
    Return a single signed spread percentage:
    Positive => KuCoin bid > Binance ask  (profit by long Binance / short KuCoin)
    Negative => KuCoin ask < Binance bid  (other side)
    """
    try:
        if not all([bin_bid, bin_ask, ku_bid, ku_ask]) or bin_ask <= 0 or bin_bid <= 0:
            return None
        pos = ((ku_bid - bin_ask) / bin_ask) * 100
        neg = ((ku_ask - bin_bid) / bin_bid) * 100
        # prefer whichever side is significant
        if pos > 0.0001:
            return pos
        if neg < -0.0001:
            return neg
        return None
    except Exception:
        logger.exception("calculate_spread error")
        return None

# -------------------- Main loop --------------------
def main():
    logger.info("SpreadOscillator STARTED - %s", timestamp())
    send_telegram("SpreadOscillator bot started. Scanning for ±0.2% spreads, monitoring shortlisted coins for 10 minutes.")

    last_alert_time = {}
    http_session = requests.Session()

    while True:
        loop_start = time.time()
        try:
            # 1) Full scan
            common_symbols, ku_map = get_common_symbols()
            if not common_symbols:
                logger.warning("No common symbols found; sleeping briefly before retry.")
                time.sleep(5)
                continue

            bin_book = get_binance_book()
            ku_symbols = [ku_map.get(sym, sym + "M") for sym in common_symbols]
            ku_prices = threaded_kucoin_prices(ku_symbols)

            # Prepare shortlist of candidates
            candidates = {}
            for sym in common_symbols:
                bin_tick = bin_book.get(sym)
                ku_sym = ku_map.get(sym, sym + "M")
                ku_tick = ku_prices.get(ku_sym)
                if not bin_tick or not ku_tick:
                    continue
                spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], ku_tick["bid"], ku_tick["ask"])
                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    # initialize monitoring record for this symbol
                    candidates[sym] = {
                        "ku_sym": ku_sym,
                        "start_spread": spread,
                        "max_spread": spread,
                        "min_spread": spread,
                        # oscillation detection state
                        "osc_count": 0,
                        "osc_timestamps": [],   # when a crossing occurred
                        "last_was_outside": abs(spread) >= OSCILLATION_THRESHOLD,
                        "samples": 0
                    }

            logger.info("[%s] Full scan shortlisted %d symbol(s): %s",
                        timestamp(), len(candidates), list(candidates.keys())[:20])

            if not candidates:
                # nothing to monitor, short sleep then loop again
                time.sleep(3)
                continue

            # 2) Focused monitoring for MONITOR_DURATION seconds
            monitor_end = time.time() + MONITOR_DURATION
            # Use a session for repeated calls
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(4, len(candidates)*2))) as executor:
                # We'll poll repeatedly until monitor_end
                while time.time() < monitor_end and candidates:
                    round_start = time.time()
                    # prepare futures: fetch binance and kucoin for each candidate
                    fut_map = {}
                    for sym, info in list(candidates.items()):
                        ku_sym = info["ku_sym"]
                        # assume Binance symbol is normalized sym (this matches earlier logic)
                        b_symbol = sym
                        fut_map[executor.submit(get_binance_price, b_symbol, http_session)] = ("bin", sym)
                        fut_map[executor.submit(get_kucoin_price_once, ku_sym, http_session)] = ("ku", sym)

                    # collect latest values
                    latest = {s: {"bin": None, "ku": None} for s in candidates.keys()}
                    for fut in as_completed(fut_map):
                        typ, sym = fut_map[fut]
                        try:
                            bid, ask = fut.result()
                        except Exception:
                            bid, ask = None, None
                        if bid and ask and sym in latest:
                            latest[sym][typ] = {"bid": bid, "ask": ask}

                    # evaluate spreads and update candidate info
                    now_ts = timestamp()
                    for sym in list(candidates.keys()):
                        info = candidates.get(sym)
                        if not info:
                            continue
                        b = latest[sym].get("bin")
                        k = latest[sym].get("ku")
                        if not b or not k:
                            continue
                        spread = calculate_spread(b["bid"], b["ask"], k["bid"], k["ask"])
                        if spread is None:
                            continue

                        # update extremes and sample count
                        info["samples"] += 1
                        if spread > info["max_spread"]:
                            info["max_spread"] = spread
                        if spread < info["min_spread"]:
                            info["min_spread"] = spread

                        # oscillation detection: count rising edge from inside -> outside ±OSCILLATION_THRESHOLD
                        currently_outside = abs(spread) >= OSCILLATION_THRESHOLD
                        if currently_outside and not info["last_was_outside"]:
                            # we transitioned from inside -> outside: count it
                            info["osc_count"] += 1
                            info["osc_timestamps"].append(f"{now_ts} ({spread:+.4f}%)")
                            logger.debug("Oscillation detected for %s: %+.4f%% (count=%d)", sym, spread, info["osc_count"])
                        info["last_was_outside"] = currently_outside

                        # OPTIONAL: instant alert logic (kept but won't remove candidate so monitoring continues)
                        if abs(spread) >= ALERT_THRESHOLD:
                            now = time.time()
                            cooldown_ok = (sym not in last_alert_time) or (now - last_alert_time[sym] > ALERT_COOLDOWN)
                            if cooldown_ok:
                                # quick confirm re-checks
                                confirmed = False
                                b_conf = k_conf = None
                                for attempt in range(CONFIRM_RETRIES):
                                    time.sleep(CONFIRM_RETRY_DELAY)
                                    b2_bid, b2_ask = get_binance_price(sym, http_session, retries=1)
                                    k2_bid, k2_ask = get_kucoin_price_once(info["ku_sym"], http_session, retries=1)
                                    if b2_bid and b2_ask and k2_bid and k2_ask:
                                        spread2 = calculate_spread(b2_bid, b2_ask, k2_bid, k2_ask)
                                        if spread2 is not None and abs(spread2) >= ALERT_THRESHOLD:
                                            confirmed = True
                                            b_conf = {"bid": b2_bid, "ask": b2_ask}
                                            k_conf = {"bid": k2_bid, "ask": k2_ask}
                                            break
                                if confirmed:
                                    direction = "Long Binance / Short KuCoin" if spread2 > 0 else "Long KuCoin / Short Binance"
                                    msg = (
                                        f"*BIG SPREAD ALERT*\n"
                                        f"`{sym}` → *{spread2:+.4f}%*\n"
                                        f"Direction → {direction}\n"
                                        f"Binance: `{b_conf['bid']:.6f}` ↔ `{b_conf['ask']:.6f}`\n"
                                        f"KuCoin : `{k_conf['bid']:.6f}` ↔ `{k_conf['ask']:.6f}`\n"
                                        f"{timestamp()}"
                                    )
                                    send_telegram(msg)
                                    logger.info("ALERT → %s %+.4f%% (confirmed)", sym, spread2)
                                    last_alert_time[sym] = time.time()

                    # respect poll interval
                    elapsed = time.time() - round_start
                    sleep_for = MONITOR_POLL - elapsed
                    if sleep_for > 0:
                        # but do not sleep beyond monitor_end
                        if time.time() + sleep_for > monitor_end:
                            sleep_for = max(0, monitor_end - time.time())
                        if sleep_for > 0:
                            time.sleep(sleep_for)

            # 3) After monitoring window: generate and send report
            report_lines = []
            report_lines.append(f"*10-min Oscillation Report* — {timestamp()}")
            report_lines.append(f"Shortlisted symbols: {len(candidates)} | Monitoring duration: {MONITOR_DURATION}s")
            # Show one line per candidate with stats
            for sym, info in candidates.items():
                line = (
                    f"`{sym}` | start {info['start_spread']:+.4f}% | "
                    f"min {info['min_spread']:+.4f}% | max {info['max_spread']:+.4f}% | "
                    f"±{OSCILLATION_THRESHOLD}% crossings: {info['osc_count']} | samples: {info.get('samples',0)}"
                )
                report_lines.append(line)
                # optionally include timestamps of crossings if any (comma-separated)
                if info["osc_timestamps"]:
                    report_lines.append("  crossings: " + ", ".join(info["osc_timestamps"][-8:]))  # last up to 8 timestamps

            report_text = "\n".join(report_lines)
            # send to Telegram and log file
            logger.info("\n" + "\n".join(report_lines))
            send_telegram(report_text)

            # align loop speed: small pause before next full scan
            elapsed_loop = time.time() - loop_start
            if elapsed_loop < 1:
                time.sleep(1)

        except Exception:
            logger.exception("Unhandled exception in main loop - sleeping briefly")
            time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down.")
    except Exception:
        logger.exception("Unhandled exception at top level")
