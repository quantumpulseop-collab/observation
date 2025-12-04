#!/usr/bin/env python3
"""
Clean Spread Movement Tracker Bot
----------------------------------
Logic:
1. Full scan → shortlist symbols with ABS(spread) >= 0.2%
2. Log ALL scans (Binance prices, KuCoin prices, spread)
3. Monitor shortlisted symbols for 10 minutes
4. Count movements:
       Every time ABS(current_spread - previous_reference) >= 0.5%
       → movement_count += 1
       → reference = current_spread
5. End-of-window report with detailed stats
6. Loop repeats forever
"""

import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ===================== CONFIG ============================
SCAN_THRESHOLD = 0.2          # shortlist threshold (ABS spread)
MOVEMENT_STEP = 0.5           # count movement every ±0.5% change
MONITOR_DURATION = 600        # 10 minutes
POLL_INTERVAL = 2             # seconds
MAX_WORKERS = 20              # thread pool

# Telegram (optional)
TELEGRAM_TOKEN = "<YOUR TOKEN>"    # leave <> for disabled
TELEGRAM_CHAT_IDS = ["<CHAT ID>"]  # leave <> for disabled
# ==========================================================


# ===================== LOGGING ============================
logger = logging.getLogger("movement_bot")
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
logger.addHandler(console)

fileh = RotatingFileHandler("movement_bot.log", maxBytes=10_000_000, backupCount=5)
fileh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
fileh.setLevel(logging.DEBUG)
logger.addHandler(fileh)


# ===================== SMALL UTILS ============================
def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_telegram(msg):
    if TELEGRAM_TOKEN.startswith("<"):
        return
    for cid in TELEGRAM_CHAT_IDS:
        if cid.startswith("<"):
            continue
        try:
            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                params={"chat_id": cid, "text": msg},
                timeout=5
            )
        except:
            pass


def normalize(symbol):
    s = symbol.upper()
    if s.endswith("USDTM") or s.endswith("USDTP") or s.endswith("M"):
        return s[:-1]
    return s


# ===================== EXCHANGE FETCH ============================
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={}"

KUCOIN_INFO_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
KUCOIN_TICK_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={}"


def get_binance_symbols():
    try:
        r = requests.get(BINANCE_INFO_URL, timeout=10)
        data = r.json()
        return [
            s["symbol"] for s in data["symbols"]
            if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
        ]
    except:
        logger.exception("Failed to fetch binance symbols")
        return []


def get_kucoin_symbols():
    try:
        r = requests.get(KUCOIN_INFO_URL, timeout=10)
        data = r.json().get("data", [])
        return [s["symbol"] for s in data if s.get("status") == "open"]
    except:
        logger.exception("Failed to fetch kucoin symbols")
        return []


def get_common_symbols():
    bin_syms = get_binance_symbols()
    ku_syms = get_kucoin_symbols()

    bin_set = {normalize(s) for s in bin_syms}
    ku_set = {normalize(s) for s in ku_syms}

    common = bin_set.intersection(ku_set)

    # map normalized → kucoin raw
    ku_map = {}
    for s in ku_syms:
        ku_map[normalize(s)] = s

    return common, ku_map


def get_binance_book():
    try:
        r = requests.get(BINANCE_BOOK_URL, timeout=6)
        data = r.json()
        out = {}
        for d in data:
            try:
                out[d["symbol"]] = {
                    "bid": float(d["bidPrice"]),
                    "ask": float(d["askPrice"])
                }
            except:
                pass
        return out
    except:
        logger.exception("Failed binance book")
        return {}


def get_binance_price(symbol, session):
    try:
        r = session.get(BINANCE_TICK_URL.format(symbol), timeout=6)
        d = r.json()
        return float(d["bidPrice"]), float(d["askPrice"])
    except:
        return None, None


def get_kucoin_price(symbol, session):
    try:
        r = session.get(KUCOIN_TICK_URL.format(symbol), timeout=6)
        d = r.json().get("data", {})
        bid = float(d.get("bestBidPrice") or d.get("bid") or 0)
        ask = float(d.get("bestAskPrice") or d.get("ask") or 0)
        if bid > 0 and ask > 0:
            return bid, ask
    except:
        pass
    return None, None


def batch_kucoin(symbols):
    out = {}
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(symbols))) as ex:
            futs = {ex.submit(get_kucoin_price, s, session): s for s in symbols}
            for f in as_completed(futs):
                sym = futs[f]
                try:
                    b, a = f.result()
                    if b and a:
                        out[sym] = {"bid": b, "ask": a}
                except:
                    pass
    return out


# ===================== SPREAD LOGIC ============================
def calc_spread(bb, ba, kb, ka):
    try:
        pos = ((kb - ba) / ba) * 100
        neg = ((ka - bb) / bb) * 100
        if pos > 0:
            return pos
        if neg < 0:
            return neg
        return None
    except:
        return None


# ===================== MAIN BOT ============================
def main():
    logger.info("=== Movement Bot Started (threshold=0.2%, step=0.5%) ===")

    session = requests.Session()

    while True:
        try:
            logger.info("========== FULL SCAN START ==========")

            common, ku_map = get_common_symbols()
            bin_book = get_binance_book()

            ku_syms = [ku_map.get(s, s+"M") for s in common]
            ku_prices = batch_kucoin(ku_syms)

            candidates = {}

            # ---- Full scan logging ----
            for sym in common:
                if sym not in bin_book:
                    continue
                b = bin_book[sym]
                ku_sym = ku_map.get(sym, sym + "M")
                k = ku_prices.get(ku_sym)

                if not k:
                    continue

                spread = calc_spread(b["bid"], b["ask"], k["bid"], k["ask"])

                logger.info(f"[SCAN] {sym} | BIN({b['bid']}/{b['ask']}) "
                            f"KU({k['bid']}/{k['ask']}) → {spread:+.4f}%")

                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    candidates[sym] = {
                        "ku": ku_sym,
                        "reference": spread,     # dynamic reference point
                        "movements": 0,
                        "max": spread,
                        "min": spread,
                        "samples": 0,
                        "movement_times": []
                    }

            logger.info(f"Shortlisted {len(candidates)} symbols: {list(candidates.keys())}")

            if not candidates:
                time.sleep(3)
                continue

            logger.info("===== MONITORING 10 MIN START =====")

            end_time = time.time() + MONITOR_DURATION

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as EX:
                while time.time() < end_time:
                    start = time.time()

                    futures = {}

                    # submit tasks
                    for sym, info in candidates.items():
                        futures[EX.submit(get_binance_price, sym, session)] = ("bin", sym)
                        futures[EX.submit(get_kucoin_price, info["ku"], session)] = ("ku", sym)

                    # collect
                    latest = {s: {"bin": None, "ku": None} for s in candidates}
                    for f in as_completed(futures):
                        typ, sym = futures[f]
                        try:
                            b, a = f.result()
                            if b and a:
                                latest[sym][typ] = {"bid": b, "ask": a}
                        except:
                            pass

                    # process
                    for sym, info in candidates.items():
                        B = latest[sym]["bin"]
                        K = latest[sym]["ku"]
                        if not B or not K:
                            continue

                        spread = calc_spread(B["bid"], B["ask"], K["bid"], K["ask"])
                        if spread is None:
                            continue

                        info["samples"] += 1
                        info["max"] = max(info["max"], spread)
                        info["min"] = min(info["min"], spread)

                        # ===== MOVEMENT LOGIC =====
                        if abs(spread - info["reference"]) >= MOVEMENT_STEP:
                            info["movements"] += 1
                            info["movement_times"].append(f"{ts()} ({spread:+.4f}%)")
                            info["reference"] = spread  # reset reference

                    # wait for next poll
                    elapsed = time.time() - start
                    if elapsed < POLL_INTERVAL:
                        time.sleep(POLL_INTERVAL - elapsed)

            # ===== REPORT =====
            logger.info("===== END WINDOW | REPORT =====")

            report_lines = []
            report_lines.append(f"10-min Movement Report ({ts()})")

            for sym, info in candidates.items():
                line = (
                    f"{sym} | movements={info['movements']} | "
                    f"min={info['min']:+.4f}% | max={info['max']:+.4f}% | "
                    f"samples={info['samples']}"
                )
                report_lines.append(line)

                if info["movement_times"]:
                    report_lines.append("   moves: " + ", ".join(info["movement_times"][-8:]))

            final_report = "\n".join(report_lines)
            logger.info("\n" + final_report)
            send_telegram(final_report)

        except Exception:
            logger.exception("Main loop error")
            time.sleep(5)


if __name__ == "__main__":
    main()
