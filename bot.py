#!/usr/bin/env python3
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ============================= CONFIG =============================
TELEGRAM_TOKEN = "8589870096:AAHahTpg6LNXbUwUMdt3q2EqVa2McIo14h8"
TELEGRAM_CHAT_IDS = ["5054484162", "497819952"]

SCAN_THRESHOLD = 0.20        # Initial filter: >= ±0.20% spread
MOVEMENT_STEP = 0.50         # Count +1 every time spread moves this much from last counted point
MONITOR_DURATION = 600       # 10 minutes
MONITOR_POLL = 2             # Poll every 2 seconds
MAX_WORKERS = 12
# ==================================================================

# API endpoints
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_BOOK_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
BINANCE_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
KUCOIN_ACTIVE_URL = "https://api-futures.kucoin.com/api/v1/contracts/active"
KUCOIN_TICKER_URL = "https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}"

# -------------------- Logging setup --------------------
logger = logging.getLogger("spread_tracker")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
ch.setFormatter(ch_formatter)
logger.addHandler(ch)

fh = RotatingFileHandler("spread_tracker.log", maxBytes=10_000_000, backupCount=5)
fh.setLevel(logging.DEBUG)
fh_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -------------------- Telegram --------------------
def send_telegram(msg):
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                params={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown", "disable_web_page_preview": True},
                timeout=10
            )
        except:
            pass

# -------------------- Symbol helpers --------------------
def normalize(s):
    if not s: return s
    s = s.upper()
    if s.endswith(("USDTM", "USDTP", "M")):
        return s.rsplit("M", 1)[0].rsplit("P", 1)[0]
    return s

def get_common_symbols():
    def fetch_binance():
        try:
            r = requests.get(BINANCE_INFO_URL, timeout=10).json()
            return [s["symbol"] for s in r["symbols"] if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"]
        except: return []

    def fetch_kucoin():
        try:
            r = requests.get(KUCOIN_ACTIVE_URL, timeout=10).json()
            return [c["symbol"] for c in r.get("data", []) if c.get("status", "").lower() == "open"]
        except: return []

    bin_syms = fetch_binance()
    ku_syms = fetch_kucoin()
    bin_set = {normalize(s) for s in bin_syms}
    ku_set = {normalize(s) for s in ku_syms}
    common = bin_set.intersection(ku_set)

    ku_map = {}
    for s in ku_syms:
        n = normalize(s)
        if n not in ku_map:
            ku_map[n] = s

    logger.info(f"Common perpetual symbols: {len(common)}")
    return common, ku_map

# -------------------- Price fetchers --------------------
def get_binance_book():
    try:
        data = requests.get(BINANCE_BOOK_URL, timeout=10).json()
        book = {}
        for item in data:
            if item["bidPrice"] and item["askPrice"]:
                book[item["symbol"]] = {"bid": float(item["bidPrice"]), "ask": float(item["askPrice"])}
        return book
    except:
        return {}

def get_price_binance(symbol, session):
    try:
        r = session.get(BINANCE_TICKER_URL.format(symbol=symbol), timeout=6)
        if r.status_code == 200:
            d = r.json()
            bid = float(d.get("bidPrice") or 0)
            ask = float(d.get("askPrice") or 0)
            if bid > 0 and ask > 0:
                return bid, ask
    except: pass
    return None, None

def get_price_kucoin(symbol, session):
    try:
        r = session.get(KUCOIN_TICKER_URL.format(symbol=symbol), timeout=6)
        if r.status_code == 200:
            d = r.json().get("data", {})
            bid = float(d.get("bestBidPrice") or d.get("bid") or 0)
            ask = float(d.get("bestAskPrice") or d.get("ask") or 0)
            if bid > 0 and ask > 0:
                return bid, ask
    except: pass
    return None, None

# -------------------- Spread calculation --------------------
def calculate_spread(bin_bid, bin_ask, ku_bid, ku_ask):
    if not all(v > 0 for v in [bin_bid, bin_ask, ku_bid, ku_ask]):
        return None
    pos = (ku_bid - bin_ask) / bin_ask * 100
    neg = (ku_ask - bin_bid) / bin_bid * 100
    if pos > 0.01:
        return round(pos, 5)
    if neg < -0.01:
        return round(neg, 5)
    return None

# -------------------- Main Loop --------------------
def main():
    logger.info("Spread Movement Tracker STARTED")
    send_telegram("Spread Movement Tracker Started\nFull scan → 10 min tracking → report → repeat")

    session = requests.Session()

    while True:
        start_time = time.time()

        try:
            # === 1. Full Scan & Candidate Selection ===
            common, ku_map = get_common_symbols()
            if not common:
                time.sleep(10)
                continue

            bin_book = get_binance_book()
            candidates = {}

            for norm_sym in common:
                bin_sym = norm_sym
                ku_sym = ku_map.get(norm_sym, norm_sym + "M")

                bin_tick = bin_book.get(bin_sym)
                if not bin_tick:
                    continue

                ku_bid, ku_ask = get_price_kucoin(ku_sym, session)
                if not ku_bid or not ku_ask:
                    continue

                spread = calculate_spread(bin_tick["bid"], bin_tick["ask"], ku_bid, ku_ask)
                if spread is not None and abs(spread) >= SCAN_THRESHOLD:
                    candidates[norm_sym] = {
                        "bin_sym": bin_sym,
                        "ku_sym": ku_sym,
                        "last_spread": spread,
                        "reference_point": spread,   # This is the point we count moves FROM
                        "move_count": 0,
                        "history": [(time.time(), spread)]
                    }

            logger.info(f"SCAN COMPLETE → {len(candidates)} candidates selected for 10-min tracking")
            if not candidates:
                logger.info("No candidates found. Sleeping 60s...")
                send_telegram("No coins with ≥0.2% spread found.")
                time.sleep(60)
                continue

            send_telegram(f"Tracking {len(candidates)} coins for 10 minutes...\n" + ", ".join(list(candidates.keys())[:15]))

            # === 2. 10-Minute Continuous Tracking ===
            end_time = start_time + MONITOR_DURATION
            poll_count = 0

            while time.time() < end_time:
                poll_start = time.time()
                poll_count += 1

                # Fetch all current prices in parallel
                latest = {}
                with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(candidates)*2)) as ex:
                    futures = {}
                    for norm_sym, info in candidates.items():
                        futures[ex.submit(get_price_binance, info["bin_sym"], session)] = ("bin", norm_sym)
                        futures[ex.submit(get_price_kucoin, info["ku_sym"], session)] = ("ku", norm_sym)

                    for f in as_completed(futures):
                        typ, sym = futures[f]
                        try:
                            bid, ask = f.result()
                            if bid and ask:
                                latest.setdefault(sym, {})[typ] = {"bid": bid, "ask": ask}
                        except:
                            pass

                # Process each candidate
                for norm_sym, info in list(candidates.items()):
                    data = latest.get(norm_sym, {})
                    bin_p = data.get("bin")
                    ku_p = data.get("ku")
                    if not bin_p or not ku_p:
                        continue

                    spread = calculate_spread(bin_p["bid"], bin_p["ask"], ku_p["bid"], ku_p["ask"])
                    if spread is None:
                        continue

                    old_ref = info["reference_point"]
                    delta = spread - old_ref

                    if abs(delta) >= MOVEMENT_STEP:
                        steps = int(abs(delta) // MOVEMENT_STEP)
                        direction = "UP" if delta > 0 else "DOWN"
                        info["move_count"] += steps
                        info["reference_point"] = old_ref + (steps * MOVEMENT_STEP * (1 if delta > 0 else -1))
                        info["last_spread"] = spread
                        info["history"].append((time.time(), spread))

                        logger.info(f"MOVE | {norm_sym:8} | {old_ref:+.4f}% → {spread:+.4f}% | "
                                    f"Δ={delta:+.4f}% | +{steps} move(s) | Total: {info['move_count']}")

                # Sleep to maintain poll interval
                elapsed = time.time() - poll_start
                if elapsed < MONITOR_POLL:
                    time.sleep(MONITOR_POLL - elapsed)

            # === 3. Final Report ===
            if candidates:
                sorted_candidates = sorted(candidates.items(), key=lambda x: x[1]["move_count"], reverse=True)
                report = f"*10-Minute Movement Report* — {ts()}\n\n"
                report += f"Tracked: {len(candidates)} coins | Polls: ~{poll_count}\n"
                report += "Ranked by number of 0.5%+ moves:\n\n"

                rank = 1
                for sym, info in sorted_candidates:
                    if info["move_count"] > 0:
                        first = info["history"][0][1]
                        last = info["history"][-1][1]
                        max_sp = max(h[1] for h in info["history"])
                        min_sp = min(h[1] for h in info["history"])
                        report += (f"{rank}. `{sym}` → *{info['move_count']} moves* | "
                                   f"Start: {first:+.3f}% → End: {last:+.3f}% | "
                                   f"Range: {min_sp:+.3f} to {max_sp:+.3f}%\n")
                        rank += 1

                if rank == 1:
                    report += "No coin moved ≥0.5% during the window.\n"

                logger.info("10-MIN REPORT:\n" + report)
                send_telegram(report)
            else:
                send_telegram("No candidates survived the 10-minute window.")

        except Exception as e:
            logger.exception("Error in main loop")
            send_telegram(f"Bot error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped by user")
        send_telegram("Spread tracker stopped.")
