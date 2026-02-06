import json
import os
import random
import time
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

import cloudscraper

# =====================================================
# PATH SAFETY (FIX)
# =====================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 7
API_TIMEOUT = 12
HARD_TIMEOUT = 15
CUTOFF_MINUTES = 200

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = datetime.now(IST).strftime("%Y%m%d")

# OUTPUT → ONLY DATA REPO
BASE_DIR = os.path.join("daily","data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SUMMARY_FILE  = os.path.join(BASE_DIR, f"movie_summary{SHARD_ID}.json")
DETAILED_FILE = os.path.join(BASE_DIR, f"detailed{SHARD_ID}.json")
LOG_FILE      = os.path.join(LOG_DIR, f"bms{SHARD_ID}.log")

# =====================================================
# LOGGING
# =====================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================================================
# HELPERS
# =====================================================
def calc_occupancy(sold, total):
    return round((sold / total) * 100, 2) if total else 0.0

# =====================================================
# HARD TIMEOUT
# =====================================================
class TimeoutError(Exception):
    pass

def hard_timeout(seconds):
    def decorator(func):
        def wrapper(*args, **kwargs):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except FutureTimeout:
                    raise TimeoutError("Hard timeout hit")
        return wrapper
    return decorator

# =====================================================
# USER AGENTS
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

thread_local = threading.local()

class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
            "X-Forwarded-For": self.ip,
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
        log("🧠 New identity created")
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    log("🔄 Identity reset")

# =====================================================
# FETCH API
# =====================================================
@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code):
    ident = get_identity()
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )

    r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
    if not r.text.strip().startswith("{"):
        raise RuntimeError("Blocked / HTML")
    return r.json()

# =====================================================
# TIME FILTER
# =====================================================
def minutes_left(show_time):
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time, "%I:%M %p")
        t = t.replace(year=now.year, month=now.month, day=now.day, tzinfo=IST)
        return (t - now).total_seconds() / 60
    except Exception:
        return 9999

# =====================================================
# PARSER
# =====================================================
def parse_payload(data):
    out = []
    sd = data.get("ShowDetails", [])
    if not sd:
        return out

    venue = sd[0].get("Venues", {})
    venue_name = venue.get("VenueName", "")
    venue_add  = venue.get("VenueAdd", "")
    chain      = venue.get("VenueCompName", "Unknown")

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "UNKNOWN")
            lang = ch.get("EventLanguage", "UNKNOWN")

            for sh in ch.get("ShowTimes", []):
                if sh.get("ShowDateCode") != DATE_CODE:
                    continue

                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold  += seats - free
                    gross += (seats - free) * price

                out.append({
                    "movie": title,
                    "venue": venue_name,
                    "address": venue_add,
                    "language": lang,
                    "dimension": dim,
                    "chain": chain,
                    "time": sh.get("ShowTime", ""),
                    "audi": sh.get("Attributes", ""),
                    "session_id": str(sh.get("SessionId", "")),
                    "totalSeats": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })
    return out

# =====================================================
# SHOW KEY
# =====================================================
def show_key(r):
    return (r["venue"], r["time"], r["session_id"], r["audi"])

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 SCRIPT STARTED")

    venues_path = os.path.join(SCRIPT_DIR, f"venues{SHARD_ID}.json")
    with open(venues_path, "r", encoding="utf-8-sig") as f:
        venues = json.load(f)

    all_rows = []
    failed_venues = []

    # ---------------- FIRST PASS ----------------
    for i, vcode in enumerate(venues, 1):
        log(f"[{i}/{len(venues)}] {vcode}")
        try:
            raw = fetch_api_raw(vcode)
            for r in parse_payload(raw):
                mins = minutes_left(r["time"])
                if mins <= CUTOFF_MINUTES:
                    r.update({
                        "minutes_left": round(mins, 1),
                        "city": venues[vcode].get("City", "Unknown"),
                        "state": venues[vcode].get("State", "Unknown"),
                        "date": DATE_CODE,
                        "source": "BMS",
                    })
                    all_rows.append(r)
        except Exception as e:
            failed_venues.append(vcode)
            reset_identity()
            log(f"❌ FAILED | {vcode} | {e}")

        time.sleep(random.uniform(0.4, 1))

    # ---------------- ONE RETRY ONLY ----------------
    if failed_venues:
        log(f"🔁 RETRYING FAILED VENUES: {len(failed_venues)}")

    retry_round = 1
    pending = failed_venues[:]   # copy
    MAX_RETRIES = 5

    while pending and retry_round <= MAX_RETRIES:
        log(f"🔁 RETRY ROUND {retry_round} | Pending venues: {len(pending)}")

        next_pending = []

        for vcode in pending:
            log(f"🔁 Retry | {vcode}")
            try:
                raw = fetch_api_raw(vcode)
                for r in parse_payload(raw):
                    mins = minutes_left(r["time"])
                    if mins <= CUTOFF_MINUTES:
                        r.update({
                            "minutes_left": round(mins, 1),
                            "city": venues[vcode].get("City", "Unknown"),
                            "state": venues[vcode].get("State", "Unknown"),
                            "date": DATE_CODE,
                            "source": "BMS",
                        })
                        all_rows.append(r)
            except Exception as e:
                next_pending.append(vcode)
                reset_identity()
                log(f"❌ FAILED AGAIN | {vcode} | {e}")

            time.sleep(random.uniform(0.4, 1))

        pending = next_pending
        retry_round += 1

    if pending:
        log(f"⚠️ PERMANENT FAILURES AFTER {MAX_RETRIES} RETRIES: {len(pending)}")
    else:
        log("✅ ALL FAILED VENUES RECOVERED")


    # =====================================================
    # MERGE OLD DATA
    # =====================================================
    old_rows = []
    if os.path.exists(DETAILED_FILE):
        with open(DETAILED_FILE, "r", encoding="utf-8") as f:
            old_rows = json.load(f)

    old_map = {show_key(r): r for r in old_rows}
    new_map = {}

    for r in all_rows:
        key = show_key(r)
        if key in old_map:
            old_map[key].update(r)
            new_map[key] = old_map[key]
        else:
            new_map[key] = r

    for k, v in old_map.items():
        if k not in new_map:
            new_map[k] = v

    detailed = list(new_map.values())

    # =====================================================
    # SUMMARY
    # =====================================================
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city  = r["city"]
        venue = r["venue"]

        sold  = r["sold"]
        total = r["totalSeats"]
        gross = r["gross"]
        occ   = calc_occupancy(sold, total)

        if movie not in summary:
            summary[movie] = {
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "venues": set(),
                "cities": set(),
                "fastfilling": 0,
                "housefull": 0,
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

    final_summary = {
        movie: {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": calc_occupancy(m["sold"], m["totalSeats"]),
        }
        for movie, m in summary.items()
    }

    # =====================================================
    # SAVE
    # =====================================================
    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
