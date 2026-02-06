import json
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
import pytz
import json
from io import BytesIO



SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 9
CONCURRENCY = 20
TIMEOUT = aiohttp.ClientTimeout(total=25)

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)

DATE_CODE = (NOW_IST + timedelta(days=0)).strftime("%Y%m%d")
DATE_DISTRICT = (NOW_IST + timedelta(days=0)).strftime("%Y-%m-%d")

BASE_DIR = os.path.join("daily", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

DETAILED_FILE = os.path.join(BASE_DIR, f"detailed{SHARD_ID}.json")
SUMMARY_FILE  = os.path.join(BASE_DIR, f"movie_summary{SHARD_ID}.json")
LOG_FILE      = os.path.join(LOG_DIR, f"district{SHARD_ID}.log")
API_URL = "https://district.boxoffice24.workers.dev?cinema_id={cid}&date={date}"

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
# LOAD VENUES
# =====================================================

venues_path = os.path.join(SCRIPT_DIR, "districtvenues.json")
with open(venues_path, "r", encoding="utf-8") as f:
    DIST_VENUES = json.load(f)

log(f"📍 Loaded {len(DIST_VENUES)} district venues")

# =====================================================
# HELPERS
# =====================================================
def calc_occupancy(sold, total):
    return round((sold / total) * 100, 2) if total else 0.0

def dedupe(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r["venue"], r["time"], r["session_id"], r["audi"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def norm(s):
    return s.strip() if s else "UNKNOWN"

# =====================================================
# FETCH SINGLE VENUE
# =====================================================
async def fetch_one(session, venue):
    cid = venue.get("id")
    url = API_URL.format(cid=cid, date=DATE_DISTRICT)

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"⚠ {cid} status {resp.status}")
                return None

            data = await resp.json()
            session_dates = data.get("data", {}).get("sessionDates", [])

            # ---- DATE SKIP ----
            if DATE_DISTRICT not in session_dates:
                return None

            return {"venue": venue, "data": data}

    except Exception as e:
        log(f"❌ {cid} {type(e).__name__}")
        return None

# =====================================================
# FETCH ALL
# =====================================================
async def fetch_all():
    sem = asyncio.Semaphore(CONCURRENCY)
    out = []

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        async def bound(v):
            async with sem:
                return await fetch_one(session, v)

        results = await asyncio.gather(*(bound(v) for v in DIST_VENUES))

    for r in results:
        if r:
            out.append(r)

    log(f"✅ Fetched {len(out)} venues with shows")
    return out


def parse_showtime(show_time):
    """
    Handles:
    - ISO string: 2026-01-04T18:30
    - Epoch ms (int or str)
    - None
    """
    if not show_time:
        return ""

    try:
        # Epoch milliseconds
        if isinstance(show_time, (int, float)) or str(show_time).isdigit():
            ts = int(show_time) / 1000
            return datetime.fromtimestamp(ts, tz=pytz.UTC) \
                .astimezone(IST) \
                .strftime("%I:%M %p")

        # ISO datetime
        return datetime.strptime(show_time, "%Y-%m-%dT%H:%M") \
            .replace(tzinfo=pytz.UTC) \
            .astimezone(IST) \
            .strftime("%I:%M %p")

    except Exception:
        return ""


# =====================================================
# PARSE
# =====================================================
def parse(results):
    detailed = []

    for res in results:
        venue_meta = res["venue"]
        data = res["data"]

        city  = venue_meta.get("city", "Unknown")
        state = venue_meta.get("state", "Unknown")

        cinema = data.get("meta", {}).get("cinema", {})
        venue_name = cinema.get("name") or venue_meta.get("name") or "Unknown"
        venue_addr = cinema.get("address") or venue_meta.get("address") or ""

        movie_map = {}
        for m in data.get("meta", {}).get("movies", []) or []:
            movie_map[m.get("id")] = m
            movie_map[str(m.get("id"))] = m

        for s in data.get("pageData", {}).get("sessions", []) or []:
            movie = movie_map.get(s.get("mid")) or movie_map.get(str(s.get("mid")))
            if not movie:
                continue

            name = movie.get("name", "Unknown")
            lang = norm(s.get("lang") or movie.get("lang"))
            fmt  = norm(s.get("scrnFmt")).replace("-", " | ")


            total = int(s.get("total", 0))
            avail = int(s.get("avail", 0))
            sold  = total - avail

            gross = sum(
                (a.get("sTotal", 0) - a.get("sAvail", 0)) * a.get("price", 0)
                for a in s.get("areas", []) or []
            )

            detailed.append({
                "movie": name,
                "city": city,
                "state": state,
                "venue": venue_name,
                "address": venue_addr,
                "time": parse_showtime(s.get("showTime")),

                "audi": s.get("audi", ""),
                "session_id": str(s.get("id", "")),
                "totalSeats": total,
                "available": avail,
                "sold": sold,
                "gross": round(gross, 2),
                "language": lang,
                "dimension": fmt,
                "source": "District",
                "date": DATE_CODE
            })

    return dedupe(detailed)

# =====================================================
# BUILD SUMMARY (NO CHAIN)
# =====================================================
def build_summary(detailed):
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city  = r["city"]
        state = r["state"]
        venue = r["venue"]
        lang  = r["language"]
        dim   = r["dimension"]

        total = r["totalSeats"]
        sold  = r["sold"]
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
                "details": {},
                "Language_details": {},
                "Format_details": {}
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

        # -------- CITY --------
        ck = (city, state)
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city,
                "state": state,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

        d = m["details"][ck]
        d["venues"].add(venue)
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98:
            d["housefull"] += 1
        elif occ >= 50:
            d["fastfilling"] += 1

        # -------- LANGUAGE --------
        if lang not in m["Language_details"]:
            m["Language_details"][lang] = {
                "language": lang,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

        L = m["Language_details"][lang]
        L["venues"].add(venue)
        L["shows"] += 1
        L["gross"] += gross
        L["sold"] += sold
        L["totalSeats"] += total
        if occ >= 98:
            L["housefull"] += 1
        elif occ >= 50:
            L["fastfilling"] += 1

        # -------- FORMAT --------
        if dim not in m["Format_details"]:
            m["Format_details"][dim] = {
                "dimension": dim,
                "venues": set(),
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0,
                "housefull": 0
            }

        F = m["Format_details"][dim]
        F["venues"].add(venue)
        F["shows"] += 1
        F["gross"] += gross
        F["sold"] += sold
        F["totalSeats"] += total
        if occ >= 98:
            F["housefull"] += 1
        elif occ >= 50:
            F["fastfilling"] += 1

    final = {}
    for movie, m in summary.items():
        final[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": calc_occupancy(m["sold"], m["totalSeats"]),
            "City_details": [],
            "Language_details": [],
            "Format_details": []
        }

        for d in m["details"].values():
            final[movie]["City_details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": calc_occupancy(d["sold"], d["totalSeats"])
            })

        for l in m["Language_details"].values():
            final[movie]["Language_details"].append({
                "language": l["language"],
                "venues": len(l["venues"]),
                "shows": l["shows"],
                "gross": round(l["gross"], 2),
                "sold": l["sold"],
                "totalSeats": l["totalSeats"],
                "fastfilling": l["fastfilling"],
                "housefull": l["housefull"],
                "occupancy": calc_occupancy(l["sold"], l["totalSeats"])
            })

        for f in m["Format_details"].values():
            final[movie]["Format_details"].append({
                "dimension": f["dimension"],
                "venues": len(f["venues"]),
                "shows": f["shows"],
                "gross": round(f["gross"], 2),
                "sold": f["sold"],
                "totalSeats": f["totalSeats"],
                "fastfilling": f["fastfilling"],
                "housefull": f["housefull"],
                "occupancy": calc_occupancy(f["sold"], f["totalSeats"])
            })

    return final

# =====================================================
# ENTRY
# =====================================================
async def main():
    log("🚀 DISTRICT SCRAPER STARTED")
    results = await fetch_all()
    detailed = parse(results)
    summary = build_summary(detailed)

    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")

if __name__ == "__main__":
    asyncio.run(main())
