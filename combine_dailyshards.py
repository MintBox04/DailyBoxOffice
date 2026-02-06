import json
import os
from datetime import datetime
import pytz

# =====================================================
# DATE (IST TODAY)
# =====================================================
IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)
DATE_CODE = NOW_IST.strftime("%Y%m%d")
LAST_UPDATED = NOW_IST.strftime("%Y-%m-%d %H:%M IST")

BASE_DIR = f"daily/data/{DATE_CODE}"
FINAL_DETAILED = os.path.join(BASE_DIR, "finaldetailed.json")
FINAL_SUMMARY  = os.path.join(BASE_DIR, "finalsummary.json")

print(f"📁 Using directory: {BASE_DIR}")
print(f"⏱ Last updated: {LAST_UPDATED}")

# =====================================================
# HELPERS
# =====================================================
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def calc_occupancy(sold, total):
    return round((sold / total) * 100, 2) if total else 0.0

# =====================================================
# NORMALIZE ROW (CASE SAFE)
# =====================================================
def normalize_row(r):
    r["movie"] = r.get("movie") or "Unknown"

    raw_city  = r.get("city") or "Unknown"
    raw_state = r.get("state") or "Unknown"

    # 🔑 keys for grouping (ignore case)
    r["_city_key"]  = raw_city.strip().lower()
    r["_state_key"] = raw_state.strip().lower()

    # 👀 display values
    r["city"]  = raw_city.strip().title()
    r["state"] = raw_state.strip().title()

    r["venue"] = r.get("venue") or "Unknown"
    r["address"] = r.get("address") or ""
    r["time"] = r.get("time") or ""
    r["audi"] = r.get("audi") or ""
    r["session_id"] = str(r.get("session_id") or "")
    r["source"] = r.get("source") or "Unknown"
    r["date"] = r.get("date") or DATE_CODE

    r["language"]  = (r.get("language") or "UNKNOWN").upper()
    r["dimension"] = (r.get("dimension") or "UNKNOWN").upper()

    r["totalSeats"] = int(r.get("totalSeats") or 0)
    r["available"]  = int(r.get("available") or 0)
    r["sold"]       = int(r.get("sold") or 0)
    r["gross"]      = float(r.get("gross") or 0.0)

    return r

# =====================================================
# DEDUPE
# =====================================================
def dedupe(rows):
    seen = set()
    out = []
    dupes = 0

    for r in rows:
        key = (
            r["venue"],
            r["time"],
            r["session_id"],
            r["audi"],
        )
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        out.append(r)

    return out, dupes

# =====================================================
# LOAD ALL SHARDS
# =====================================================
all_rows = []

for i in range(1, 10):
    path = os.path.join(BASE_DIR, f"detailed{i}.json")
    data = load_json(path)
    if data:
        print(f"✅ detailed{i}.json → {len(data)} rows")
        all_rows.extend(data)

print(f"📊 Raw rows: {len(all_rows)}")

# =====================================================
# NORMALIZE + DEDUPE
# =====================================================
all_rows = [normalize_row(r) for r in all_rows]
final_rows, dupes = dedupe(all_rows)

print(f"🧹 Duplicates removed: {dupes}")
print(f"🎯 Final detailed rows: {len(final_rows)}")

# =====================================================
# SORT FINAL DETAILED
# =====================================================
final_rows.sort(
    key=lambda x: (x["movie"], x["city"], x["venue"], x["time"])
)

# =====================================================
# SAVE finaldetailed.json
# =====================================================
save_json(
    FINAL_DETAILED,
    {
        "last_updated": LAST_UPDATED,
        "data": final_rows
    }
)

print("🎉 finaldetailed.json saved")

# =====================================================
# BUILD FINAL SUMMARY (CASE-INSENSITIVE CITY)
# =====================================================
summary = {}

for r in final_rows:
    movie = r["movie"]
    venue = r["venue"]
    city_key  = r["_city_key"]
    state_key = r["_state_key"]
    city  = r["city"]
    state = r["state"]
    lang  = r["language"]
    dim   = r["dimension"]

    total = r["totalSeats"]
    sold  = r["sold"]
    gross = r["gross"]
    occ   = (sold / total * 100) if total else 0

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
            "City_details": {},
            "Language_details": {},
            "Format_details": {}
        }

    m = summary[movie]
    m["shows"] += 1
    m["gross"] += gross
    m["sold"] += sold
    m["totalSeats"] += total
    m["venues"].add(venue)
    m["cities"].add(city_key)

    if occ >= 98:
        m["housefull"] += 1
    elif occ >= 50:
        m["fastfilling"] += 1

    # ---------------- CITY (IGNORE CASE) ----------------
    ck = (city_key, state_key)
    if ck not in m["City_details"]:
        m["City_details"][ck] = {
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

    d = m["City_details"][ck]
    d["venues"].add(venue)
    d["shows"] += 1
    d["gross"] += gross
    d["sold"] += sold
    d["totalSeats"] += total

    if occ >= 98:
        d["housefull"] += 1
    elif occ >= 50:
        d["fastfilling"] += 1

    # ---------------- LANGUAGE ----------------
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

    # ---------------- FORMAT ----------------
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

# =====================================================
# FINALIZE SUMMARY JSON
# =====================================================
final_summary = {}

for movie, m in summary.items():
    final_summary[movie] = {
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

    for d in m["City_details"].values():
        final_summary[movie]["City_details"].append({
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
        final_summary[movie]["Language_details"].append({
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
        final_summary[movie]["Format_details"].append({
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

# =====================================================
# SAVE finalsummary.json
# =====================================================
save_json(
    FINAL_SUMMARY,
    {
        "last_updated": LAST_UPDATED,
        "movies": final_summary
    }
)

print("🎉 finalsummary.json created successfully")
print("📄 Files ready:")
print(f"   • {FINAL_DETAILED}")
print(f"   • {FINAL_SUMMARY}")
