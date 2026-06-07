#!/usr/bin/env python3
"""
Run from Termux: python3 ~/DataEngineering/etoro-react/diag_api.py
Discovers what recent-trade data is available for ThomasPJ
"""
import requests, json

CID = 12569157
USERNAME = "ThomasPJ"
HDRS = {"User-Agent": "Mozilla/5.0 (Android 13; Mobile)"}

def check(label, url, params=None):
    try:
        r = requests.get(url, headers=HDRS, params=params, timeout=12)
        print(f"\n{'='*60}")
        print(f"{label}  [{r.status_code}]")
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, dict):
                print("Keys:", list(d.keys()))
                for k, v in d.items():
                    if isinstance(v, list):
                        print(f"  {k}: {len(v)} items", end="")
                        if v: print(f"  first keys: {list(v[0].keys()) if isinstance(v[0], dict) else type(v[0])}", end="")
                        print()
            elif isinstance(d, list):
                print(f"Array: {len(d)} items")
                if d and isinstance(d[0], dict):
                    print("First item keys:", list(d[0].keys()))
        else:
            print(r.text[:200])
    except Exception as e:
        print(f"  ERROR: {e}")

# 1. Portfolio actuel (voir s'il y a ClosedPositions)
check("Portfolio live",
    f"https://www.etoro.com/sapi/trade-data-real/live/public/portfolios",
    {"cid": CID})

# 2. Historique portfolio
check("Portfolio history",
    f"https://www.etoro.com/sapi/trade-data-real/history/public/credit/flat/portfolios",
    {"cid": CID, "PageSize": 10})

# 3. Activity feed social
check("Social activity",
    f"https://www.etoro.com/api/logininfo/v1.1/users/{USERNAME}/activity")

# 4. Social feed
check("Social feed",
    f"https://www.etoro.com/api/logininfo/v1.1/users/{USERNAME}/social/feed")

# 5. Feed alternatif
check("User stream",
    f"https://www.etoro.com/api/logininfo/v1.1/users/{USERNAME}/social/userstream")

# 6. Stats portfolio
check("Portfolio stats",
    f"https://www.etoro.com/sapi/userstats/CopierStats/data/users/{CID}/portfolio")

print("\n\nDone.")
