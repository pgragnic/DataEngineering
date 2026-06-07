#!/usr/bin/env python3
"""Run from Termux: python3 ~/DataEngineering/etoro-react/diag_api2.py"""
import requests, json

CID = 12569157
USERNAME = "ThomasPJ"
HDRS = {"User-Agent": "Mozilla/5.0 (Android 13; Mobile)"}

def get(url, params=None):
    r = requests.get(url, headers=HDRS, params=params, timeout=12)
    r.raise_for_status()
    return r.json()

# --- 1. Inspecter tous les champs du portfolio live ---
print("=== PORTFOLIO LIVE - tous les champs ===")
d = get("https://www.etoro.com/sapi/trade-data-real/live/public/portfolios", {"cid": CID})
for k, v in d.items():
    if isinstance(v, list):
        print(f"{k}: {len(v)} items")
        if v and isinstance(v[0], dict):
            print(f"  keys: {list(v[0].keys())}")
            print(f"  first: {json.dumps(v[0])}")
    else:
        print(f"{k}: {v}")

# --- 2. Tenter des endpoints alternatifs ---
endpoints = [
    ("History flat", f"https://www.etoro.com/sapi/trade-data-real/history/public/credit/flat", {"cid": CID, "PageSize": 5}),
    ("History v2",   f"https://www.etoro.com/sapi/trade-data-real/history/public/portfolios",  {"cid": CID}),
    ("Social posts", f"https://www.etoro.com/api/logininfo/v1.1/users/{USERNAME}/social/posts", None),
    ("Activity v2",  f"https://www.etoro.com/api/logininfo/v1.1/users/{USERNAME}/social/userActivity", None),
    ("Feed v2",      f"https://www.etoro.com/social/feed/{USERNAME}", None),
    ("NewsStream",   f"https://www.etoro.com/api/logininfo/v1.1/users/{USERNAME}/social/newsStream", None),
]

for label, url, params in endpoints:
    try:
        r = requests.get(url, headers=HDRS, params=params, timeout=8)
        print(f"\n{label} [{r.status_code}]")
        if r.status_code == 200:
            try:
                d2 = r.json()
                if isinstance(d2, dict):
                    print("  keys:", list(d2.keys())[:8])
                elif isinstance(d2, list):
                    print(f"  array {len(d2)} items")
                    if d2: print("  first keys:", list(d2[0].keys()) if isinstance(d2[0], dict) else type(d2[0]))
            except:
                print("  (non-JSON)", r.text[:100])
        elif r.status_code != 404:
            print(" ", r.text[:100])
    except Exception as e:
        print(f"  ERROR: {e}")

print("\nDone.")
