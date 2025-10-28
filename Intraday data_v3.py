import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta

# ================== CONFIG ==================
ACCESS_TOKEN = ""
INPUT_CSV = r"C:\Niranjan\Personal\Stock_Price_pridiction\extracted_instrument_keys_nse_eq_filtered.csv"
NSE_JSON = r"C:\Niranjan\Personal\Stock_Price_pridiction\NSE.json"
OUT_CSV = r"C:\Niranjan\Personal\Stock_Price_pridiction\historical_15min_data.csv"
RATE_LIMIT_DELAY = 0.3  # seconds between calls
UNIT = "minutes"
INTERVAL = 15
MAX_DAYS_PER_CALL = 30  # Upstox limit for intraday window
# ============================================

# 📅 Ask for date input
from_date_str = input("Enter start date (YYYY-MM-DD): ").strip()
if not from_date_str:
    from_date_str = "2025-08-17"

from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
to_date = datetime.now()

print(f"\nFetching 15-min candles from {from_date.date()} → {to_date.date()}")

# 📘 Load instrument-key mapping
df_keys = pd.read_csv(INPUT_CSV)
if 'instrument_key' not in df_keys.columns or 'trading_symbol' not in df_keys.columns:
    raise SystemExit("❌ CSV must contain 'trading_symbol' and 'instrument_key' columns")

# 📗 Load optional metadata
with open(NSE_JSON, 'r') as f:
    master_items = json.load(f)
meta_map = {it.get('instrument_key'): it for it in master_items if it.get('instrument_key')}

records = []
unique_keys = df_keys['instrument_key'].dropna().astype(str).unique().tolist()
print(f"\n📈 Total instruments to fetch: {len(unique_keys)}")

# 🧭 Helper: generate date segments (30-day chunks)
def generate_date_ranges(start, end, step_days):
    ranges = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=step_days), end)
        ranges.append((cur, nxt))
        cur = nxt
    return ranges

# 🧭 Loop through each instrument key
for idx, ik in enumerate(unique_keys, start=1):
    print(f"\n[{idx}/{len(unique_keys)}] Fetching data for {ik} ...")
    date_ranges = generate_date_ranges(from_date, to_date, MAX_DAYS_PER_CALL)
    total_candles = 0

    for (start_dt, end_dt) in date_ranges:
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")
        url = f"https://api.upstox.com/v3/historical-candle/{ik}/{UNIT}/{INTERVAL}/{end_str}/{start_str}"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {ACCESS_TOKEN}"
        }

        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                print(f"  ⚠️ {start_str}→{end_str} Error {resp.status_code}: {resp.text}")
                time.sleep(RATE_LIMIT_DELAY)
                continue
            data = resp.json().get("data", {})
            candles = data.get("candles", [])
        except Exception as e:
            print(f"  ⚠️ Exception {start_str}→{end_str}: {e}")
            time.sleep(RATE_LIMIT_DELAY)
            continue

        if not candles:
            print(f"  No candles for {start_str}→{end_str}")
            time.sleep(RATE_LIMIT_DELAY)
            continue

        meta = meta_map.get(ik, {})
        trading_symbol = (
            meta.get("trading_symbol")
            or df_keys.loc[df_keys['instrument_key'] == ik, 'trading_symbol'].iloc[0]
        )

        for c in candles:
            rec = {
                "datetime": c[0] if len(c) > 0 else None,
                "open": c[1] if len(c) > 1 else None,
                "high": c[2] if len(c) > 2 else None,
                "low": c[3] if len(c) > 3 else None,
                "close": c[4] if len(c) > 4 else None,
                "volume": c[5] if len(c) > 5 else None,
                "open_interest": None,
                "segment": meta.get("segment", ""),
                "name": meta.get("name", ""),
                "asset_symbol": meta.get("asset_symbol", ""),
                "tick_size": meta.get("tick_size", ""),
                "asset_type": meta.get("asset_type", ""),
                "strike_price": meta.get("strike_price", ""),
                "trading_symbol": trading_symbol
            }
            records.append(rec)

        total_candles += len(candles)
        print(f"  ✅ {start_str}→{end_str}: {len(candles)} candles")
        time.sleep(RATE_LIMIT_DELAY)

    print(f"  📊 Total {total_candles} candles fetched for {ik}")

# 💾 Save to CSV
if records:
    df_out = pd.DataFrame(records)
    try:
        df_out["datetime_iso"] = pd.to_datetime(df_out["datetime"])
    except Exception:
        pass
    df_out = df_out[[
        "datetime","open","high","low","close","volume","open_interest",
        "segment","name","asset_symbol","tick_size","asset_type","strike_price","trading_symbol"
    ]]
    df_out.to_csv(OUT_CSV, index=False)
    print(f"\n✅ Wrote {len(df_out)} rows to {OUT_CSV}")
else:
    print("\n⚠️ No candles fetched for any instrument.")

