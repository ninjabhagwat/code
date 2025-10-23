import pandas as pd
import json

# Paths
csv_path = r"C:\Niranjan\Personal\Stock_Price_pridiction\extracted_instrument_keys_nse_eq_filtered.csv"
json_path = r"C:\Niranjan\Personal\Stock_Price_pridiction\NSE.json"
output_path = r"C:\Niranjan\Personal\Stock_Price_pridiction\remapped_instrument_keys.csv"

# Load CSV
df_csv = pd.read_csv(csv_path)

# Load NSE master JSON
with open(json_path, 'r', encoding='utf-8') as f:
    master_data = json.load(f)

# master_data may be a dict with 'data' or similar
if isinstance(master_data, dict):
    possible = master_data.get('data') or master_data.get('instruments') or master_data.get('items') or master_data.get('rows')
    if isinstance(possible, list):
        master_list = possible
    else:
        # fallback: try to find a list inside the dict
        master_list = []
        for v in master_data.values():
            if isinstance(v, list):
                master_list = v
                break
else:
    master_list = master_data if isinstance(master_data, list) else []

# Build ISIN to instrument_key map (normalize ISINs)
isin_map = {}
trading_map = {}
for item in master_list:
    isin_val = item.get('isin') or item.get('ISIN') or item.get('Isin')
    ik = item.get('instrument_key') or item.get('instrumentKey') or item.get('instrumentKeyId') or item.get('instrumentToken')
    ts = item.get('trading_symbol') or item.get('tradingSymbol') or item.get('symbol') or item.get('name')
    if isin_val and ik:
        isin_norm = str(isin_val).strip().upper()
        isin_map[isin_norm] = {
            'instrument_key': ik,
            'trading_symbol': ts
        }
    if ts and ik:
        trading_map[str(ts).strip().upper()] = ik

# Remap ISINs
remapped = []
unmapped = []
for _, row in df_csv.iterrows():
    src_ik = str(row.get('instrument_key') or '')
    # Some rows may contain multiple instrument_keys separated by '|' or pipes; pick last token as ISIN
    parts = [p.strip() for p in src_ik.split('|') if p.strip()]
    isin = parts[-1] if parts else ''
    isin_norm = isin.strip().upper()

    mapped = None
    if isin_norm and isin_norm in isin_map:
        mapped = isin_map[isin_norm]
    else:
        # Fallback: try match by trading_symbol column in CSV
        ts_csv = None
        if 'trading_symbol' in df_csv.columns:
            ts_csv = str(row.get('trading_symbol') or '').strip()
        # try upper keys
        if ts_csv:
            mapped_ik = trading_map.get(ts_csv.upper())
            if mapped_ik:
                mapped = {'instrument_key': mapped_ik, 'trading_symbol': ts_csv}
        # Fallback: scan master_list for isin substring or trading symbol similarity
        if mapped is None and isin_norm:
            for item in master_list:
                candidate_isin = str(item.get('isin') or '').strip().upper()
                if candidate_isin and isin_norm == candidate_isin:
                    mapped = {'instrument_key': item.get('instrument_key') or item.get('instrumentKey'), 'trading_symbol': item.get('trading_symbol')}
                    break

    if mapped:
        remapped.append({
            'source_instrument_key': src_ik,
            'resolved_instrument_key': mapped['instrument_key'],
            'trading_symbol': mapped.get('trading_symbol') or ''
        })
    else:
        unmapped.append({
            'source_instrument_key': src_ik,
            'resolved_instrument_key': None,
            'trading_symbol': row.get('trading_symbol', '') if 'trading_symbol' in df_csv.columns else ''
        })

# Combine and save (keep unmapped for debugging)
out_df = pd.DataFrame(remapped + unmapped)
out_df.to_csv(output_path, index=False)
print(f"Remapped {len(remapped)} instrument keys. {len(unmapped)} unmapped. Saved to: {output_path}")

# Print a small sample of unmapped keys to help debugging
if unmapped:
    print('\nSample unmapped instrument keys:')
    for u in unmapped[:20]:
        print(' ', u['source_instrument_key'], '->', u['resolved_instrument_key'], ' trading_symbol:', u['trading_symbol'])
