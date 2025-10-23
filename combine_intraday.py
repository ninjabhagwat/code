"""
Simple utility to combine two 15-min intraday CSVs and group/save by trading_symbol.
Saves a master combined CSV and per-symbol CSVs in a directory.

Usage:
    python combine_intraday.py
or override inputs/outputs:
    python combine_intraday.py --a path/to/intraday_15min_latest.csv \
        --b path/to/historical_15min_data.csv --out master.csv --out-dir combined_by_symbol
"""
import os
import argparse
import pandas as pd

DEFAULT_A = r"C:\Niranjan\Personal\Stock_Price_pridiction\intraday_15min_latest.csv"
DEFAULT_B = r"C:\Niranjan\Personal\Stock_Price_pridiction\historical_15min_data.csv"
DEFAULT_OUT = r"C:\Niranjan\Personal\Stock_Price_pridiction\combined_historical_intraday.csv"
DEFAULT_OUT_DIR = r"C:\Niranjan\Personal\Stock_Price_pridiction\combined_by_symbol"


def load_csv(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    # try to parse datetime column if present
    try:
        df = pd.read_csv(path, parse_dates=["datetime"]) if "datetime" in pd.read_csv(path, nrows=0).columns else pd.read_csv(path)
    except Exception:
        df = pd.read_csv(path)
    return df


def main(a, b, out_master, out_dir):
    print("Loading files...")
    df_a = load_csv(a)
    df_b = load_csv(b)

    # Ensure columns align
    cols_a = list(df_a.columns)
    cols_b = list(df_b.columns)
    if set(cols_a) != set(cols_b):
        # try to align by intersection and warn
        common = [c for c in cols_a if c in cols_b]
        if not common:
            raise RuntimeError("No common columns between files")
        print("Warning: Columns differ between files. Using intersection of columns:", common)
        df_a = df_a[common]
        df_b = df_b[common]

    # concat and clean
    df = pd.concat([df_b, df_a], ignore_index=True)  # historical first, latest appended

    # normalize datetime
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    # drop rows with missing trading_symbol or datetime
    if "trading_symbol" in df.columns:
        df = df.dropna(subset=["trading_symbol"])
    if "datetime" in df.columns:
        df = df.dropna(subset=["datetime"])

    # sort
    sort_cols = []
    if "trading_symbol" in df.columns:
        sort_cols.append("trading_symbol")
    if "datetime" in df.columns:
        sort_cols.append("datetime")
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    # drop exact duplicates
    if set(["trading_symbol", "datetime"]).issubset(df.columns):
        before = len(df)
        df = df.drop_duplicates(subset=["trading_symbol", "datetime"], keep="last").reset_index(drop=True)
        after = len(df)
        print(f"Dropped {before-after} duplicate rows (by trading_symbol+datetime)")

    # save master
    os.makedirs(os.path.dirname(out_master), exist_ok=True)
    df.to_csv(out_master, index=False)
    print(f"Wrote master combined CSV: {out_master} ({len(df)} rows)")

    # save per-symbol files
    os.makedirs(out_dir, exist_ok=True)
    if "trading_symbol" in df.columns:
        grouped = df.groupby("trading_symbol")
        print(f"Saving per-symbol files to: {out_dir} (symbols: {len(grouped)})")
        for sym, g in grouped:
            # safe filename
            safe = "".join(c if c.isalnum() or c in ("-","_") else "_" for c in str(sym))
            path = os.path.join(out_dir, f"{safe}.csv")
            g.to_csv(path, index=False)
    else:
        print("No trading_symbol column present; skipping per-symbol files.")

    print("Done.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Combine two intraday CSVs and group by trading_symbol")
    p.add_argument("--a", default=DEFAULT_A, help="Path to first CSV (default intraday latest)")
    p.add_argument("--b", default=DEFAULT_B, help="Path to second CSV (default historical)")
    p.add_argument("--out", default=DEFAULT_OUT, help="Path to write master combined CSV")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Directory to write per-symbol CSVs")
    args = p.parse_args()
    main(args.a, args.b, args.out, args.out_dir)
