import os, json, time
from datetime import date, timedelta, datetime
import requests
import pandas as pd

# --- config from env ---
API_TOKEN = os.environ.get("EODHD_API_TOKEN") or ""
BUCKET_ROOT = os.environ.get("BUCKET_ROOT", "s3://stock-options-ai-app/ohlc")
TICKERS_FILE = os.environ.get("TICKERS_FILE", "tickers.txt")
REGION = os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
AWS_PROFILE = os.environ.get("AWS_PROFILE")  # optional

if not API_TOKEN:
    raise SystemExit("Set EODHD_API_TOKEN")

STORAGE_OPTS = {"anon": False, "client_kwargs": {"region_name": REGION}}
if AWS_PROFILE:
    STORAGE_OPTS["profile"] = AWS_PROFILE

def parquet_path(symbol, year):
    return f"{BUCKET_ROOT}/curated/exchange=NSE/ticker={symbol}/year={year}/part-000.parquet"

def manifest_path(symbol):
    return f"{BUCKET_ROOT}/meta/manifests/{symbol}.json"

def read_manifest_max(symbol):
    import s3fs
    fs = s3fs.S3FileSystem(**STORAGE_OPTS)
    try:
        with fs.open(manifest_path(symbol)) as f:
            m = json.load(f)
            if m.get("max_date"):
                return datetime.fromisoformat(m["max_date"]).date()
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return None

def write_manifest(symbol, all_dates):
    import s3fs
    fs = s3fs.S3FileSystem(**STORAGE_OPTS)
    meta = {
        "ticker": symbol,
        "exchange": "NSE",
        "min_date": str(min(all_dates)) if all_dates else None,
        "max_date": str(max(all_dates)) if all_dates else None,
        "rows": int(len(all_dates)),
        "updated_at": date.today().isoformat(),
    }
    with fs.open(manifest_path(symbol), "w") as f:
        f.write(json.dumps(meta, indent=2))

def fetch_range(symbol, d1, d2):
    url = f"https://eodhd.com/api/eod/{symbol}"
    p = {"from": d1.isoformat(), "to": d2.isoformat(), "api_token": API_TOKEN, "fmt": "json", "order": "a"}
    for attempt in range(4):
        r = requests.get(url, params=p, timeout=45)
        if r.status_code == 429 and attempt < 3:
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    return []

def normalize(rows, symbol):
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for c in ["open","high","low","close","adjusted_close","volume"]:
        if c not in df.columns:
            df[c] = None
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.drop_duplicates("date").sort_values("date")
    df["ticker"] = symbol
    df["exchange"] = "NSE"
    # sanity
    if not df.empty and (df["date"].max() > date.today()):
        df = df[df["date"] <= date.today()]
    return df[["date","open","high","low","close","adjusted_close","volume","ticker","exchange"]]

def merge_year(symbol, df):
    years = pd.to_datetime(df["date"]).dt.year.unique()
    for y in years:
        path = parquet_path(symbol, int(y))
        dfy = df[pd.to_datetime(df["date"]).dt.year == y]
        try:
            existing = pd.read_parquet(path, storage_options=STORAGE_OPTS)
            out = pd.concat([existing, dfy], ignore_index=True).drop_duplicates("date").sort_values("date")
        except Exception:
            out = dfy.sort_values("date")
        out.to_parquet(path, index=False, storage_options=STORAGE_OPTS)

def list_all_dates(symbol):
    """Fast-ish: read only the 'date' column from each year file we just touched (or fallback)."""
    dates = []
    # try the current year and last year (cheap, typical for daily)
    years = [date.today().year, date.today().year - 1]
    for y in years:
        p = parquet_path(symbol, y)
        try:
            d = pd.read_parquet(p, columns=["date"], storage_options=STORAGE_OPTS)
            dates.append(pd.to_datetime(d["date"]).dt.date)
        except Exception:
            pass
    if dates:
        return pd.concat([pd.Series(x) for x in dates], ignore_index=True).drop_duplicates().tolist()
    # fallback: nothing found (maybe first ever update)
    return []

def main():
    yday = date.today() - timedelta(days=1)  # avoid partial 'today'
    with open(TICKERS_FILE) as f:
        tickers = [t.strip() for t in f if t.strip()]

    total_rows = 0
    updated = 0
    skipped = 0

    for sym in tickers:
        last = read_manifest_max(sym)
        start = (last + timedelta(days=1)) if last else (yday - timedelta(days=7))  # small backstop if no manifest
        if start > yday:
            print(f"[skip] {sym}: up-to-date (max_date={last})")
            skipped += 1
            continue

        rows = fetch_range(sym, start, yday)
        df = normalize(rows, sym)
        if df.empty:
            print(f"[ok] {sym}: no new rows (weekend/holiday)")
            skipped += 1
            continue

        merge_year(sym, df)
        # refresh manifest
        all_dates = list_all_dates(sym)
        all_dates.extend(df["date"].tolist())
        all_dates = sorted(set(all_dates))
        write_manifest(sym, all_dates)

        total_rows += len(df)
        updated += 1
        md = f"{df['date'].min()}→{df['date'].max()}"
        print(f"[update] {sym}: {len(df)} rows ({md})")

    print(f"[DONE] tickers={len(tickers)} updated={updated} skipped={skipped} rows_added={total_rows}")

if __name__ == "__main__":
    main()
