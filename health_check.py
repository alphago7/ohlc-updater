import os, json
from datetime import date, timedelta
import pandas as pd
import s3fs

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
PROFILE = os.environ.get("AWS_PROFILE")
BUCKET_ROOT = os.environ.get("BUCKET_ROOT", "s3://stock-options-ai-app/ohlc")

fs_kwargs = {"anon": False, "client_kwargs": {"region_name": REGION}}
if PROFILE: fs_kwargs["profile"] = PROFILE
fs = s3fs.S3FileSystem(**fs_kwargs)

def last_business_day(d: date) -> date:
    # simple Mon–Fri calendar; refine later with NSE holidays
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d -= timedelta(days=1)
    return d

def main():
    yday = last_business_day(date.today() - timedelta(days=1))
    manifests = fs.glob(f"{BUCKET_ROOT}/meta/manifests/*.json")
    rows = []
    for p in manifests:
        with fs.open(p) as f:
            m = json.load(f)
        sym = m["ticker"]
        maxd = m.get("max_date")
        if not maxd:
            rows.append((sym, None, None, "no-data"))
            continue
        lag = (yday - pd.to_datetime(maxd).date()).days
        status = "ok" if lag <= 0 else ("stale-1d" if lag == 1 else "stale")
        rows.append((sym, maxd, lag, status))

    df = pd.DataFrame(rows, columns=["ticker","max_date","days_behind","status"]).sort_values(["status","ticker"])
    # write a run report
    outp = f"{BUCKET_ROOT}/meta/runs/{date.today().isoformat()}_health.csv"
    df.to_csv(outp, index=False, storage_options=fs_kwargs)
    print(df.head(20).to_string(index=False))

    # fail CI if too many stale tickers (so you notice)
    many_stale = (df["days_behind"] > 1).sum()
    if many_stale > 0:
        print(f"[WARN] tickers >1 day behind: {many_stale}")
    # optional hard fail:
    # import sys; sys.exit(1 if many_stale > 0 else 0)

if __name__ == "__main__":
    main()
