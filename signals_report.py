import os
from datetime import date
import pandas as pd

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
PROFILE = os.environ.get("AWS_PROFILE")
BUCKET_ROOT = os.environ.get("BUCKET_ROOT", "s3://stock-options-ai-app/ohlc")
TICKERS_FILE = os.environ.get("TICKERS_FILE", "tickers.txt")

fsopts = {"anon": False, "client_kwargs": {"region_name": REGION}}
if PROFILE: fsopts["profile"] = PROFILE

def feat_path(symbol, year):
    return f"{BUCKET_ROOT}/features/exchange=NSE/ticker={symbol}/year={year}/part-000.parquet"

def main():
    y = date.today().year
    with open(TICKERS_FILE) as f:
        tickers = [t.strip() for t in f if t.strip()]

    rows = []
    for sym in tickers:
        try:
            df = pd.read_parquet(feat_path(sym, y), storage_options=fsopts)
            if df.empty: continue
            last = df.sort_values("date").iloc[-1]
            rows.append({
                "ticker": sym,
                "date": str(last["date"])[:10],
                "px": float(last["px"]),
                "bull_cross": bool(last["bull_cross"]),
                "bear_cross": bool(last["bear_cross"]),
                "rsi14": float(last["rsi14"]) if pd.notna(last["rsi14"]) else None,
                "rsi_overbought": bool(last["rsi_overbought"]) if pd.notna(last["rsi14"]) else False,
                "rsi_oversold": bool(last["rsi_oversold"]) if pd.notna(last["rsi14"]) else False,
            })
        except Exception:
            pass

    rep = pd.DataFrame(rows).sort_values(["bull_cross","bear_cross","rsi_overbought","rsi_oversold","ticker"], ascending=[False, False, False, False, True])
    outp = f"{BUCKET_ROOT}/meta/reports/{date.today().isoformat()}_signals.csv"
    rep.to_csv(outp, index=False, storage_options=fsopts)
    print(rep.head(30).to_string(index=False))

if __name__ == "__main__":
    main()
