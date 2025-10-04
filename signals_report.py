import os
from datetime import date
import pandas as pd
import s3fs

REGION  = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
PROFILE = os.getenv("AWS_PROFILE")
ROOT    = os.getenv("BUCKET_ROOT", "s3://stock-options-ai-app/ohlc")
TFILE   = os.getenv("TICKERS_FILE", "tickers.txt")

S3OPTS = {"anon": False, "client_kwargs": {"region_name": REGION}}
if PROFILE:
    S3OPTS["profile"] = PROFILE
fs = s3fs.S3FileSystem(**S3OPTS)

def feat_path(sym, y): return f"{ROOT}/features/exchange=NSE/ticker={sym}/year={y}/part-000.parquet"

def load_last_row(sym: str):
    """Return last row dict from features (try current year then previous)."""
    y = date.today().year
    for yr in (y, y - 1):
        p = feat_path(sym, yr)
        if fs.exists(p):
            df = pd.read_parquet(p, storage_options=S3OPTS)
            if df.empty:
                continue
            df = df.sort_values("date")
            # Ensure optional columns exist or compute them
            if "rsi14" in df.columns:
                if "rsi_overbought" not in df.columns:
                    df["rsi_overbought"] = df["rsi14"] > 70
                if "rsi_oversold" not in df.columns:
                    df["rsi_oversold"] = df["rsi14"] < 30
            for c, default in [
                ("bull_cross", False),
                ("bear_cross", False),
                ("rsi_overbought", False),
                ("rsi_oversold", False),
            ]:
                if c not in df.columns:
                    df[c] = default
            return df.iloc[-1].to_dict()
    return None

def main():
    with open(TFILE) as f:
        tickers = [t.strip() for t in f if t.strip()]

    rows = []
    for sym in tickers:
        try:
            last = load_last_row(sym)
            if not last:
                continue
            rows.append({
                "ticker": sym,
                "date": str(last.get("date"))[:10],
                "px": float(last.get("px")) if pd.notna(last.get("px")) else None,
                "bull_cross": bool(last.get("bull_cross", False)),
                "bear_cross": bool(last.get("bear_cross", False)),
                "rsi14": float(last.get("rsi14")) if last.get("rsi14") is not None and pd.notna(last.get("rsi14")) else None,
                "rsi_overbought": bool(last.get("rsi_overbought", False)),
                "rsi_oversold": bool(last.get("rsi_oversold", False)),
            })
        except Exception as e:
            print(f"[warn] {sym}: {e}")

    cols = ["ticker", "date", "px", "bull_cross", "bear_cross", "rsi14", "rsi_overbought", "rsi_oversold"]
    rep = pd.DataFrame(rows, columns=cols)

    if not rep.empty:
        rep = rep.sort_values(
            ["bull_cross", "bear_cross", "rsi_overbought", "rsi_oversold", "ticker"],
            ascending=[False, False, False, False, True],
        )

    outp = f"{ROOT}/meta/reports/{date.today().isoformat()}_signals.csv"
    rep.to_csv(outp, index=False, storage_options=S3OPTS)
    print(f"[signals] rows={len(rep)} written: {outp}")
    if not rep.empty:
        print(rep.head(30).to_string(index=False))

if __name__ == "__main__":
    main()
