import os
from datetime import date
import pandas as pd
import s3fs

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
PROFILE = os.environ.get("AWS_PROFILE")
BUCKET_ROOT = os.environ.get("BUCKET_ROOT", "s3://stock-options-ai-app/ohlc")
TICKERS_FILE = os.environ.get("TICKERS_FILE", "tickers.txt")

fsopts = {"anon": False, "client_kwargs": {"region_name": REGION}}
if PROFILE: fsopts["profile"] = PROFILE

def feat_path(symbol, year):
    return f"{BUCKET_ROOT}/features/exchange=NSE/ticker={symbol}/year={year}/part-000.parquet"

def cur_path(symbol, year):
    return f"{BUCKET_ROOT}/curated/exchange=NSE/ticker={symbol}/year={year}/part-000.parquet"

def rsi(series, n=14):
    delta = series.diff()
    up = (delta.where(delta > 0, 0)).rolling(n).mean()
    down = (-delta.where(delta < 0, 0)).rolling(n).mean()
    rs = up / (down.replace(0, pd.NA))
    return 100 - (100 / (1 + rs))

def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def macd(close):
    ema12 = ema(close, 12)
    ema26 = ema(close, 26)
    line = ema12 - ema26
    signal = ema(line, 9)
    hist = line - signal
    return line, signal, hist

def load_recent(symbol):
    # read current & last year (enough history for indicators)
    yr = date.today().year
    frames = []
    for y in (yr-1, yr):
        p = cur_path(symbol, y)
        try:
            df = pd.read_parquet(p, storage_options=fsopts, columns=["date","open","high","low","close","adjusted_close","volume","ticker","exchange"])
            frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True).drop_duplicates("date").sort_values("date")
    # prefer adjusted_close if present, fall back to close
    df["px"] = df["adjusted_close"].fillna(df["close"])
    return df

def build_for(symbol):
    df = load_recent(symbol)
    if df.empty:
        return 0
    df["sma20"]  = df["px"].rolling(20).mean()
    df["sma50"]  = df["px"].rolling(50).mean()
    df["ema20"]  = ema(df["px"], 20)
    df["rsi14"]  = rsi(df["px"], 14)
    macd_line, macd_sig, macd_hist = macd(df["px"])
    df["macd"] = macd_line
    df["macd_signal"] = macd_sig
    df["macd_hist"] = macd_hist

    # signals (boolean flags)
    df["bull_cross"] = (df["sma20"] > df["sma50"]) & (df["sma20"].shift(1) <= df["sma50"].shift(1))
    df["bear_cross"] = (df["sma20"] < df["sma50"]) & (df["sma20"].shift(1) >= df["sma50"].shift(1))
    df["rsi_overbought"] = df["rsi14"] > 70
    df["rsi_oversold"]   = df["rsi14"] < 30

    keep = ["date","ticker","exchange","px","sma20","sma50","ema20","rsi14","macd","macd_signal","macd_hist","bull_cross","bear_cross","rsi_overbought","rsi_oversold"]
    df = df[keep].copy()

    # write by current year (idempotent merge)
    y = date.today().year
    path = feat_path(symbol, y)
    try:
        ex = pd.read_parquet(path, storage_options=fsopts)
        out = pd.concat([ex, df[df["date"] >= pd.to_datetime(f"{y}-01-01")]], ignore_index=True)
        out = out.drop_duplicates(subset=["date"]).sort_values("date")
    except Exception:
        out = df[df["date"] >= pd.to_datetime(f"{y}-01-01")]
    out.to_parquet(path, index=False, storage_options=fsopts)
    return len(out)

def main():
    with open(TICKERS_FILE) as f:
        tickers = [t.strip() for t in f if t.strip()]
    total = 0; done = 0
    for i, sym in enumerate(tickers, 1):
        try:
            n = build_for(sym); done += 1; total += n
            if i % 10 == 0: print(f"... {i}/{len(tickers)}")
        except Exception as e:
            print(f"[err] {sym}: {e}")
    print(f"[FEATURES DONE] tickers={done} (rows written cumulative ~{total})")

if __name__ == "__main__":
    main()
