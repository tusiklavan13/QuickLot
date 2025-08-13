# scripts/build_history.py
import os, json, math, datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf

# ----- מיפוי הסימבולים שלך -> טיקר ב-Yahoo -----
SYMBOL_MAP = {
    # Micros -> E-mini
    "MES": "ES=F",   # Micro E-mini S&P 500 -> ES
    "MNQ": "NQ=F",   # Micro E-mini Nasdaq 100 -> NQ
    "MYM": "YM=F",   # Micro Mini Dow -> YM
    "M2K": "RTY=F",  # Micro E-mini Russell 2000 -> RTY
    "MGC": "GC=F",   # Micro Gold -> GC
    "MCL": "CL=F",   # Micro WTI -> CL
    "MHG": "HG=F",   # Micro Copper -> HG
    "MNG": "NG=F",   # Micro NatGas -> NG
    "SIL": "SI=F",   # Silver (הוספת סימבול מלא)
    # E-mini / רגילים
    "ES":  "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F",
    "GC": "GC=F", "SI": "SI=F", "HG": "HG=F",
    "CL": "CL=F", "NG": "NG=F", "RB": "RB=F", "HO": "HO=F",
    "PL": "PL=F",
    # Treasuries
    "ZT": "ZT=F", "ZF": "ZF=F", "ZN": "ZN=F", "ZB": "ZB=F", "UB": "UB=F", "TN": "TN=F",
    # Grains
    "ZC": "ZC=F", "ZW": "ZW=F", "ZS": "ZS=F", "ZM": "ZM=F", "ZL": "ZL=F",
    # FX futures (6A וכו')
    "6A": "6A=F", "6B": "6B=F", "6C": "6C=F", "6E": "6E=F",
    "6J": "6J=F", "6S": "6S=F", "6N": "6N=F", "6M": "6M=F",
}

OUTPUT_DIR = "history"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """ATR קלאסי על בסיס High/Low/Close יומיים."""
    df = df.copy()
    # True Range
    prev_close = df["Close"].shift(1)
    tr1 = df["High"] - df["Low"]
    tr2 = (df["High"] - prev_close).abs()
    tr3 = (df["Low"] - prev_close).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # ATR – EMA period
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr

def build_symbol(sym: str, yahoo: str) -> pd.DataFrame:
    # נוריד עד שנתיים אחורה (מספיק כדי להפיק שנה מלאה של ATR)
    df = yf.download(yahoo, period="2y", interval="1d", auto_adjust=False, progress=False)
    if df.empty or {"High","Low","Close"} - set(df.columns):
        raise RuntimeError(f"empty or missing columns for {sym} ({yahoo})")
    df = df.dropna(subset=["High","Low","Close"])
    df["ATR"] = compute_atr(df, period=14)
    # נשמור רק את העמודות הרצויות
    out = df[["ATR"]].dropna().copy()
    out.index = out.index.tz_localize(None) if out.index.tz is not None else out.index
    out.reset_index(inplace=True)
    out.rename(columns={"Date":"date","ATR":"atr"}, inplace=True)
    return out

def main():
    combined = {}  # לשימוש בבניית market-history.json
    errors = []

    for sym, yahoo in SYMBOL_MAP.items():
        try:
            data = build_symbol(sym, yahoo)
            # נשמור CSV
            csv_path = os.path.join(OUTPUT_DIR, f"{sym}.csv")
            data.to_csv(csv_path, index=False)
            # ניקח רק שנה אחורה (365 ימים) ל-json
            data_year = data.tail(365)
            combined[sym] = [[d.strftime("%Y-%m-%d"), round(float(a), 2)]
                             for d, a in zip(pd.to_datetime(data_year["date"]), data_year["atr"])]
            print(f"[OK] {sym} -> {len(data)} rows (saved {csv_path})")
        except Exception as e:
            print(f"[ERR] {sym}: {e}")
            errors.append((sym, str(e)))

    # נכתוב market-history.json בשורש הריפו
    meta = {
        "_meta": {
            "updated_utc": dt.datetime.utcnow().isoformat() + "Z",
            "source": "Yahoo Finance via yfinance",
            "atr_period": 14
        }
    }
    out_json = {**combined, **meta}
    with open("market-history.json", "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False)

    if errors:
        print("\nCompleted with errors in:", ", ".join(s for s,_ in errors))

if __name__ == "__main__":
    main()
