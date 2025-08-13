# scripts/build_history.py
# יוצר market-history.json (שנה אחורה) על בסיס דאטה אמיתי מ-Yahoo (yfinance)
import json, math
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path

# מיפוי סימבול האתר -> טיקר ב-Yahoo
YF_MAP = {
    # מיקרו אינדקסים → רציף רגיל
    "MES": "ES=F", "MNQ": "NQ=F", "MYM": "YM=F", "M2K": "RTY=F",
    # מתכות/אנרגיה
    "MGC": "GC=F", "SIL": "SI=F", "MHG": "HG=F",
    "MCL": "CL=F", "MNG": "NG=F",
    # הגדולים
    "ES": "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F",
    "GC": "GC=F", "SI": "SI=F", "HG": "HG=F",
    "CL": "CL=F", "NG": "NG=F", "RB": "RB=F", "HO": "HO=F", "PL": "PL=F",
    # אג״ח (Yahoo תומך בזיהוי עם =F)
    "ZT": "ZT=F", "ZF": "ZF=F", "ZN": "ZN=F", "ZB": "ZB=F", "UB": "UB=F", "TN": "TN=F",
    # חקלאות
    "ZC": "ZC=F", "ZW": "ZW=F", "ZS": "ZS=F", "ZM": "ZM=F", "ZL": "ZL=F",
    # מטבעות (חוזים)
    "6A": "6A=F", "6B": "6B=F", "6C": "6C=F", "6E": "6E=F",
    "6J": "6J=F", "6S": "6S=F", "6N": "6N=F", "6M": "6M=F",
}

# גודל טיק (tick size) כדי להמיר ATR לטיקים (בקירוב; אפשר לעדן בהמשך)
TICK_SIZE = {
    "ES":0.25, "MES":0.25, "NQ":0.25, "MNQ":0.25, "YM":1.0, "MYM":1.0, "RTY":0.1, "M2K":0.1,
    "GC":0.1, "MGC":0.1, "SI":0.005, "SIL":0.005, "HG":0.0005, "MHG":0.0005,
    "CL":0.01, "MCL":0.01, "NG":0.001, "MNG":0.001, "RB":0.0001, "HO":0.0001, "PL":0.1,
    "ZC":0.25, "ZW":0.25, "ZS":0.25, "ZM":0.1, "ZL":0.01,
    "ZT":0.0078125, "ZF":0.0078125, "ZN":0.015625, "ZB":0.03125, "UB":0.03125, "TN":0.015625,
    "6A":0.0001, "6B":0.0001, "6C":0.0001, "6E":0.00005, "6J":0.0000005, "6S":0.0001, "6N":0.0001, "6M":0.0001,
}

ATR_PERIOD = 14
DAYS_BACK = 370  # קצת יותר משנה עבור חגים/סופ״ש
OUT_PATH = Path("market-history.json")

def atr_series(df: pd.DataFrame, period: int) -> pd.Series:
    # df index: Date, columns: ['Open','High','Low','Close']
    high = df['High']; low = df['Low']; close = df['Close']
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    # Wilder's ATR (EMA-like)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr

def fetch_history(symbol: str, yf_ticker: str) -> list[tuple[str, float]]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DAYS_BACK)
    try:
        df = yf.download(yf_ticker, start=start.date(), end=end.date(), interval="1d", auto_adjust=False, progress=False)
        if df.empty: return []
        df = df[['Open','High','Low','Close']].dropna()
        atr = atr_series(df, ATR_PERIOD)
        # שמירה כתאריכים ISO וערכי ATR במחיר (לא בטיקים)
        out = []
        for ts, val in atr.dropna().items():
            date = ts.strftime("%Y-%m-%d")
            out.append([date, round(float(val), 6)])
        return out[-365:]  # שנה אחרונה
    except Exception as e:
        print(f"[WARN] {symbol} -> {yf_ticker}: {e}")
        return []

def main():
    # אם יש market.json – נעדיף את הסימבולים ממנו; אחרת מהמפה
    syms = set(YF_MAP.keys())
    try:
        mj = json.load(open("market.json","r"))
        for k in (mj.get("daily") or {}): syms.add(k)
        for k in (mj.get("hourly") or {}): syms.add(k)
    except Exception:
        pass

    result_price = {}  # ATR במחיר
    for sym in sorted(syms):
        yf_tk = YF_MAP.get(sym)
        if not yf_tk:
            print(f"[SKIP] No Yahoo mapping for {sym}")
            continue
        series = fetch_history(sym, yf_tk)
        if series:
            result_price[sym] = series
        else:
            print(f"[NO DATA] {sym} ({yf_tk})")

    # אופציה: להמיר לטיקים ולשמור קובץ נוסף (אם תרצה בעתיד)
    # כאן נשמור בפורמט המחיר (כמו בדוגמאות שלנו), והפרונט יציג "Pips/Ticks" בהתאם.
    meta = {
        "_meta": {
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "source": "Yahoo Finance via yfinance",
            "atr_period": ATR_PERIOD,
            "unit": "price"  # "price" ולא "ticks"
        }
    }
    out = result_price | meta
    OUT_PATH.write_text(json.dumps(out, indent=2))
    print(f"Wrote {OUT_PATH} with {len(result_price)} symbols.")

if __name__ == "__main__":
    main()
