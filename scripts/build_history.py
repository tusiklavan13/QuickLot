# scripts/build_history.py
import os, json, math, time, pathlib
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

# מיוחד: override כששם הטיקר ב-Yahoo לא זהה לשם אצלך.
# לכל השאר נשתמש כברירת-מחדל sym+"=F" (ראה שלב 2).
YF_MAP = {
    # Micros ↔ E-minis / Commodities
    "MES":"ES=F", "MNQ":"NQ=F", "MYM":"YM=F", "M2K":"RTY=F",
    "MGC":"GC=F", "MCL":"CL=F", "MHG":"HG=F", "SIL":"SI=F", "MNG":"NG=F",

    "ES":"ES=F", "NQ":"NQ=F", "YM":"YM=F", "RTY":"RTY=F",
    "GC":"GC=F", "SI":"SI=F", "HG":"HG=F", "CL":"CL=F", "NG":"NG=F",
    "RB":"RB=F", "HO":"HO=F", "PL":"PL=F",

    # Treasuries (Yahoo מחזיק אותם)
    "ZT":"ZT=F", "ZF":"ZF=F", "ZN":"ZN=F", "ZB":"ZB=F", "UB":"UB=F", "TN":"TN=F",

    # Grains
    "ZC":"ZC=F", "ZW":"ZW=F", "ZS":"ZS=F", "ZM":"ZM=F", "ZL":"ZL=F",

    # FX futures (CME 6*)
    "6A":"6A=F", "6B":"6B=F", "6C":"6C=F", "6E":"6E=F",
    "6J":"6J=F", "6S":"6S=F", "6N":"6N=F", "6M":"6M=F",
}

# tick size (בערכים דצימליים). אם לא בטוח – תן None והצג ATR כ"נקודות".
TICK = {
    # indices
    "ES":0.25, "MES":0.25, "NQ":0.25, "MNQ":0.25, "YM":1.0, "MYM":1.0,
    "RTY":0.10, "M2K":0.10,

    # metals/energy
    "GC":0.10, "MGC":0.10, "SI":0.005, "SIL":0.005, "HG":0.0005, "MHG":0.0005,
    "CL":0.01,  "MCL":0.01,  "NG":0.001, "MNG":0.001,
    "RB":0.0001,"HO":0.0001, "PL":0.1,

    # treasuries (קירוב סביר)
    "ZT":0.0078125, "ZF":0.0078125, "ZN":0.015625, "TN":0.015625,
    "ZB":0.03125,  "UB":0.03125,

    # grains
    "ZC":0.0025, "ZW":0.0025, "ZS":0.0025, "ZM":0.1, "ZL":0.0001,

    # FX
    "6A":0.0001,  "6B":0.0001,  "6C":0.00005, "6E":0.00005,
    "6J":0.0000005,"6S":0.0001,  "6N":0.0001,  "6M":0.000005,
}


# גודל טיק להמרת ATR -> "Ticks/Pips" (בערך; העיקר שיהיה עקבי)
TICK = {
    "ES":0.25, "MES":0.25,
    "NQ":0.25, "MNQ":0.25,
    "YM":1.0,  "MYM":1.0,
    "RTY":0.10,"M2K":0.10,
    "GC":0.10, "MGC":0.10,
    "SI":0.005,"SIL":0.005,
    "HG":0.0005,"MHG":0.0005,
    "CL":0.01, "MCL":0.01,
    "NG":0.001,"MNG":0.001,
}

OUT_DIR = pathlib.Path("history")
OUT_DIR.mkdir(parents=True, exist_ok=True)
JSON_PATH = pathlib.Path("market-history.json")

def atr14(df: pd.DataFrame) -> pd.Series:
    """ATR(14) יומי (נקודות), ואז נהפוך ל-Ticks לפי הסימבול."""
    high = df['High']
    low  = df['Low']
    close_prev = df['Close'].shift(1)

    tr = pd.concat([
        (high - low).abs(),
        (high - close_prev).abs(),
        (low - close_prev).abs(),
    ], axis=1).max(axis=1)

    return tr.rolling(14, min_periods=14).mean()

def build_one(sym: str, yf_ticker: str):
    print(f"[build] {sym} <- {yf_ticker}")
    # שנה אחורה ועוד קצת כדי לחשב ATR
    period_days = 400
    df = yf.download(yf_ticker, period=f"{period_days}d", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        print(f"  no data for {sym}")
        return None

    df = df.dropna()
    df['ATR'] = atr14(df)
    # המרה ל-Ticks
    tick = TICK.get(sym, None)
    if tick:
        df['ATR_TICKS'] = (df['ATR'] / tick).round(2)
        values = df['ATR_TICKS'].dropna()
    else:
        values = df['ATR'].dropna()

    # נשמור CSV גולמי
    csv_path = OUT_DIR / f"{sym}.csv"
    out_df = df[['Open','High','Low','Close','ATR','ATR_TICKS']].copy()
    out_df.to_csv(csv_path)

    # JSON לגרפים: רשימת [date, value] אחרונות (נניח 300 נק')
    series = []
    for dt, val in values.tail(300).items():
        if isinstance(dt, (pd.Timestamp, )):
            date_str = dt.strftime("%Y-%m-%d")
        else:
            date_str = str(dt)[:10]
        series.append([date_str, float(val)])

    return series

def main():
    all_series = {}
    for sym, yf_ticker in YF_MAP.items():
        try:
            s = build_one(sym, yf_ticker)
            if s and len(s) >= 2:
                all_series[sym] = s
        except Exception as e:
            print(f"ERROR {sym}: {e}")
            time.sleep(2)

    meta = {
        "_meta": {
            "updated_utc": datetime.now(timezone.utc).isoformat(),
            "source": "Yahoo Finance via yfinance",
            "atr_period": 14,
        }
    }

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({**all_series, **meta}, f, ensure_ascii=False)

    print(f"wrote {JSON_PATH} with {len(all_series)} symbols")
    print(f"csv files in {OUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
