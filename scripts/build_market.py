#!/usr/bin/env python3
import json, math, sys, datetime as dt
from pathlib import Path

import pandas as pd
import yfinance as yf

# ---- CONFIG ----
# מיפוי סימבולים שלך -> טיקר ב-Yahoo
SYMBOL_MAP = {
    "MNQ": "NQ=F",   # Micro E-mini Nasdaq-100 -> E-mini proxy
    "MES": "ES=F",   # Micro E-mini S&P 500   -> E-mini proxy
    "MYM": "YM=F",   # Micro Mini Dow         -> Mini proxy
    "M2K": "RTY=F",  # Micro Russell 2000     -> E-mini proxy
    "MGC": "GC=F",   # Micro Gold -> Gold
    "MCL": "CL=F",   # Micro WTI  -> WTI
    # תוכל להוסיף כאן סימבולים נוספים בהמשך
}

# tick size (points per tick) כדי להמיר ATR (ב"נקודות") ל-"טיקים"
TICK_SIZE = {
    "MNQ": 0.25,
    "MES": 0.25,
    "MYM": 1.00,
    "M2K": 0.10,
    "MGC": 0.10,
    "MCL": 0.01,
}

# ערך טיק (USD) – אם תרצה שהטבלה תחשב $ כשאין usd בקובץ
TICK_VALUE = {
    "MNQ": 0.50,
    "MES": 1.25,
    "MYM": 0.50,
    "M2K": 0.50,
    "MGC": 1.00,
    "MCL": 1.00,
}

# כמה נרות למשוך
DAILY_LOOKBACK = "60d"
HOURLY_LOOKBACK = "30d"  # Yahoo מאפשר בד"כ intraday ל~30 יום


def atr14(df: pd.DataFrame) -> float:
    """ATR(14) בנקודות (אותו scale של המחיר)"""
    if df is None or df.empty:
        return float("nan")
    h, l, c = df["High"], df["Low"], df["Close"]
    prev_c = c.shift(1)
    tr = pd.concat([
        (h - l).abs(),
        (h - prev_c).abs(),
        (l - prev_c).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean().iloc[-1]
    return float(atr) if pd.notna(atr) else float("nan")


def pct_and_trend(df: pd.DataFrame):
    """החזר שינוי אחוזי בין שני הנרות האחרונים וכיוון"""
    if df is None or len(df) < 2:
        return None, "flat"
    last = df["Close"].iloc[-1]
    prev = df["Close"].iloc[-2]
    if not (pd.notna(last) and pd.notna(prev) and prev != 0):
        return None, "flat"
    pct = (last - prev) / abs(prev) * 100.0
    trend = "up" if pct > 0.05 else ("down" if pct < -0.05 else "flat")
    return float(pct), trend


def safe_round(x, nd=2):
    try:
        if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
            return None
        return round(float(x), nd)
    except Exception:
        return None


def main():
    out = {"daily": {}, "hourly": {}}
    now = dt.datetime.utcnow().isoformat() + "Z"

    for sym, yahoo in SYMBOL_MAP.items():
        print(f"[build_market] fetching {sym} <- {yahoo}")
        # Daily
        try:
            df_d = yf.Ticker(yahoo).history(period=DAILY_LOOKBACK, interval="1d", auto_adjust=False)
        except Exception as e:
            print("daily fetch error:", sym, e)
            df_d = pd.DataFrame()
        atr_d = atr14(df_d)
        pct_d, trend_d = pct_and_trend(df_d)

        # Hourly
        try:
            df_h = yf.Ticker(yahoo).history(period=HOURLY_LOOKBACK, interval="60m", auto_adjust=False)
        except Exception as e:
            print("hourly fetch error:", sym, e)
            df_h = pd.DataFrame()
        atr_h = atr14(df_h)
        pct_h, trend_h = pct_and_trend(df_h)

        tick_size = TICK_SIZE.get(sym)
        # המרה לטיקים: ATR_points / tick_size
        pips_daily = (atr_d / tick_size) if (tick_size and atr_d and not math.isnan(atr_d)) else None
        pips_hourly = (atr_h / tick_size) if (tick_size and atr_h and not math.isnan(atr_h)) else None

        out["daily"][sym] = {
            "trend": trend_d,
            "pips": safe_round(pips_daily, 2),
            "usd": None,              # הטבלה שלך כבר יודעת לחשב לפי TICK_VALUE
            "pct": safe_round(pct_d, 2)
        }
        out["hourly"][sym] = {
            "trend": trend_h,
            "pips": safe_round(pips_hourly, 2),
            "usd": None,
            "pct": safe_round(pct_h, 2)
        }

    # נשמור גם metadata קטן אם תרצה לבדוק זמן עדכון
    out["_meta"] = {"updated_utc": now, "source": "Yahoo Finance via yfinance", "atr_period": 14}

    Path("market.json").write_text(json.dumps(out, indent=2))
    print("[build_market] wrote market.json with", len(out["daily"]), "symbols")


if __name__ == "__main__":
    sys.exit(main())
