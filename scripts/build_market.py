#!/usr/bin/env python3
import json, math, sys, datetime as dt
from pathlib import Path

import pandas as pd
import yfinance as yf

# ---- CONFIG ----
# מיפוי סימבולים שלך -> טיקר ב-Yahoo
SYMBOL_MAP = {
    # Micros → ממופים לחוזה הסטנדרטי (טוב ל-ATR)
    "MES": "ES=F", "MNQ": "NQ=F", "MYM": "YM=F", "M2K": "RTY=F",
    "MGC": "GC=F", "SIL": "SI=F", "MHG": "HG=F", "MCL": "CL=F", "MNG": "NG=F",

    # E-mini / רגילים
    "ES": "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F",
    "GC": "GC=F", "SI": "SI=F", "HG": "HG=F", "CL": "CL=F", "NG": "NG=F",
    "RB": "RB=F", "HO": "HO=F", "PL": "PL=F",

    # Treasuries
    "ZT": "ZT=F", "ZF": "ZF=F", "ZN": "ZN=F", "ZB": "ZB=F", "UB": "UB=F", "TN": "TN=F",

    # Grains
    "ZC": "ZC=F", "ZW": "ZW=F", "ZS": "ZS=F", "ZM": "ZM=F", "ZL": "ZL=F",

    # FX futures
    "6A": "6A=F", "6B": "6B=F", "6C": "6C=F", "6E": "6E=F",
    "6J": "6J=F", "6S": "6S=F", "6N": "6N=F", "6M": "6M=F"
}


# tick size (points per tick) כדי להמיר ATR (ב"נקודות") ל-"טיקים"
TICK_SIZE = {
    # Equity
    "ES": 0.25, "MES": 0.25, "NQ": 0.25, "MNQ": 0.25, "YM": 1.00, "MYM": 1.00,
    "RTY": 0.10, "M2K": 0.10,

    # Metals / Energy
    "GC": 0.10, "MGC": 0.10, "SI": 0.005, "SIL": 0.005, "HG": 0.0005, "MHG": 0.0005,
    "CL": 0.01, "MCL": 0.01, "NG": 0.001, "MNG": 0.001, "RB": 0.0001, "HO": 0.0001, "PL": 0.10,

    # Rates (32nds ו-halves; כאן בשבר נקודה עשרונית של נקודה)
    "ZT": 0.0078125, "ZF": 0.0078125, "ZN": 0.015625,
    "ZB": 0.03125, "UB": 0.03125, "TN": 0.015625,

    # Grains
    "ZC": 0.25, "ZW": 0.25, "ZS": 0.25, "ZM": 0.10, "ZL": 0.01,

    # FX
    "6A": 0.0001, "6B": 0.0001, "6C": 0.00005, "6E": 0.00005,
    "6J": 0.0000005, "6S": 0.0001, "6N": 0.0001, "6M": 0.0001
}

# ערך טיק (USD) – אם תרצה שהטבלה תחשב $ כשאין usd בקובץ
TICK_VALUE = {
    # Equity
    "ES": 12.50, "MES": 1.25, "NQ": 5.00, "MNQ": 0.50, "YM": 5.00, "MYM": 0.50,
    "RTY": 5.00, "M2K": 0.50,

    # Metals / Energy
    "GC": 10.00, "MGC": 1.00, "SI": 25.00, "SIL": 1.00, "HG": 12.50, "MHG": 1.25,
    "CL": 10.00, "MCL": 1.00, "NG": 10.00, "MNG": 1.00, "RB": 4.20, "HO": 4.20, "PL": 5.00,

    # Rates
    "ZT": 15.625, "ZF": 7.8125, "ZN": 15.625, "ZB": 31.25, "UB": 31.25, "TN": 31.25,

    # Grains
    "ZC": 12.50, "ZW": 12.50, "ZS": 12.50, "ZM": 10.00, "ZL": 6.00,

    # FX
    "6A": 6.25, "6B": 6.25, "6C": 10.00, "6E": 12.50,
    "6J": 12.50, "6S": 12.50, "6N": 10.00, "6M": 5.00
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
