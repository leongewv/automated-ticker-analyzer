import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0
ENTRY_MIN_BARS = 3   
ENTRY_MAX_BARS = 30  
SL_BUFFER = 0.01
STEEPNESS_THRESHOLD = 0.005 

def get_data(ticker, interval):
    period_map = {"4h": "730d", "1d": "5y", "1wk": "max", "1mo": "max"}
    try:
        df = yf.Ticker(ticker).history(period=period_map.get(interval, "2y"), interval=interval)
        if df.empty or len(df) < 250: return None 
        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'], df['BB_UPPER'], df['BB_LOWER'] = bbands['BB_MIDDLE'], bbands['BB_UPPER'], bbands['BB_LOWER']
        
        # Slope is normalized by price to handle different asset classes
        df['UPPER_SLOPE'] = (df['BB_UPPER'] - df['BB_UPPER'].shift(3)) / df['close']
        df['LOWER_SLOPE'] = (df['BB_LOWER'] - df['BB_LOWER'].shift(3)) / df['close']
        
        df.dropna(inplace=True)
        return df
    except: return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    return "Uptrend" if last['BB_MID'] > last['EMA_200'] else "Downtrend"

def get_bars_since_cross(df):
    bb_mid, ema_200 = df['BB_MID'].values, df['EMA_200'].values
    # Check backwards for the most recent cross
    for i in range(len(bb_mid) - 1, 0, -1):
        if bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]:
            return "Uptrend", (len(bb_mid)-1)-i, df.index[i]
        if bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i]:
            return "Downtrend", (len(bb_mid)-1)-i, df.index[i]
    return None, None, None

def analyze_ticker(ticker):
    tiers = [("4h", "1d"), ("1d", "1wk"), ("1wk", "1mo")]
    tier_logs = []
    
    for signal_tf, context_tf in tiers:
        sig_df = get_data(ticker, signal_tf)
        if sig_df is None:
            tier_logs.append(f"{signal_tf}:NoData")
            continue
        
        cross_type, bars_ago, _ = get_bars_since_cross(sig_df)
        last_bar = sig_df.iloc[-1]
        
        # Log basic findings for this tier
        if cross_type:
            log_entry = f"{signal_tf}:{cross_type[:4]}@{bars_ago}b"
        else:
            log_entry = f"{signal_tf}:NoCross"
            tier_logs.append(log_entry)
            continue

        # Check if the cross is within our window
        if not (ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS):
            tier_logs.append(f"{log_entry}(OutWindow)")
            continue

        ctx_df = get_data(ticker, context_tf)
        if ctx_df is None:
            tier_logs.append(f"{log_entry}(NoCtxData)")
            continue
            
        ctx_trend = get_trend_status(ctx_df)
        
        # --- CASE 1: TREND FOLLOWING ---
        if cross_type == ctx_trend:
            is_moving = last_bar['close'] > sig_df.iloc[-3]['close'] if cross_type == "Uptrend" else last_bar['close'] < sig_df.iloc[-3]['close']
            if is_moving:
                return {
                    "Ticker": ticker, "Signal": f"TREND {cross_type.upper()}",
                    "TF": f"{signal_tf}/{context_tf}", "Status": "Supported",
                    "Price": round(last_bar['close'], 4), "Bars Ago": bars_ago, "Trace": " | ".join(tier_logs)
                }
            else:
                tier_logs.append(f"{log_entry}(FlatPrice)")

        # --- CASE 2: CONTRARIAN ---
        else:
            if cross_type == "Downtrend" and ctx_trend == "Uptrend":
                slope = round(last_bar['LOWER_SLOPE'], 5)
                if last_bar['LOWER_SLOPE'] < -STEEPNESS_THRESHOLD:
                    return {
                        "Ticker": ticker, "Signal": "CONTRARIAN SELL",
                        "TF": f"{signal_tf}/{context_tf}", "Status": "BB Steep Dive",
                        "Price": round(last_bar['close'], 4), "Bars Ago": bars_ago, "Trace": " | ".join(tier_logs)
                    }
                else:
                    tier_logs.append(f"{log_entry}(Slope:{slope})")
            
            elif cross_type == "Uptrend" and ctx_trend == "Downtrend":
                slope = round(last_bar['UPPER_SLOPE'], 5)
                if last_bar['UPPER_SLOPE'] > STEEPNESS_THRESHOLD:
                    return {
                        "Ticker": ticker, "Signal": "CONTRARIAN BUY",
                        "TF": f"{signal_tf}/{context_tf}", "Status": "BB Steep Climb",
                        "Price": round(last_bar['close'], 4), "Bars Ago": bars_ago, "Trace": " | ".join(tier_logs)
                    }
                else:
                    tier_logs.append(f"{log_entry}(Slope:{slope})")
        
    return {"Ticker": ticker, "Signal": "No Signal", "Status": "Checked Tiers", "Trace": " | ".join(tier_logs)}

def run_scanner(tickers):
    results = [analyze_ticker(t) for t in tickers]
    return pd.DataFrame(results)
