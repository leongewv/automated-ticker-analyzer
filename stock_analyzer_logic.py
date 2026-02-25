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
STEEPNESS_THRESHOLD = 0.002 
SL_BUFFER = 0.01 # 1% buffer from the cross price

def calculate_exact_cross(prev_sma, curr_sma, prev_ema, curr_ema):
    """Calculates the exact price point where the two lines intersected."""
    denom = (prev_sma - curr_sma) - (prev_ema - curr_ema)
    return (prev_sma * curr_ema - curr_sma * prev_ema) / denom if denom != 0 else curr_sma

def get_data(ticker, interval):
    period_map = {"4h": "730d", "1d": "5y", "1wk": "max", "1mo": "max"}
    try:
        df = yf.Ticker(ticker).history(period=period_map.get(interval, "2y"), interval=interval)
        if df.empty or len(df) < 250: return None 
        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'], df['BB_UPPER'], df['BB_LOWER'] = bbands['BB_MIDDLE'], bbands['BB_UPPER'], bbands['BB_LOWER']
        
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
    for i in range(len(bb_mid) - 1, 0, -1):
        if (bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]) or \
           (bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i]):
            
            direction = "Uptrend" if bb_mid[i] > ema_200[i] else "Downtrend"
            bars_ago = (len(bb_mid)-1)-i
            # Calculate the exact mathematical price of the cross for SL
            cross_price = calculate_exact_cross(bb_mid[i-1], bb_mid[i], ema_200[i-1], ema_200[i])
            
            return direction, bars_ago, cross_price
    return None, None, None

def analyze_ticker(ticker):
    tiers = [("4h", "1d"), ("1d", "1wk"), ("1wk", "1mo")]
    tier_logs = []
    
    for signal_tf, context_tf in tiers:
        sig_df = get_data(ticker, signal_tf)
        if sig_df is None: continue
        
        cross_type, bars_ago, cross_price = get_bars_since_cross(sig_df)
        
        if not cross_type or not (ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS):
            tier_logs.append(f"{signal_tf}:NoMatch")
            continue

        ctx_df = get_data(ticker, context_tf)
        if ctx_df is None: continue
            
        ctx_trend = get_trend_status(ctx_df)
        last_sig = sig_df.iloc[-1]
        last_ctx = ctx_df.iloc[-1]
        
        # Calculate Stop Loss based on cross price and direction
        sl_price = cross_price * (1 - SL_BUFFER) if cross_type == "Uptrend" else cross_price * (1 + SL_BUFFER)

        # Validation Logic
        if cross_type == "Uptrend":
            if last_ctx['UPPER_SLOPE'] > STEEPNESS_THRESHOLD:
                label = "TREND UPTREND" if ctx_trend == "Uptrend" else "CONTRARIAN BUY"
                return {
                    "Ticker": ticker, "Signal": label, "TF": f"{signal_tf}/{context_tf}",
                    "Stop Loss": round(sl_price, 4), "Price": round(last_sig['close'], 4),
                    "Status": f"High TF Expansion ({round(last_ctx['UPPER_SLOPE'], 5)})",
                    "Bars Ago": bars_ago, "Trace": " | ".join(tier_logs)
                }
        elif cross_type == "Downtrend":
            if last_ctx['LOWER_SLOPE'] < -STEEPNESS_THRESHOLD:
                label = "TREND DOWNTREND" if ctx_trend == "Downtrend" else "CONTRARIAN SELL"
                return {
                    "Ticker": ticker, "Signal": label, "TF": f"{signal_tf}/{context_tf}",
                    "Stop Loss": round(sl_price, 4), "Price": round(last_sig['close'], 4),
                    "Status": f"High TF Dive ({round(last_ctx['LOWER_SLOPE'], 5)})",
                    "Bars Ago": bars_ago, "Trace": " | ".join(tier_logs)
                }
        
    return {"Ticker": ticker, "Signal": "No Signal", "Trace": " | ".join(tier_logs)}

def run_scanner(tickers):
    return pd.DataFrame([analyze_ticker(t) for t in tickers])
