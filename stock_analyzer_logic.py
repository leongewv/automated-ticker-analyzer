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
ENTRY_MAX_BARS = 30  # Updated per your request
SL_BUFFER = 0.01
STEEPNESS_THRESHOLD = 0.005 

def get_data(ticker, interval):
    period_map = {"4h": "730d", "1d": "5y", "1wk": "max", "1mo": "max"}
    try:
        # Fetch data with sufficient buffer for 200 EMA
        df = yf.Ticker(ticker).history(period=period_map.get(interval, "2y"), interval=interval)
        if df.empty or len(df) < 250: return None 
        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        
        # Indicators
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'], df['BB_UPPER'], df['BB_LOWER'] = bbands['BB_MIDDLE'], bbands['BB_UPPER'], bbands['BB_LOWER']
        
        # Calculate BB Slope (Steepness) over last 3 bars for Contrarian confirmation
        df['UPPER_SLOPE'] = (df['BB_UPPER'] - df['BB_UPPER'].shift(3)) / df['close']
        df['LOWER_SLOPE'] = (df['BB_LOWER'] - df['BB_LOWER'].shift(3)) / df['close']
        
        df.dropna(inplace=True)
        return df
    except: return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    # Simple trend definition based on BB Mid vs 200 EMA
    return "Uptrend" if last['BB_MID'] > last['EMA_200'] else "Downtrend"

def get_bars_since_cross(df):
    bb_mid, ema_200 = df['BB_MID'].values, df['EMA_200'].values
    for i in range(len(bb_mid) - 1, 0, -1):
        # Golden Cross (Bullish)
        if bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]:
            return "Uptrend", (len(bb_mid)-1)-i, df.index[i]
        # Death Cross (Bearish)
        if bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i]:
            return "Downtrend", (len(bb_mid)-1)-i, df.index[i]
    return None, None, None

def analyze_ticker(ticker):
    # Tiered Analysis Strategy
    # (Signal Timeframe, Higher Context Timeframe)
    tiers = [("4h", "1d"), ("1d", "1wk"), ("1wk", "1mo")]
    
    for signal_tf, context_tf in tiers:
        sig_df = get_data(ticker, signal_tf)
        if sig_df is None: continue
        
        cross_type, bars_ago, cross_time = get_bars_since_cross(sig_df)
        
        # Only process if the cross falls within the 3-30 bar lookback window
        if cross_type and ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS:
            ctx_df = get_data(ticker, context_tf)
            if ctx_df is None: continue
            
            ctx_trend = get_trend_status(ctx_df)
            last_bar = sig_df.iloc[-1]
            
            # --- CASE 1: TREND FOLLOWING ---
            # Signal matches the higher timeframe trend
            if cross_type == ctx_trend:
                # Confirm price is moving in the direction of the trend
                is_moving = last_bar['close'] > sig_df.iloc[-3]['close'] if cross_type == "Uptrend" else last_bar['close'] < sig_df.iloc[-3]['close']
                
                if is_moving:
                    return {
                        "Ticker": ticker, 
                        "Signal": f"TREND {cross_type.upper()}",
                        "TF": f"{signal_tf}/{context_tf}", 
                        "Status": "Supported by Context",
                        "Price": round(last_bar['close'], 4), 
                        "Bars Ago": bars_ago
                    }

            # --- CASE 2: CONTRARIAN ---
            # Signal opposes the higher timeframe trend (Potential Reversal)
            else:
                # Bearish Contrarian: Death Cross on Entry TF, but Context is Uptrend
                if cross_type == "Downtrend" and ctx_trend == "Uptrend":
                    if last_bar['LOWER_SLOPE'] < -STEEPNESS_THRESHOLD:
                        return {
                            "Ticker": ticker, 
                            "Signal": "CONTRARIAN SELL",
                            "TF": f"{signal_tf}/{context_tf}", 
                            "Status": "BB Steep Dive",
                            "Price": round(last_bar['close'], 4), 
                            "Bars Ago": bars_ago
                        }
                
                # Bullish Contrarian: Golden Cross on Entry TF, but Context is Downtrend
                elif cross_type == "Uptrend" and ctx_trend == "Downtrend":
                    if last_bar['UPPER_SLOPE'] > STEEPNESS_THRESHOLD:
                        return {
                            "Ticker": ticker, 
                            "Signal": "CONTRARIAN BUY",
                            "TF": f"{signal_tf}/{context_tf}", 
                            "Status": "BB Steep Climb",
                            "Price": round(last_bar['close'], 4), 
                            "Bars Ago": bars_ago
                        }
        
        # If no signal found in the current tier, loop automatically proceeds to next higher pair
    
    return {"Ticker": ticker, "Signal": "No Signal", "Status": "Checked 4H, 1D, 1W"}

def run_scanner(tickers):
    results = []
    for t in tickers:
        try:
            results.append(analyze_ticker(t))
        except Exception as e:
            results.append({"Ticker": t, "Signal": "Error", "Status": str(e)})
    return pd.DataFrame(results)
