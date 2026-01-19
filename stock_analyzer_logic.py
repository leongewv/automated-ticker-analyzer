import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# --- Configuration ---
EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0

# The window to define a "Recent" cross (10 to 30 bars ago)
CROSS_MIN_BARS = 10
CROSS_MAX_BARS = 30

# Stop Loss Buffer (1%)
SL_BUFFER = 0.01

# --- Economic Danger Logic (Preserved) ---
def check_economic_danger(ticker, eco_df, current_time=None):
    if eco_df is None or eco_df.empty: return "-"
    if current_time is None: current_time = datetime.now()

    clean_ticker = ticker.replace("/", "").replace("-", "").upper().replace("=X", "")
    currencies = [clean_ticker[:3], clean_ticker[3:]] if len(clean_ticker) == 6 else [clean_ticker]

    start_window = current_time
    end_window = current_time + timedelta(hours=24)

    if not pd.api.types.is_datetime64_any_dtype(eco_df['Start']):
        eco_df['Start'] = pd.to_datetime(eco_df['Start'], errors='coerce')

    mask = (
        (eco_df['Start'] >= start_window) & 
        (eco_df['Start'] <= end_window) & 
        (eco_df['Currency'].isin(currencies)) &
        (eco_df['Impact'].isin(['HIGH', 'MEDIUM']))
    )
    
    danger_events = eco_df[mask]
    if danger_events.empty: return "Safe"
    
    warnings = []
    for _, row in danger_events.iterrows():
        time_str = row['Start'].strftime('%H:%M')
        warnings.append(f"{row['Currency']} {row['Name']} ({row['Impact']}) at {time_str}")
    return " | ".join(warnings)

# --- Data & Indicators ---

def get_data(ticker, interval):
    # Mapping interval to data period required
    period_map = {
        "1h": "1y",
        "4h": "2y", 
        "1d": "5y", # Longer period for Daily to find significant S/R
        "1wk": "max",
        "1mo": "max"
    }
    
    period = period_map.get(interval, "2y")
    
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df.empty or len(df) < 100: return None 

        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        
        # Indicators
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'] = bbands['BB_MIDDLE'] # 20 SMA
        df['BB_UPPER'] = bbands['BB_UPPER']
        df['BB_LOWER'] = bbands['BB_LOWER']
        
        df.dropna(inplace=True)
        return df
    except Exception as e:
        return None

def get_trend_status(df):
    """
    Stable Uptrend: 20 SMA > 200 EMA
    Stable Downtrend: 20 SMA < 200 EMA
    """
    if df is None or len(df) < 1: return "None"
    
    last = df.iloc[-1]
    if last['BB_MID'] > last['EMA_200']:
        return "Uptrend"
    elif last['BB_MID'] < last['EMA_200']:
        return "Downtrend"
    return "Neutral"

def check_recent_cross(df, direction):
    """
    Checks if a Golden/Death Cross occurred within [Current-30 to Current-10].
    Returns: (bool found, timestamp cross_time, float cross_price)
    """
    if len(df) < CROSS_MAX_BARS + 5: return False, None, 0.0

    window_df = df.iloc[-(CROSS_MAX_BARS):-(CROSS_MIN_BARS)]
    
    bb_mid = window_df['BB_MID'].values
    ema_200 = window_df['EMA_200'].values
    closes = window_df['close'].values
    dates = window_df.index
    
    found = False
    cross_time = None
    cross_price = 0.0
    
    for i in range(1, len(bb_mid)):
        curr_bb = bb_mid[i]
        curr_ema = ema_200[i]
        prev_bb = bb_mid[i-1]
        prev_ema = ema_200[i-1]
        
        if direction == "Uptrend": # Golden Cross
            if prev_bb <= prev_ema and curr_bb > curr_ema:
                found = True
                cross_time = dates[i]
                cross_price = closes[i] # Price at the moment of cross
                break
                
        elif direction == "Downtrend": # Death Cross
            if prev_bb >= prev_ema and curr_bb < curr_ema:
                found = True
                cross_time = dates[i]
                cross_price = closes[i]
                break
                
    return found, cross_time, cross_price

# --- Support & Resistance Logic (Higher Timeframe) ---

def find_next_sr_level(ticker, current_tf, direction, current_price):
    """
    Looks at the NEXT higher timeframe to find the nearest resistance (for buys)
    or support (for sells).
    """
    # Define Ladder
    tf_order = ["4h", "1d", "1wk", "1mo"]
    
    try:
        curr_idx = tf_order.index(current_tf)
        next_tf = tf_order[curr_idx + 1]
    except (ValueError, IndexError):
        # If current is 1mo or unknown, stick to 1mo for S/R check
        next_tf = "1mo"

    df = get_data(ticker, next_tf)
    if df is None or len(df) < 50:
        return "Unknown", f"No data for {next_tf}"

    # Identify Pivot Points (Simple 5-bar fractal)
    # A high is a pivot if it's higher than 2 bars left and right.
    # A low is a pivot if it's lower than 2 bars left and right.
    
    window = 5
    df['is_high'] = df['high'].rolling(window=window, center=True).max() == df['high']
    df['is_low'] = df['low'].rolling(window=window, center=True).min() == df['low']
    
    pivots_high = df[df['is_high']]['high'].values
    pivots_low = df[df['is_low']]['low'].values

    # Filter for "Next" Level
    target_level = None
    note = ""

    if direction == "Uptrend": # Buy -> Look for Resistance (Highs > Price)
        candidates = [p for p in pivots_high if p > current_price]
        if candidates:
            target_level = min(candidates) # Closest one above
            note = f"Resistance on {next_tf}"
        else:
            note = "ATH (All Time High)" # Warning
            
    elif direction == "Downtrend": # Sell -> Look for Support (Lows < Price)
        candidates = [p for p in pivots_low if p < current_price]
        if candidates:
            target_level = max(candidates) # Closest one below
            note = f"Support on {next_tf}"
        else:
            note = "ATL (All Time Low)" # Warning

    if target_level:
        return round(target_level, 4), note
    else:
        return "N/A", note

# --- Core Scanner Logic ---

def analyze_ticker(ticker):
    # 1. Check 1H Filter
    df_1h = get_data(ticker, "1h")
    if df_1h is None: return {"Signal": "No Signal", "Reason": "Data Error (1H)"}
    
    trend_1h = get_trend_status(df_1h)
    if trend_1h == "Neutral":
        return {"Signal": "No Signal", "Reason": "1H Market Choppy/Flat"}
    
    bias = trend_1h
    
    # 2. Ladder Climb
    ladder = [("4h", "4-Hour"), ("1d", "Daily"), ("1wk", "Weekly"), ("1mo", "Monthly")]
    
    for tf_key, tf_name in ladder:
        df = get_data(ticker, tf_key)
        if df is None: return {"Signal": "No Signal", "Reason": f"Data Error ({tf_name})"}
        
        # Trend Alignment Check
        current_tf_trend = get_trend_status(df)
        if current_tf_trend != bias:
             return {"Signal": "No Signal", "Reason": f"{tf_name} Trend Misaligned ({current_tf_trend} vs 1H {bias})"}

        # Cross Check
        is_recent, cross_time, cross_price = check_recent_cross(df, bias)
        
        if is_recent:
            # --- SIGNAL FOUND ---
            current_price = df.iloc[-1]['close']
            
            # A. Calculate Stop Loss (1% off Cross Price)
            if bias == "Uptrend":
                sl = cross_price * (1 - SL_BUFFER)
            else:
                sl = cross_price * (1 + SL_BUFFER)
            
            # B. Calculate Take Profit (Next Higher TF S/R)
            tp_price, tp_note = find_next_sr_level(ticker, tf_key, bias, current_price)
            
            # Format Time
            if isinstance(cross_time, pd.Timestamp):
                cross_time_str = cross_time.strftime('%Y-%m-%d %H:%M')
            else:
                cross_time_str = str(cross_time)

            # Warning Logic for ATH/ATL
            warning_msg = ""
            if "ATH" in tp_note or "ATL" in tp_note:
                warning_msg = f" | WARNING: Price at {tp_note}. Trade with caution."

            return {
                "Signal": f"CONFIRMED {bias.upper()}",
                "Timeframe": tf_name,
                "Entry Trigger": f"Golden Cross" if bias == "Uptrend" else "Death Cross",
                "Cross Time": cross_time_str,
                "Current Price": round(current_price, 4),
                "Stop Loss": round(sl, 4),
                "Take Profit": f"{tp_price} ({tp_note})",
                "Reason": f"Recent cross on {tf_name}{warning_msg}"
            }
        
        # If not recent, loop continues to next higher TF
        continue

    return {"Signal": "No Signal", "Reason": "Trend Mature / No Recent Cross on any TF"}

def run_scanner(tickers, eco_df=None):
    results = []
    print(f"Scanning {len(tickers)} tickers using Trend Climber Strategy...")
    
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Checking {ticker}...", end="\r")
        
        try:
            analysis = analyze_ticker(ticker)
        except Exception as e:
            analysis = {"Signal": "Error", "Reason": str(e)}

        danger_msg = "-"
        if eco_df is not None:
            danger_msg = check_economic_danger(ticker, eco_df)
        
        # Add Reason/Warning to Remarks if valid signal
        remarks = danger_msg
        if "CONFIRMED" in analysis.get("Signal", ""):
            # Append specific trade warnings (like ATH) to remarks
            if "WARNING" in analysis.get("Reason", ""):
                remarks += analysis["Reason"].split("|")[1]

        if "CONFIRMED" in analysis.get("Signal", ""):
            results.append({
                "Ticker": ticker,
                "Signal": analysis["Signal"],
                "Timeframe": analysis["Timeframe"],
                "Current Price": analysis["Current Price"],
                "Stop Loss": analysis["Stop Loss"],
                "Take Profit": analysis["Take Profit"],
                "Cross Time": analysis["Cross Time"],
                "Remarks": remarks
            })
        else:
            results.append({
                "Ticker": ticker,
                "Signal": "No Signal",
                "Timeframe": "-",
                "Current Price": "-",
                "Stop Loss": "-",
                "Take Profit": "-",
                "Cross Time": "-",
                "Remarks": f"{analysis.get('Reason', 'Unknown')} | {danger_msg}"
            })
            
    print("\nScan Complete.")
    return pd.DataFrame(results)
