import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
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
        "1d": "5y", 
        "1wk": "max",
        "1mo": "max"
    }
    
    period = period_map.get(interval, "2y")
    
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df.empty or len(df) < 250: return None 

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
    Returns the CURRENT trend status.
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
                cross_price = closes[i]
                break
                
        elif direction == "Downtrend": # Death Cross
            if prev_bb >= prev_ema and curr_bb < curr_ema:
                found = True
                cross_time = dates[i]
                cross_price = closes[i]
                break
                
    return found, cross_time, cross_price

# --- Support & Resistance Logic ---

def find_next_sr_level(ticker, current_tf, direction, current_price):
    """
    Looks at the NEXT higher timeframe to find the nearest S/R.
    """
    tf_order = ["4h", "1d", "1wk", "1mo"]
    
    try:
        curr_idx = tf_order.index(current_tf)
        # If current is 1mo, we just use 1mo again or "max"
        next_tf = tf_order[curr_idx + 1] if curr_idx < len(tf_order)-1 else "1mo"
    except (ValueError, IndexError):
        next_tf = "1mo"

    df = get_data(ticker, next_tf)
    if df is None or len(df) < 50:
        return "Unknown", f"No data for {next_tf}"

    # Identify Pivot Points (Simple 5-bar fractal)
    window = 5
    df['is_high'] = df['high'].rolling(window=window, center=True).max() == df['high']
    df['is_low'] = df['low'].rolling(window=window, center=True).min() == df['low']
    
    pivots_high = df[df['is_high']]['high'].values
    pivots_low = df[df['is_low']]['low'].values

    target_level = None
    note = ""

    if direction == "Uptrend": # Buy -> Look for Resistance
        candidates = [p for p in pivots_high if p > current_price]
        if candidates:
            target_level = min(candidates)
            note = f"Resistance on {next_tf}"
        else:
            note = "ATH (All Time High)"
            
    elif direction == "Downtrend": # Sell -> Look for Support
        candidates = [p for p in pivots_low if p < current_price]
        if candidates:
            target_level = max(candidates)
            note = f"Support on {next_tf}"
        else:
            note = "ATL (All Time Low)"

    if target_level:
        return round(target_level, 4), note
    else:
        return "N/A", note

# --- Core Scanner Logic ---

def analyze_ticker(ticker):
    """
    Revised "Ladder" Logic:
    1. Base 1H Stable? -> Check 4H Cross.
    2. Base 4H Stable? -> Check 1D Cross.
    3. Base 1D Stable? -> Check 1W Cross.
    4. Base 1W Stable? -> Check 1M Cross.
    """
    
    # 1. ESTABLISH INITIAL BASE (1H)
    base_tf = "1h"
    df_base = get_data(ticker, base_tf)
    
    if df_base is None:
        return {"Signal": "No Signal", "Reason": "Data Error (1H)"}
    
    # Check Initial Trend
    trend_base = get_trend_status(df_base)
    if trend_base == "Neutral":
        return {"Signal": "No Signal", "Reason": "1H Trend Neutral"}
    
    current_direction = trend_base # e.g., "Uptrend"
    
    # 2. THE LADDER LOOP
    # We define the steps: (Base TF, Target TF for Entry)
    # logic: if Base is Stable, look at Target.
    steps = [
        ("1h", "4h"), 
        ("4h", "1d"), 
        ("1d", "1wk"), 
        ("1wk", "1mo")
    ]
    
    for base_name, target_name in steps:
        # A. Verify Base Stability
        # (For 1h, we already did it. For others, we need to check if the PREVIOUS target 
        # is actually stable enough to serve as the new base).
        
        if base_name != "1h":
            df_base = get_data(ticker, base_name)
            if df_base is None: return {"Signal": "No Signal", "Reason": f"Data Error ({base_name})"}
            
            status = get_trend_status(df_base)
            
            # CRITICAL: The Base must align with our established direction.
            # If 1H was UP, but 4H is DOWN, we cannot use 4H as a base for Daily.
            if status != current_direction:
                return {"Signal": "No Signal", "Reason": f"Chain Broken at {base_name} ({status} vs {current_direction})"}

        # B. Check Target for Entry (Recent Cross)
        df_target = get_data(ticker, target_name)
        if df_target is None: return {"Signal": "No Signal", "Reason": f"Data Error ({target_name})"}
        
        is_recent, cross_time, cross_price = check_recent_cross(df_target, current_direction)
        
        if is_recent:
            # --- SIGNAL FOUND ---
            current_price = df_target.iloc[-1]['close']
            
            # Stop Loss (1% off Cross Price)
            sl = cross_price * (1 - SL_BUFFER) if current_direction == "Uptrend" else cross_price * (1 + SL_BUFFER)
            
            # Take Profit (Next Higher TF S/R)
            tp_price, tp_note = find_next_sr_level(ticker, target_name, current_direction, current_price)
            
            if isinstance(cross_time, pd.Timestamp):
                cross_time_str = cross_time.strftime('%Y-%m-%d %H:%M')
            else:
                cross_time_str = str(cross_time)
                
            warning_msg = ""
            if "ATH" in tp_note or "ATL" in tp_note:
                warning_msg = f" | WARNING: {tp_note}"

            return {
                "Signal": f"CONFIRMED {current_direction.upper()}",
                "Timeframe": f"Ladder {base_name}->{target_name}",
                "Entry Trigger": f"{target_name} Cross",
                "Cross Time": cross_time_str,
                "Current Price": round(current_price, 4),
                "Stop Loss": round(sl, 4),
                "Take Profit": f"{tp_price} ({tp_note})",
                "Reason": f"Entry on {target_name} (Base: {base_name}){warning_msg}"
            }
        
        # If NO recent cross on Target, we loop.
        # The current 'target' becomes the 'base' for the next iteration.
        # But only if it's stable (which we check at the start of next loop).
        continue

    return {"Signal": "No Signal", "Reason": "All Trends Mature / Chain Completed without Entry"}

def run_scanner(tickers, eco_df=None):
    results = []
    print(f"Scanning {len(tickers)} tickers using Daisy-Chain Ladder Strategy...")
    
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Checking {ticker}...", end="\r")
        
        try:
            analysis = analyze_ticker(ticker)
        except Exception as e:
            analysis = {"Signal": "Error", "Reason": str(e)}

        danger_msg = "-"
        if eco_df is not None:
            danger_msg = check_economic_danger(ticker, eco_df)
        
        remarks = danger_msg
        if "CONFIRMED" in analysis.get("Signal", ""):
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
