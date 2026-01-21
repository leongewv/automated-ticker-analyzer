import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0

# --- STABILITY SETTING: 3 to 10 bars ---
ENTRY_MIN_BARS = 3   
ENTRY_MAX_BARS = 10  

# Early Exit Reversal Threshold (Lower TF)
EXIT_MIN_BARS = 10

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
        "30m": "1mo", 
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
        
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'] = bbands['BB_MIDDLE'] # 20 SMA
        df['BB_UPPER'] = bbands['BB_UPPER']
        df['BB_LOWER'] = bbands['BB_LOWER']
        
        df.dropna(inplace=True)
        return df
    except Exception:
        return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    if last['BB_MID'] > last['EMA_200']: return "Uptrend"
    elif last['BB_MID'] < last['EMA_200']: return "Downtrend"
    return "Neutral"

# --- Helper for Precise Intersection ---
def calculate_exact_cross(prev_sma, curr_sma, prev_ema, curr_ema):
    denom = (prev_sma - curr_sma) - (prev_ema - curr_ema)
    if denom == 0: return curr_sma 
    numer = (prev_sma * curr_ema) - (curr_sma * prev_ema)
    return numer / denom

def get_bars_since_cross(df, direction):
    limit = 500 
    if len(df) < limit: limit = len(df)
    
    window_df = df.iloc[-limit:]
    bb_mid = window_df['BB_MID'].values
    ema_200 = window_df['EMA_200'].values
    dates = window_df.index
    
    for i in range(len(bb_mid) - 1, 0, -1):
        curr_bb = bb_mid[i]
        curr_ema = ema_200[i]
        prev_bb = bb_mid[i-1]
        prev_ema = ema_200[i-1]
        
        found = False
        if direction == "Uptrend": 
            if prev_bb <= prev_ema and curr_bb > curr_ema: found = True
        elif direction == "Downtrend": 
            if prev_bb >= prev_ema and curr_bb < curr_ema: found = True
            
        if found:
            bars_ago = (len(bb_mid) - 1) - i
            exact_price = calculate_exact_cross(prev_bb, curr_bb, prev_ema, curr_ema)
            return bars_ago, dates[i], exact_price

    return None, None, None

def find_previous_opposing_cross(df, current_direction, entry_price):
    target_type = "Golden" if current_direction == "Downtrend" else "Death"
    
    bb_mid = df['BB_MID'].values
    ema_200 = df['EMA_200'].values
    
    skipped_bad_cross = False 
    
    for i in range(len(bb_mid) - 1, 0, -1):
        curr_bb = bb_mid[i]
        curr_ema = ema_200[i]
        prev_bb = bb_mid[i-1]
        prev_ema = ema_200[i-1]
        
        found = False
        if target_type == "Golden": # Looking for Buy Cross
             if prev_bb <= prev_ema and curr_bb > curr_ema: found = True
        else: # Looking for Sell Cross
             if prev_bb >= prev_ema and curr_bb < curr_ema: found = True

        if found:
            exact_price = calculate_exact_cross(prev_bb, curr_bb, prev_ema, curr_ema)
            
            # Profitability Check
            is_valid_tp = False
            if current_direction == "Uptrend":
                if exact_price > entry_price: is_valid_tp = True
            else:
                if exact_price < entry_price: is_valid_tp = True
            
            if is_valid_tp:
                note = f"Previous {target_type} Cross Level"
                if skipped_bad_cross:
                    note += " (Deep Search)"
                return round(exact_price, 4), note
            else:
                skipped_bad_cross = True
            
    return None, None

def find_next_sr_level(ticker, current_tf, direction, current_price):
    tf_order = ["4h", "1d", "1wk", "1mo"]
    try:
        curr_idx = tf_order.index(current_tf)
        next_tf = tf_order[curr_idx + 1] if curr_idx < len(tf_order)-1 else "1mo"
    except (ValueError, IndexError): next_tf = "1mo"

    df = get_data(ticker, next_tf)
    if df is None or len(df) < 50: return "Unknown", f"No data for {next_tf}"

    window = 5
    df['is_high'] = df['high'].rolling(window=window, center=True).max() == df['high']
    df['is_low'] = df['low'].rolling(window=window, center=True).min() == df['low']
    
    pivots_high = df[df['is_high']]['high'].values
    pivots_low = df[df['is_low']]['low'].values

    target_level = None
    note = ""

    if direction == "Uptrend":
        candidates = [p for p in pivots_high if p > current_price]
        if candidates:
            target_level = min(candidates)
            note = f"Resistance on {next_tf}"
        else: note = "ATH (All Time High)"
            
    elif direction == "Downtrend":
        candidates = [p for p in pivots_low if p < current_price]
        if candidates:
            target_level = max(candidates)
            note = f"Support on {next_tf}"
        else: note = "ATL (All Time Low)"

    return (round(target_level, 4), note) if target_level else ("N/A", note)

def check_early_exit(ticker, signal_tf, trade_direction):
    """
    Checks ALL relevant lower timeframes down to 30m for conflicting trends.
    """
    lower_tf_map = {
        "1mo": ["1wk", "1d", "4h", "1h", "30m"],     
        "1wk": ["1d", "4h", "1h", "30m"],            
        "1d":  ["4h", "1h", "30m"],                  
        "4h":  ["1h", "30m"]                         
    }
    
    check_tfs = lower_tf_map.get(signal_tf, [])
    warnings = []
    
    opposing_direction = "Downtrend" if trade_direction == "Uptrend" else "Uptrend"
    
    for l_tf in check_tfs:
        df = get_data(ticker, l_tf)
        if df is None: continue
        
        status = get_trend_status(df)
        if status == opposing_direction:
            bars_ago, _, _ = get_bars_since_cross(df, opposing_direction)
            if bars_ago is not None and bars_ago >= EXIT_MIN_BARS:
                warnings.append(f"{l_tf} Opposing ({bars_ago} bars)")
    
    if warnings:
        return " | ".join(warnings)
    return "Safe"

# --- Core Scanner Logic ---

def analyze_ticker(ticker):
    log_trace = [] 
    
    # Priority: FRESH signals (Higher or Lower) > EXISTING signals (Lowest TF preferred)
    check_timeframes = ["4h", "1d", "1wk", "1mo"]
    
    # Store the first "Existing" trend we find as a fallback
    fallback_existing_trend = None
    
    for tf_name in check_timeframes:
        df = get_data(ticker, tf_name)
        if df is None: 
            log_trace.append(f"{tf_name}:NoData")
            continue
        
        current_direction = get_trend_status(df)
        if current_direction == "Neutral":
            log_trace.append(f"{tf_name}:Neutral")
            continue
            
        bars_ago, cross_time, cross_price = get_bars_since_cross(df, current_direction)
        
        if bars_ago is not None:
            cross_info = f"{bars_ago} bars ago"
        else:
            cross_info = "Trend Active (No recent cross)"
        log_trace.append(f"{tf_name}:{cross_info}")
        
        # --- PREPARE RESULT OBJECT ---
        current_price = df.iloc[-1]['close']
        sl = cross_price * (1 - SL_BUFFER) if current_direction == "Uptrend" else cross_price * (1 + SL_BUFFER)
        
        tp_price, tp_note = find_previous_opposing_cross(df, current_direction, current_price)
        
        warning_msg = ""
        if tp_note and "Deep Search" in tp_note:
            warning_msg += " | WARNING: Market Choppy - Deep TP Search"
        
        if tp_price is None:
            tp_price, tp_note = find_next_sr_level(ticker, tf_name, current_direction, current_price)
            tp_note = f"{tp_note} (Fallback)"
        
        if "ATH" in tp_note or "ATL" in tp_note:
            warning_msg += f" | WARNING: {tp_note}"

        cross_time_str = str(cross_time)
        if isinstance(cross_time, pd.Timestamp): cross_time_str = cross_time.strftime('%Y-%m-%d %H:%M')
        
        exit_warning = check_early_exit(ticker, tf_name, current_direction)
        
        # --- DECISION LOGIC ---
        is_fresh = (bars_ago is not None and ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS)
        
        result_payload = {
            "Signal": f"CONFIRMED {current_direction.upper()}" if is_fresh else f"EXISTING {current_direction.upper()}",
            "Timeframe": tf_name, 
            "Current Price": round(current_price, 4),
            "Stop Loss": round(sl, 4),
            "Take Profit": f"{tp_price} ({tp_note})",
            "Cross Time": cross_time_str,
            "Exit Warning": exit_warning,
            "Reason": f"{'Entry' if is_fresh else 'Active Trend'} on {tf_name} @ {round(cross_price, 4)}{warning_msg}",
            "Trace": " | ".join(log_trace) 
        }

        # 1. IF FRESH: Return immediately (Highest Priority)
        if is_fresh:
            result_payload["Trace"] = " | ".join(log_trace)
            return result_payload
            
        # 2. IF EXISTING: Store as fallback (only the first/lowest one)
        if fallback_existing_trend is None:
            fallback_existing_trend = result_payload

    # End of Loop
    if fallback_existing_trend:
        fallback_existing_trend["Trace"] = " | ".join(log_trace)
        return fallback_existing_trend

    return {"Signal": "No Signal", "Reason": f"No Active Trend Found [Trace: {' | '.join(log_trace)}]"}

def run_scanner(tickers, eco_df=None):
    results = []
    print(f"Scanning {len(tickers)} tickers using Freshness Priority Strategy...")
    
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Checking {ticker}...", end="\r")
        
        try:
            analysis = analyze_ticker(ticker)
        except Exception as e:
            analysis = {"Signal": "Error", "Reason": str(e)}

        danger_msg = "-"
        if eco_df is not None:
            danger_msg = check_economic_danger(ticker, eco_df)
        
        trace_log = analysis.get("Trace", "")
        
        final_remarks = danger_msg
        if trace_log: 
            final_remarks = f"[Trace: {trace_log}] | {danger_msg}"
        elif "Trace" in analysis.get("Reason", ""):
            final_remarks = f"[{analysis['Reason']}] | {danger_msg}"
        
        if "CONFIRMED" in analysis.get("Signal", "") or "EXISTING" in analysis.get("Signal", ""):
             results.append({
                "Ticker": ticker,
                "Signal": analysis["Signal"],
                "Timeframe": analysis["Timeframe"],
                "Current Price": analysis["Current Price"],
                "Stop Loss": analysis["Stop Loss"],
                "Take Profit": analysis["Take Profit"],
                "Exit Warning": analysis.get("Exit Warning", "-"),
                "Cross Time": analysis["Cross Time"],
                "Remarks": final_remarks
            })
        else:
            results.append({
                "Ticker": ticker,
                "Signal": "No Signal",
                "Timeframe": "-",
                "Current Price": "-",
                "Stop Loss": "-",
                "Take Profit": "-",
                "Exit Warning": "-",
                "Cross Time": "-",
                "Remarks": final_remarks
            })
            
    print("\nScan Complete.")
    return pd.DataFrame(results)
