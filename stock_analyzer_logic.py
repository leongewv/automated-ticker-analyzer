import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0

# --- UPDATE: Lowered Minimum bars to 3 to catch fresher signals (like AUDCAD Weekly) ---
ENTRY_MIN_BARS = 3   # Previously 10
ENTRY_MAX_BARS = 30  # Stays at 30

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

def get_bars_since_cross(df, direction):
    """
    Scans the ENTIRE dataframe (or last 500 bars) to find the MOST RECENT cross.
    Returns: 
        bars_ago (int): Number of bars since cross (0 if just crossed).
        cross_time (str): Timestamp of cross.
        cross_price (float): Close price at cross.
    If no cross found, returns (None, None, None).
    """
    limit = 500 
    if len(df) < limit: limit = len(df)
    
    window_df = df.iloc[-limit:]
    bb_mid = window_df['BB_MID'].values
    ema_200 = window_df['EMA_200'].values
    closes = window_df['close'].values
    dates = window_df.index
    
    # Iterate backwards to find the most recent cross
    for i in range(len(bb_mid) - 1, 0, -1):
        curr_bb = bb_mid[i]
        curr_ema = ema_200[i]
        prev_bb = bb_mid[i-1]
        prev_ema = ema_200[i-1]
        
        found = False
        if direction == "Uptrend": # Golden Cross
            if prev_bb <= prev_ema and curr_bb > curr_ema: found = True
        elif direction == "Downtrend": # Death Cross
            if prev_bb >= prev_ema and curr_bb < curr_ema: found = True
            
        if found:
            bars_ago = (len(bb_mid) - 1) - i
            return bars_ago, dates[i], closes[i]

    return None, None, None

# --- Support & Resistance Logic ---

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

# --- Core Scanner Logic ---

def analyze_ticker(ticker):
    log_trace = [] 
    
    # 1. Base 1H
    df_1h = get_data(ticker, "1h")
    if df_1h is None: return {"Signal": "No Signal", "Reason": "Data Error (1H)"}
    
    trend_1h = get_trend_status(df_1h)
    log_trace.append(f"1H:{trend_1h}")
    
    if trend_1h == "Neutral":
        return {"Signal": "No Signal", "Reason": f"1H Neutral [Trace: {' | '.join(log_trace)}]"}
    
    current_direction = trend_1h
    
    # 2. Ladder
    steps = [("1h", "4h"), ("4h", "1d"), ("1d", "1wk"), ("1wk", "1mo")]
    
    for base_name, target_name in steps:
        # Check Base
        if base_name != "1h":
            df_base = get_data(ticker, base_name)
            if df_base is None: return {"Signal": "No Signal", "Reason": f"Data Error {base_name}"}
            status = get_trend_status(df_base)
            
            if status != current_direction:
                log_trace.append(f"{base_name}:MISALIGNED({status})")
                return {"Signal": "No Signal", "Reason": f"Trace: {' | '.join(log_trace)}"}

        # Check Target
        df_target = get_data(ticker, target_name)
        if df_target is None: return {"Signal": "No Signal", "Reason": f"Data Error {target_name}"}
        
        bars_ago, cross_time, cross_price = get_bars_since_cross(df_target, current_direction)
        
        cross_info = f"{bars_ago} bars ago" if bars_ago is not None else "No Cross"
        log_trace.append(f"{target_name}:{cross_info}")
        
        # Valid Entry Check (Updated Thresholds)
        if bars_ago is not None and ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS:
            # TRIGGER
            current_price = df_target.iloc[-1]['close']
            sl = cross_price * (1 - SL_BUFFER) if current_direction == "Uptrend" else cross_price * (1 + SL_BUFFER)
            tp_price, tp_note = find_next_sr_level(ticker, target_name, current_direction, current_price)
            
            cross_time_str = str(cross_time)
            if isinstance(cross_time, pd.Timestamp): cross_time_str = cross_time.strftime('%Y-%m-%d %H:%M')
            
            warning = f" | WARNING: {tp_note}" if "ATH" in tp_note or "ATL" in tp_note else ""
            
            return {
                "Signal": f"CONFIRMED {current_direction.upper()}",
                "Timeframe": f"Ladder {base_name}->{target_name}",
                "Current Price": round(current_price, 4),
                "Stop Loss": round(sl, 4),
                "Take Profit": f"{tp_price} ({tp_note})",
                "Cross Time": cross_time_str,
                "Reason": f"Entry on {target_name}{warning}",
                "Trace": " | ".join(log_trace)
            }
        
        # Continue Climbing
        continue

    return {"Signal": "No Signal", "Reason": f"Trends Mature/No Entry [Trace: {' | '.join(log_trace)}]"}

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
        
        trace_log = analysis.get("Trace", "")
        
        final_remarks = danger_msg
        if trace_log: 
            final_remarks = f"[Trace: {trace_log}] | {danger_msg}"
        elif "Trace" in analysis.get("Reason", ""):
            final_remarks = f"[{analysis['Reason']}] | {danger_msg}"
            
        if "CONFIRMED" in analysis.get("Signal", ""):
             results.append({
                "Ticker": ticker,
                "Signal": analysis["Signal"],
                "Timeframe": analysis["Timeframe"],
                "Current Price": analysis["Current Price"],
                "Stop Loss": analysis["Stop Loss"],
                "Take Profit": analysis["Take Profit"],
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
                "Cross Time": "-",
                "Remarks": final_remarks
            })
            
    print("\nScan Complete.")
    return pd.DataFrame(results)
