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
ENTRY_MAX_BARS = 10  
EXIT_MIN_BARS = 10
SL_BUFFER = 0.01

# --- New Helper for Precise Current Price ---
def get_live_price(ticker):
    """Fetches the most recent price using a 1-minute interval for maximum accuracy."""
    try:
        # We download 1 day of 1-minute data to get the absolute latest tick
        live_data = yf.download(ticker, period="1d", interval="1m", progress=False, group_by='ticker')
        if not live_data.empty:
            # Handle multi-index columns if yf returns them for FX
            if isinstance(live_data.columns, pd.MultiIndex):
                return live_data[ticker]['Close'].iloc[-1]
            return live_data['Close'].iloc[-1]
    except Exception:
        return None
    return None

# --- Economic Danger Logic ---
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

def get_data(ticker, interval):
    period_map = {"30m": "1mo", "1h": "1y", "4h": "2y", "1d": "5y", "1wk": "max", "1mo": "max"}
    period = period_map.get(interval, "2y")
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df.empty or len(df) < 250: return None 
        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'] = bbands['BB_MIDDLE']
        df['BB_UPPER'] = bbands['BB_UPPER']
        df['BB_LOWER'] = bbands['BB_LOWER']
        df.dropna(inplace=True)
        return df
    except Exception: return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    if last['BB_MID'] > last['EMA_200']: return "Uptrend"
    elif last['BB_MID'] < last['EMA_200']: return "Downtrend"
    return "Neutral"

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
        curr_bb, curr_ema, prev_bb, prev_ema = bb_mid[i], ema_200[i], bb_mid[i-1], ema_200[i-1]
        found = False
        if direction == "Uptrend" and prev_bb <= prev_ema and curr_bb > curr_ema: found = True
        elif direction == "Downtrend" and prev_bb >= prev_ema and curr_bb < curr_ema: found = True
        if found:
            bars_ago = (len(bb_mid) - 1) - i
            exact_price = calculate_exact_cross(prev_bb, curr_bb, prev_ema, curr_ema)
            return bars_ago, dates[i], exact_price
    return None, None, None

def find_previous_opposing_cross(df, current_direction, entry_price):
    target_type = "Golden" if current_direction == "Downtrend" else "Death"
    bb_mid, ema_200 = df['BB_MID'].values, df['EMA_200'].values
    skipped_bad_cross = False 
    for i in range(len(bb_mid) - 1, 0, -1):
        curr_bb, curr_ema, prev_bb, prev_ema = bb_mid[i], ema_200[i], bb_mid[i-1], ema_200[i-1]
        found = False
        if target_type == "Golden" and prev_bb <= prev_ema and curr_bb > curr_ema: found = True
        elif target_type == "Death" and prev_bb >= prev_ema and curr_bb < curr_ema: found = True
        if found:
            exact_price = calculate_exact_cross(prev_bb, curr_bb, prev_ema, curr_ema)
            is_valid_tp = (current_direction == "Uptrend" and exact_price > entry_price) or (current_direction == "Downtrend" and exact_price < entry_price)
            if is_valid_tp:
                note = f"Previous {target_type} Cross Level" + (" (Deep Search)" if skipped_bad_cross else "")
                return round(exact_price, 4), note
            skipped_bad_cross = True
    return None, None

def find_next_sr_level(ticker, current_tf, direction, current_price):
    tf_order = ["4h", "1d", "1wk", "1mo"]
    try: next_tf = tf_order[tf_order.index(current_tf) + 1] if tf_order.index(current_tf) < 3 else "1mo"
    except: next_tf = "1mo"
    df = get_data(ticker, next_tf)
    if df is None or len(df) < 50: return "Unknown", f"No data for {next_tf}"
    df['is_high'] = df['high'].rolling(window=5, center=True).max() == df['high']
    df['is_low'] = df['low'].rolling(window=5, center=True).min() == df['low']
    pivots_high, pivots_low = df[df['is_high']]['high'].values, df[df['is_low']]['low'].values
    target_level, note = None, ""
    if direction == "Uptrend":
        candidates = [p for p in pivots_high if p > current_price]
        if candidates: target_level, note = min(candidates), f"Resistance on {next_tf}"
        else: note = "ATH (All Time High)"
    elif direction == "Downtrend":
        candidates = [p for p in pivots_low if p < current_price]
        if candidates: target_level, note = max(candidates), f"Support on {next_tf}"
        else: note = "ATL (All Time Low)"
    return (round(target_level, 4), note) if target_level else ("N/A", note)

def check_early_exit(ticker, signal_tf, trade_direction):
    lower_tf_map = {"1mo": ["1wk", "1d", "4h", "1h", "30m"], "1wk": ["1d", "4h", "1h", "30m"], "1d": ["4h", "1h", "30m"], "4h": ["1h", "30m"]}
    warnings, opposing = [], ("Downtrend" if trade_direction == "Uptrend" else "Uptrend")
    for l_tf in lower_tf_map.get(signal_tf, []):
        df = get_data(ticker, l_tf)
        if df is None: continue
        status = get_trend_status(df)
        if status == opposing:
            bars_ago, _, _ = get_bars_since_cross(df, opposing)
            if bars_ago is not None and bars_ago >= EXIT_MIN_BARS: warnings.append(f"{l_tf} Opposing ({bars_ago} bars)")
    return " | ".join(warnings) if warnings else "Safe"

def analyze_ticker(ticker):
    log_trace, fallback_existing_trend = [], None
    live_price = get_live_price(ticker) # Fetch live price once per ticker
    
    for tf_name in ["4h", "1d", "1wk", "1mo"]:
        df = get_data(ticker, tf_name)
        if df is None:
            log_trace.append(f"{tf_name}:NoData")
            continue
        current_direction = get_trend_status(df)
        if current_direction == "Neutral":
            log_trace.append(f"{tf_name}:Neutral")
            continue
        bars_ago, cross_time, cross_price = get_bars_since_cross(df, current_direction)
        log_trace.append(f"{tf_name}:{f'{bars_ago} bars ago' if bars_ago is not None else 'Active'}")
        
        # Determine display price (Live or fallback to TF close)
        display_price = live_price if live_price is not None else df.iloc[-1]['close']
        sl = cross_price * (1 - SL_BUFFER) if current_direction == "Uptrend" else cross_price * (1 + SL_BUFFER)
        tp_price, tp_note = find_previous_opposing_cross(df, current_direction, display_price)
        
        warning_msg = " | WARNING: Market Choppy" if tp_note and "Deep Search" in tp_note else ""
        if tp_price is None:
            tp_price, tp_note = find_next_sr_level(ticker, tf_name, current_direction, display_price)
            tp_note = f"{tp_note} (Fallback)"
        if "ATH" in tp_note or "ATL" in tp_note: warning_msg += f" | WARNING: {tp_note}"
        
        cross_time_str = cross_time.strftime('%Y-%m-%d %H:%M') if isinstance(cross_time, pd.Timestamp) else str(cross_time)
        is_fresh = (bars_ago is not None and ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS)
        
        result = {
            "Signal": f"CONFIRMED {current_direction.upper()}" if is_fresh else f"EXISTING {current_direction.upper()}",
            "Timeframe": tf_name, 
            "Current Price": round(display_price, 4),
            "Stop Loss": round(sl, 4),
            "Take Profit": f"{tp_price} ({tp_note})",
            "Cross Time": cross_time_str,
            "Exit Warning": check_early_exit(ticker, tf_name, current_direction),
            "Reason": f"{'Entry' if is_fresh else 'Active'} on {tf_name} @ {round(cross_price, 4)}{warning_msg}",
            "Trace": " | ".join(log_trace)
        }
        if is_fresh: return result
        if fallback_existing_trend is None: fallback_existing_trend = result

    return fallback_existing_trend if fallback_existing_trend else {"Signal": "No Signal", "Reason": f"No Active Trend [Trace: {' | '.join(log_trace)}]"}

def run_scanner(tickers, eco_df=None):
    results = []
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Checking {ticker}...", end="\r")
        try: analysis = analyze_ticker(ticker)
        except Exception as e: analysis = {"Signal": "Error", "Reason": str(e)}
        
        danger_msg = check_economic_danger(ticker, eco_df) if eco_df is not None else "-"
        trace = analysis.get("Trace", analysis.get("Reason", ""))
        
        if "CONFIRMED" in analysis.get("Signal", "") or "EXISTING" in analysis.get("Signal", ""):
             results.append({
                "Ticker": ticker, "Signal": analysis["Signal"], "Timeframe": analysis["Timeframe"],
                "Current Price": analysis["Current Price"], "Stop Loss": analysis["Stop Loss"],
                "Take Profit": analysis["Take Profit"], "Exit Warning": analysis.get("Exit Warning", "-"),
                "Cross Time": analysis["Cross Time"], "Remarks": f"[Trace: {trace}] | {danger_msg}"
            })
        else:
            results.append({
                "Ticker": ticker, "Signal": "No Signal", "Timeframe": "-", "Current Price": "-",
                "Stop Loss": "-", "Take Profit": "-", "Exit Warning": "-", "Cross Time": "-",
                "Remarks": f"[{trace}] | {danger_msg}"
            })
    return pd.DataFrame(results)
