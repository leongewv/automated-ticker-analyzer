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

# --- Data & Indicators ---

def get_data(ticker, interval):
    period_map = {
        "1h": "1y",
        "4h": "1y",
        "1d": "2y",
        "1wk": "5y",
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
    Determines if the trend is STABLE based on the most recent completed bar.
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
    Checks if a Golden Cross (Buy) or Death Cross (Sell) occurred within 
    the lookback window [Current-30 to Current-10].
    
    Returns: (bool is_recent, str cross_time)
    """
    if len(df) < CROSS_MAX_BARS + 5: return False, None

    # We check the specific window in history
    # We want the cross to have happened between 10 bars ago and 30 bars ago.
    # Python slicing: df.iloc[-30 : -10]
    
    window_df = df.iloc[-(CROSS_MAX_BARS):-(CROSS_MIN_BARS)]
    
    bb_mid = window_df['BB_MID']
    ema_200 = window_df['EMA_200']
    
    found = False
    cross_time = None
    
    # Convert series to numpy for faster iteration
    bb_arr = bb_mid.values
    ema_arr = ema_200.values
    dates = window_df.index
    
    for i in range(1, len(bb_arr)):
        curr_bb = bb_arr[i]
        curr_ema = ema_arr[i]
        prev_bb = bb_arr[i-1]
        prev_ema = ema_arr[i-1]
        
        if direction == "Uptrend": # Looking for Golden Cross
            if prev_bb <= prev_ema and curr_bb > curr_ema:
                found = True
                cross_time = dates[i]
                break
                
        elif direction == "Downtrend": # Looking for Death Cross
            if prev_bb >= prev_ema and curr_bb < curr_ema:
                found = True
                cross_time = dates[i]
                break
                
    return found, cross_time

def calculate_stop_loss(df, direction):
    """
    Calculates Stop Loss based on the CURRENT bar's Bollinger Bands.
    Buy: 1% below Lower Band
    Sell: 1% above Upper Band
    """
    last = df.iloc[-1]
    
    if direction == "Uptrend": # Long
        sl_level = last['BB_LOWER'] * (1 - SL_BUFFER)
        return round(sl_level, 4)
    elif direction == "Downtrend": # Short
        sl_level = last['BB_UPPER'] * (1 + SL_BUFFER)
        return round(sl_level, 4)
    return 0.0

# --- Core Scanner Logic ---

def analyze_ticker(ticker):
    """
    Implements the "Trend Climber" Strategy:
    1H (Filter) -> 4H -> 1D -> 1W -> 1M (Triggers)
    """
    
    # --- STEP 1: 1H Timeframe (The Gatekeeper) ---
    df_1h = get_data(ticker, "1h")
    if df_1h is None:
        return {"Signal": "No Signal", "Reason": "Data Error (1H)"}
    
    trend_1h = get_trend_status(df_1h)
    
    if trend_1h == "Neutral":
        return {"Signal": "No Signal", "Reason": "1H Market Choppy/Flat"}
    
    # We now have a bias (Uptrend or Downtrend)
    bias = trend_1h
    
    # --- TIME FRAME LADDER ---
    ladder = [
        ("4h", "4-Hour"),
        ("1d", "Daily"),
        ("1wk", "Weekly"),
        ("1mo", "Monthly")
    ]
    
    for tf_key, tf_name in ladder:
        # Fetch Data
        df = get_data(ticker, tf_key)
        
        if df is None:
            return {"Signal": "No Signal", "Reason": f"Data Error ({tf_name})"}
            
        # Check Trend alignment
        current_tf_trend = get_trend_status(df)
        
        if current_tf_trend != bias:
             return {"Signal": "No Signal", "Reason": f"{tf_name} Trend Misaligned ({current_tf_trend} vs 1H {bias})"}

        # Check for RECENT CROSS (10-30 bars ago)
        # --- FIX: Removed the extra variable '_' to match return values ---
        is_recent, cross_time = check_recent_cross(df, bias)
        
        if is_recent:
            # TRIGGER FOUND!
            sl = calculate_stop_loss(df, bias)
            current_price = df.iloc[-1]['close']
            
            if isinstance(cross_time, pd.Timestamp):
                cross_time_str = cross_time.strftime('%Y-%m-%d %H:%M')
            else:
                cross_time_str = str(cross_time)

            return {
                "Signal": f"CONFIRMED {bias.upper()}",
                "Timeframe": tf_name,
                "Entry Trigger": f"Golden Cross" if bias == "Uptrend" else "Death Cross",
                "Cross Time": cross_time_str,
                "Current Price": round(current_price, 4),
                "Stop Loss": sl,
                "Reason": f"Recent cross on {tf_name} (within 10-30 bars)"
            }
        
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

        # Economic Check
        danger_msg = "-"
        if eco_df is not None:
            danger_msg = check_economic_danger(ticker, eco_df)

        if "CONFIRMED" in analysis.get("Signal", ""):
            results.append({
                "Ticker": ticker,
                "Signal": analysis["Signal"],
                "Timeframe": analysis["Timeframe"],
                "Current Price": analysis["Current Price"],
                "Stop Loss": analysis["Stop Loss"],
                "Cross Time": analysis["Cross Time"],
                "Remarks": danger_msg
            })
        else:
            results.append({
                "Ticker": ticker,
                "Signal": "No Signal",
                "Timeframe": "-",
                "Current Price": "-",
                "Stop Loss": "-",
                "Cross Time": "-",
                "Remarks": f"{analysis.get('Reason', 'Unknown')} | {danger_msg}"
            })
            
    print("\nScan Complete.")
    return pd.DataFrame(results)
