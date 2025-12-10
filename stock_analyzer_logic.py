import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# --- Configuration ---
SLOPE_LOOKBACK_SLOW = 20  
SLOPE_LOOKBACK_FAST = 8   

EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0
MEAN_REV_TOLERANCE_MAX = 0.03 
TREND_FLIP_MIN = 0.02
RETEST_TOLERANCE = 0.015

# --- NEW: Economic Danger Logic ---
def check_economic_danger(ticker, eco_df, current_time=None):
    """
    Checks if there are High/Medium impact economic events for the ticker's currencies
    within the next 24 hours.
    """
    if eco_df is None or eco_df.empty: return "-"
    if current_time is None: current_time = datetime.now()

    # Clean ticker for currency matching (e.g. "GBPUSD=X" -> "GBP", "USD")
    clean_ticker = ticker.replace("/", "").replace("-", "").upper().replace("=X", "")
    currencies = [clean_ticker[:3], clean_ticker[3:]] if len(clean_ticker) == 6 else [clean_ticker]

    # Define Danger Window (Next 24 Hours)
    start_window = current_time
    end_window = current_time + timedelta(hours=24)

    # Ensure 'Start' is datetime
    if not pd.api.types.is_datetime64_any_dtype(eco_df['Start']):
        eco_df['Start'] = pd.to_datetime(eco_df['Start'], errors='coerce')

    # Filter Events
    mask = (
        (eco_df['Start'] >= start_window) & 
        (eco_df['Start'] <= end_window) & 
        (eco_df['Currency'].isin(currencies)) &
        (eco_df['Impact'].isin(['HIGH', 'MEDIUM']))
    )
    
    danger_events = eco_df[mask]
    if danger_events.empty: return "Safe"
    
    # Format Warning
    warnings = []
    for _, row in danger_events.iterrows():
        time_str = row['Start'].strftime('%H:%M')
        warnings.append(f"{row['Currency']} {row['Name']} ({row['Impact']}) at {time_str}")
    return " | ".join(warnings)

# --- Technical Analysis Helper Functions ---

def get_data(ticker, period="2y", interval="1d"):
    if interval == "1h": period = "1y" 
    elif interval == "4h": period = "1y"
    
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        if df.empty: return None
        
        # Calculate Indicators
        df['EMA_200'] = TA.EMA(df, EMA_PERIOD)
        bb = TA.BBANDS(df, BB_PERIOD, BB_MULTIPLIER)
        df['BBM_20'] = bb['BB_MIDDLE']
        df['BB_UP'] = bb['BB_UPPER']
        df['BB_LOW'] = bb['BB_LOWER']
        
        # Bandwidth for Squeeze
        df['BB_WIDTH'] = (df['BB_UP'] - df['BB_LOW']) / df['BBM_20']
        
        return df
    except Exception as e:
        print(f"Error fetching {ticker} ({interval}): {e}")
        return None

def get_slope(series, lookback):
    if len(series) < lookback: return 0
    y = series[-lookback:]
    x = np.arange(len(y))
    slope, _ = np.polyfit(x, y, 1)
    return slope

def check_slope_transition(series, index, lookback, suffix=""):
    """
    Detects if slope changed sign recently.
    Returns: transition_type ("Neg->Pos" or "Pos->Neg"), signal_text, time, price
    """
    if len(series) < lookback + 5: return None, None, None, None
    
    curr_slope = get_slope(series, lookback)
    prev_slope = get_slope(series[:-1], lookback) # Slope 1 candle ago
    
    # Check for Flip UP (Buy)
    if prev_slope < 0 and curr_slope > 0:
        return "Neg->Pos", f"Slope Flip {suffix}", index[-1], series.iloc[-1]
    
    # Check for Flip DOWN (Sell)
    if prev_slope > 0 and curr_slope < 0:
        return "Pos->Neg", f"Slope Flip {suffix}", index[-1], series.iloc[-1]
        
    return None, None, None, None

def check_retest_validity(df, lookback, direction):
    """
    Checks if price retested the 200 EMA recently after a crossover.
    """
    # Simplified Logic: Check if Low touched EMA 200 recently
    recent = df.iloc[-10:]
    ema = recent['EMA_200']
    
    if direction == "Buy":
        # Price should be above EMA, but Low dipped near it
        min_dist = ((recent['Low'] - ema) / ema).min()
        if abs(min_dist) <= RETEST_TOLERANCE:
            return True, recent['Low'].min(), str(recent.index[0].date())

    elif direction == "Sell":
        # Price should be below EMA, but High rallied near it
        min_dist = ((ema - recent['High']) / ema).min()
        if abs(min_dist) <= RETEST_TOLERANCE:
            return True, recent['High'].max(), str(recent.index[0].date())
            
    return False, 0, "-"

def check_crossover(df):
    """
    Checks for Golden/Death Cross (SMA 50 crossing SMA 200)
    """
    if len(df) < 200: return None, None
    
    df['SMA_50'] = TA.SMA(df, 50)
    df['SMA_200'] = TA.SMA(df, 200)
    
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    
    if prev['SMA_50'] < prev['SMA_200'] and curr['SMA_50'] > curr['SMA_200']:
        return "Bullish Cross", str(curr.name)
    
    if prev['SMA_50'] > prev['SMA_200'] and curr['SMA_50'] < curr['SMA_200']:
        return "Bearish Cross", str(curr.name)
        
    return None, None

# --- Main Analysis Logic ---

def analyze_lower_timeframes(ticker, daily_dir):
    timeframes = ["4h", "1h"]
    confirmations = []
    time_logs = []
    failure_details = [] 
    
    for tf in timeframes:
        df = get_data(ticker, period="1y", interval=tf)
        if df is None: 
            failure_details.append(f"{tf}: No Data")
            continue
        
        last = df.iloc[-1]
        bbm = df['BBM_20']
        
        # Calculate Zone Distance for TF
        dist_pct_tf = abs(last['BBM_20'] - last['EMA_200']) / last['EMA_200']
        is_in_zone_tf = dist_pct_tf <= MEAN_REV_TOLERANCE_MAX
        
        # Calculate Transitions
        trans_slow, sig_text_slow, time_slow, price_slow = check_slope_transition(bbm, df.index, SLOPE_LOOKBACK_SLOW, "(Slow)")
        trans_fast, sig_text_fast, time_fast, price_fast = check_slope_transition(bbm, df.index, SLOPE_LOOKBACK_FAST, "(Fast)")
        
        cross_sig, cross_time = check_crossover(df)
        is_above = last['BBM_20'] > last['EMA_200']
        
        tf_notes = []
        tf_time = "-"
        is_valid_tf = False
        
        # Prioritize Fast Transition if available, else Slow
        active_trans = None
        if trans_fast:
            active_trans, active_trans_sig, active_trans_time, active_trans_price = trans_fast, sig_text_fast, time_fast, price_fast
        if trans_slow:
            active_trans, active_trans_sig, active_trans_time, active_trans_price = trans_slow, sig_text_slow, time_slow, price_slow

        current_slope_fast = get_slope(bbm, SLOPE_LOOKBACK_FAST)
        current_slope_slow = get_slope(bbm, SLOPE_LOOKBACK_SLOW)
        
        if daily_dir == "Buy":
            if is_above:
                # 1. Flip Check
                if active_trans == "Neg->Pos":
                    tf_notes.append(active_trans_sig)
                    tf_time = f"{active_trans_time} @ {active_trans_price:.2f}"
                    is_valid_tf = True
                
                # 2. Dip Check (Must be in Zone)
                elif is_in_zone_tf and (current_slope_slow < 0 or current_slope_fast < 0):
                    tf_notes.append("Dip (Slope Neg)")
                    tf_time = "Pending Turn (Up)"
                    is_valid_tf = True
                    
                # 3. Retest Check
                elif current_slope_fast > 0 or current_slope_slow > 0:
                    retest_ok, retest_price, flip_time = check_retest_validity(df, SLOPE_LOOKBACK_SLOW, "Buy")
                    if not retest_ok: retest_ok, retest_price, flip_time = check_retest_validity(df, SLOPE_LOOKBACK_FAST, "Buy")
                    if retest_ok:
                        tf_notes.append("Trend Up (Retest Confirmed)")
                        tf_time = f"Retest @ {retest_price:.2f} (Flip: {flip_time})"
                        is_valid_tf = True
                    else:
                        failure_details.append(f"{tf}: Momentum Up but No Retest/Flip found")

                # 4. Cross Check
                if cross_sig == "Bullish Cross":
                    tf_notes.append("GOLDEN CROSS")
                    tf_time = cross_time
                    is_valid_tf = True
            
            else:
                failure_details.append(f"{tf}: Misaligned")

        elif daily_dir == "Sell":
            if not is_above:
                # 1. Flip Check
                if active_trans == "Pos->Neg":
                    tf_notes.append(active_trans_sig)
                    tf_time = f"{active_trans_time} @ {active_trans_price:.2f}"
                    is_valid_tf = True
                
                # 2. Rally Check (Must be in Zone)
                elif is_in_zone_tf and (current_slope_slow > 0 or current_slope_fast > 0):
                    tf_notes.append("Rally (Slope Pos)")
                    tf_time = "Pending Turn (Down)"
                    is_valid_tf = True

                # 3. Retest Check
                elif current_slope_fast < 0 or current_slope_slow < 0:
                    retest_ok, retest_price, flip_time = check_retest_validity(df, SLOPE_LOOKBACK_SLOW, "Sell")
                    if not retest_ok: retest_ok, retest_price, flip_time = check_retest_validity(df, SLOPE_LOOKBACK_FAST, "Sell")
                    if retest_ok:
                        tf_notes.append("Trend Down (Retest Confirmed)")
                        tf_time = f"Retest @ {retest_price:.2f} (Flip: {flip_time})"
                        is_valid_tf = True
                    else:
                        failure_details.append(f"{tf}: Momentum Down but No Retest/Flip found")
                
                # 4. Cross Check
                if cross_sig == "Bearish Cross":
                    tf_notes.append("DEATH CROSS")
                    tf_time = cross_time
            else:
                 failure_details.append(f"{tf}: Misaligned")

        if is_valid_tf:
            confirmations.append(f"{tf}: {' + '.join(tf_notes)}")
            time_logs.append(f"{tf}: {tf_time}")
            
    return confirmations, time_logs, failure_details

def analyze_market_structure(tickers, eco_df):
    results = []
    print(f"Analyzing {len(tickers)} tickers...")
    
    for ticker in tickers:
        print(f"Processing {ticker}...")
        df = get_data(ticker)
        
        # Economic Danger Check
        danger_msg = check_economic_danger(ticker, eco_df)
        
        if df is None:
            results.append({
                "Ticker": ticker, "Signal": "No Signal", 
                "Current Price": "-", "Daily Setup": "None", 
                "Failure Reason": "Data Fetch Error",
                "Confirmations": "-", "Switch Time": "-", "Est. Price": "-",
                "Remarks": danger_msg
            })
            continue
            
        # Daily Analysis
        last = df.iloc[-1]
        ema = last['EMA_200']
        bbm = last['BBM_20']
        
        # 1. Squeeze Check
        is_squeeze = last['BB_WIDTH'] < 0.10 # 10% Bandwidth threshold
        
        # 2. Zone Check (Price vs EMA)
        dist_pct = abs(bbm - ema) / ema
        is_in_zone = dist_pct <= MEAN_REV_TOLERANCE_MAX
        
        # 3. Daily Direction Logic
        daily = {"direction": None, "setup_type": None, "is_squeeze": is_squeeze, "current_close": last['Close'], "bbm_price": bbm}
        failure_reason = "None"
        
        slope_slow = get_slope(df['BBM_20'], SLOPE_LOOKBACK_SLOW)
        
        # --- BUY LOGIC ---
        if bbm > ema: 
            if is_in_zone:
                # Setup A: Flip Buy (Reversal)
                if slope_slow > 0:
                     # Check if it *just* flipped or if it's established
                     trans, _, _, _ = check_slope_transition(df['BBM_20'], df.index, SLOPE_LOOKBACK_SLOW)
                     if trans == "Neg->Pos":
                         daily["direction"] = "Buy"
                         daily["setup_type"] = "Mean Rev (Flip Buy - Fresh)"
                     else:
                         # Established uptrend, treating as continuation
                         daily["direction"] = "Buy"
                         daily["setup_type"] = "Mean Rev (Dip Buy)"
                
                # Setup B: Anticipation (Slope still negative but in zone)
                elif slope_slow < 0:
                    daily["direction"] = "Buy" # Provisional, needs lower TF confirmation
                    daily["setup_type"] = "Mean Rev (Flip Buy - Slow)"
            else:
                 failure_reason = f"Not in Zone (Dist: {dist_pct*100:.2f}%, Price: {bbm:.2f})"
        
        # --- SELL LOGIC ---
        elif bbm < ema:
            if is_in_zone:
                if slope_slow < 0:
                    trans, _, _, _ = check_slope_transition(df['BBM_20'], df.index, SLOPE_LOOKBACK_SLOW)
                    if trans == "Pos->Neg":
                        daily["direction"] = "Sell"
                        daily["setup_type"] = "Mean Rev (Flip Sell - Fresh)"
                    else:
                        daily["direction"] = "Sell"
                        daily["setup_type"] = "Mean Rev (Rally Sell)"
                
                elif slope_slow > 0:
                    daily["direction"] = "Sell"
                    daily["setup_type"] = "Mean Rev (Flip Sell - Slow)"
            else:
                failure_reason = f"Not in Zone (Dist: {dist_pct*100:.2f}%, Price: {bbm:.2f})"
                
        # --- FILTER INVALID DAILY SETUPS ---
        if not daily['direction']:
             results.append({
                "Ticker": ticker, "Signal": "No Signal", "Current Price": "-", 
                "Daily Setup": "None", "Failure Reason": failure_reason,
                "Confirmations": "-", "Switch Time": "-", "Est. Price": "-",
                "Remarks": danger_msg
            })
             continue

        # Check for Trend Continuation Validity (Prevent buying top of rally)
        # If Slope is already UP, ensure we didn't just miss the move.
        if "Dip Buy" in daily['setup_type'] and slope_slow > TREND_FLIP_MIN: 
             # Logic: If slope is steep positive, only take if price < BBM (Pullback)
             if last['Close'] > bbm * 1.02: # 2% above mean
                 daily['direction'] = None
                 failure_reason = "Setup Invalid: Slope Invalid (Already Up - Overextended)"
        
        if "Rally Sell" in daily['setup_type'] and slope_slow < -TREND_FLIP_MIN:
             if last['Close'] < bbm * 0.98:
                 daily['direction'] = None
                 failure_reason = "Setup Invalid: Slope Invalid (Already Down - Missed Rally)"

        # Re-check validity after filters
        if not daily['direction']:
             results.append({
                "Ticker": ticker, "Signal": "No Signal", "Current Price": "-", 
                "Daily Setup": "None", "Failure Reason": failure_reason,
                "Confirmations": "-", "Switch Time": "-", "Est. Price": "-",
                "Remarks": danger_msg
            })
             continue
        
        time.sleep(1) 
        confs, times, fail_details = analyze_lower_timeframes(ticker, daily['direction'])
        curr_price = daily['current_close'] 
        
        if confs:
            labels = []
            if "Squeeze" not in daily['setup_type'] and daily['is_squeeze']: labels.append("Squeeze")
            labels.append(daily['setup_type'])
            final_setup = " + ".join(labels)
            
            full_notes = " | ".join(confs)
            time_notes = " | ".join(times)
            signal_type = "SUPER" if "CROSS" in full_notes else "Standard"
            
            results.append({
                "Ticker": ticker, "Signal": f"{signal_type} {daily['direction']}",
                "Current Price": curr_price, "Daily Setup": final_setup,
                "Failure Reason": "None", "Confirmations": full_notes,
                "Switch Time": time_notes, "Est. Price": round(daily['bbm_price'], 2),
                "Remarks": danger_msg
            })
        else:
            detailed_fail_reason = " | ".join(fail_details) if fail_details else "Lower TF Mismatch"
            results.append({
                "Ticker": ticker, "Signal": "No Signal",
                "Current Price": curr_price, "Daily Setup": daily['setup_type'],
                "Failure Reason": f"Lower TF Mismatch: [{detailed_fail_reason}]",
                "Confirmations": "-", "Switch Time": "-",
                "Est. Price": round(daily['bbm_price'], 2),
                "Remarks": danger_msg
            })
    
    print("\nScan Complete.")
    return pd.DataFrame(results)

# --- ALIAS FOR COMPATIBILITY ---
run_scanner = analyze_market_structure
