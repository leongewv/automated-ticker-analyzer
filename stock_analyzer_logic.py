import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# --- Configuration ---
SLOPE_LOOKBACK_SLOW = 20  # 40-candle footprint
SLOPE_LOOKBACK_FAST = 8   # 16-candle footprint

EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0
MEAN_REV_TOLERANCE_MAX = 0.03 
TREND_FLIP_MIN = 0.02
RETEST_TOLERANCE = 0.015  # 1.5% tolerance for the retest "kiss"

def get_data(ticker, period="2y", interval="1d"):
    """Fetches data and calculates indicators."""
    if interval == "1h": period = "1y" 
    elif interval == "4h": period = "1y" 
        
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        if df.empty or len(df) < 250: return None

        df.rename(columns={
            "Open": "open", "High": "high", "Low": "low", 
            "Close": "close", "Volume": "volume"
        }, inplace=True)

        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BBM_20'] = bbands['BB_MIDDLE']
        df['BBU_20'] = bbands['BB_UPPER']
        df['BBL_20'] = bbands['BB_LOWER']
        df['BB_WIDTH'] = (df['BBU_20'] - df['BBL_20']) / df['BBM_20']

        df.dropna(inplace=True)
        return df
    except Exception as e:
        return None

def get_slope(series, lookback):
    """Calculates linear regression slope."""
    if len(series) < lookback: return 0
    y = series.iloc[-lookback:].values
    x = np.arange(lookback)
    slope, _ = np.polyfit(x, y, 1)
    return slope

def check_slope_transition(series, dates, lookback, label_suffix=""):
    """Checks for fresh slope sign shift at the current moment."""
    if len(series) < (lookback * 2): return None, None, None

    curr_slope = get_slope(series.iloc[-lookback:], lookback)
    prev_series = series.iloc[-(lookback*2):-lookback]
    prev_slope = get_slope(prev_series, lookback)
    
    event_idx = -lookback
    event_time = dates[event_idx].strftime('%Y-%m-%d %H:%M')

    sig_text = None
    if prev_slope < 0 and curr_slope > 0:
        sig_text = f"Slope Flip {label_suffix}"
        return "Neg->Pos", sig_text, event_time
    if prev_slope > 0 and curr_slope < 0:
        sig_text = f"Slope Flip {label_suffix}"
        return "Pos->Neg", sig_text, event_time
        
    return None, None, None

def check_retest_validity(df, lookback_speed, direction):
    """
    Scans BACKWARDS to find the last valid slope flip.
    Then checks if price retested that flip level (within tolerance) and bounced.
    """
    series = df['BBM_20']
    limit = 60 # Scan back limit
    
    # We iterate backwards from 'now' to find the most recent flip
    # Start loop from -1 down to -limit
    found_flip_idx = None
    flip_level = None
    
    # We need enough data history
    if len(series) < limit + lookback_speed * 2: return False

    # 1. Find the Flip
    for i in range(1, limit):
        # Slice for the historical window check
        # 'end' is the point we are checking. 
        # Window A: end-lookback to end
        # Window B: end-2*lookback to end-lookback
        
        # Adjust indices for slicing relative to 'i' steps back
        idx_now = len(series) - i
        
        # Current window at that historical point
        hist_curr = series.iloc[idx_now - lookback_speed : idx_now]
        # Previous window
        hist_prev = series.iloc[idx_now - (lookback_speed*2) : idx_now - lookback_speed]
        
        s_curr = get_slope(hist_curr, lookback_speed)
        s_prev = get_slope(hist_prev, lookback_speed)
        
        # Check Flip Logic
        if direction == "Buy" and s_prev < 0 and s_curr > 0:
            found_flip_idx = idx_now - lookback_speed # The pivot point
            flip_level = series.iloc[found_flip_idx]
            break
        elif direction == "Sell" and s_prev > 0 and s_curr < 0:
            found_flip_idx = idx_now - lookback_speed
            flip_level = series.iloc[found_flip_idx]
            break
            
    if found_flip_idx is None:
        return False # No structural flip found in recent history

    # 2. Verify Retest (The "Kiss")
    # We look at price action from the Flip Index until Now
    segment_lows = df['low'].iloc[found_flip_idx:]
    segment_highs = df['high'].iloc[found_flip_idx:]
    current_close = df['close'].iloc[-1]

    if direction == "Buy":
        # Did price drop near the flip level?
        lowest_since_flip = segment_lows.min()
        
        # Tolerance Check: Is the Low within 1.5% of the Flip Level?
        # Note: It can be slightly below or slightly above
        dist = abs(lowest_since_flip - flip_level) / flip_level
        is_retest_ok = dist <= RETEST_TOLERANCE
        
        # Bounce Check: Are we currently above the lows?
        is_bouncing = current_close > lowest_since_flip
        
        return is_retest_ok and is_bouncing

    elif direction == "Sell":
        # Did price rise near the flip level?
        highest_since_flip = segment_highs.max()
        
        dist = abs(highest_since_flip - flip_level) / flip_level
        is_retest_ok = dist <= RETEST_TOLERANCE
        
        is_bouncing = current_close < highest_since_flip
        
        return is_retest_ok and is_bouncing

    return False

def check_crossover(df, lookback=5):
    """Checks for BBM crossing EMA_200."""
    if len(df) < lookback + 1: return None, None
    bbm = df['BBM_20']
    ema = df['EMA_200']
    dates = df.index

    for i in range(1, lookback + 1):
        curr_diff = bbm.iloc[-i] - ema.iloc[-i]
        prev_diff = bbm.iloc[-(i+1)] - ema.iloc[-(i+1)]
        current_time = dates[-i].strftime('%Y-%m-%d %H:%M')

        if prev_diff < 0 and curr_diff > 0: return "Bullish Cross", current_time
        if prev_diff > 0 and curr_diff < 0: return "Bearish Cross", current_time

    return None, None

def analyze_daily_chart(ticker):
    """Step 1: Identify Potential on Daily."""
    df = get_data(ticker, period="2y", interval="1d")
    if df is None: return None

    last = df.iloc[-1]
    dist_pct = abs(last['BBM_20'] - last['EMA_200']) / last['EMA_200']
    
    is_in_zone = dist_pct <= MEAN_REV_TOLERANCE_MAX
    
    lookback_squeeze = 126
    if len(df) > lookback_squeeze:
        recent_widths = df['BB_WIDTH'].iloc[-lookback_squeeze:]
        rank = (recent_widths < last['BB_WIDTH']).mean()
        is_squeeze = rank <= 0.20
    else:
        is_squeeze = False

    if not (is_in_zone or is_squeeze): return None

    cross_signal, _ = check_crossover(df, lookback=5) 
    current_direction = "Buy" if last['BBM_20'] > last['EMA_200'] else "Sell"
    bbm_slope = get_slope(df['BBM_20'], lookback=SLOPE_LOOKBACK_SLOW)
    
    setup_type = ""
    is_valid_setup = False

    if cross_signal:
        if dist_pct >= TREND_FLIP_MIN: 
            if current_direction == "Buy" and cross_signal == "Bullish Cross":
                setup_type = "Trend Flip (Up)"
                is_valid_setup = True
            elif current_direction == "Sell" and cross_signal == "Bearish Cross":
                setup_type = "Trend Flip (Down)"
                is_valid_setup = True
        else: is_valid_setup = False
            
    elif is_in_zone:
        if current_direction == "Buy" and bbm_slope < 0: 
            setup_type = "Mean Rev (Bounce Up)"
            is_valid_setup = True
        elif current_direction == "Sell" and bbm_slope > 0: 
            setup_type = "Mean Rev (Bounce Down)"
            is_valid_setup = True

    if is_squeeze:
        if is_valid_setup: setup_type = f"Squeeze ({setup_type})"
        else: setup_type = "Squeeze"
        is_valid_setup = True

    if not is_valid_setup: return None

    return {
        "ticker": ticker,
        "direction": current_direction,
        "setup_type": setup_type,
        "is_squeeze": is_squeeze,
        "is_mean_rev": is_in_zone,
        "price": last['BBM_20']
    }

def analyze_lower_timeframes(ticker, daily_dir):
    """Step 2: Check 4H and 1H using Dual-Speed + Retest Logic."""
    timeframes = ["4h", "1h"]
    confirmations = []
    time_logs = []
    
    for tf in timeframes:
        df = get_data(ticker, period="1y", interval=tf)
        if df is None: continue
        
        last = df.iloc[-1]
        bbm = df['BBM_20']
        
        # --- DUAL SPEED CHECK ---
        trans_slow, sig_text_slow, time_slow = check_slope_transition(bbm, df.index, SLOPE_LOOKBACK_SLOW, "(Slow)")
        trans_fast, sig_text_fast, time_fast = check_slope_transition(bbm, df.index, SLOPE_LOOKBACK_FAST, "(Fast)")
        
        cross_sig, cross_time = check_crossover(df)
        is_above = last['BBM_20'] > last['EMA_200']
        
        tf_notes = []
        tf_time = "Established" 
        is_valid_tf = False
        
        # Prioritize Transition Signals
        active_trans = None
        active_trans_sig = None
        active_trans_time = None
        
        if trans_fast:
            active_trans, active_trans_sig, active_trans_time = trans_fast, sig_text_fast, time_fast
        if trans_slow:
            active_trans, active_trans_sig, active_trans_time = trans_slow, sig_text_slow, time_slow

        current_slope_fast = get_slope(bbm, SLOPE_LOOKBACK_FAST)

        # --- Evaluate Logic ---
        if daily_dir == "Buy":
            if is_above:
                # A. Fresh Flip
                if active_trans == "Neg->Pos":
                    tf_notes.append(active_trans_sig)
                    tf_time = active_trans_time
                    is_valid_tf = True
                
                # B. Trend Continuation (With Retest Check)
                elif current_slope_fast > 0:
                    # Check if we respected previous structure (Fast or Slow flip retest)
                    # We check Slow first as it's more significant
                    retest_ok = check_retest_validity(df, SLOPE_LOOKBACK_SLOW, "Buy")
                    if not retest_ok:
                        # Fallback to Fast flip retest
                        retest_ok = check_retest_validity(df, SLOPE_LOOKBACK_FAST, "Buy")
                    
                    if retest_ok:
                        tf_notes.append("Trend Up (Retest Confirmed)")
                        is_valid_tf = True
                
                # C. Golden Cross
                if cross_sig == "Bullish Cross":
                    tf_notes.append("GOLDEN CROSS")
                    tf_time = cross_time
                    is_valid_tf = True

        elif daily_dir == "Sell":
            if not is_above:
                if active_trans == "Pos->Neg":
                    tf_notes.append(active_trans_sig)
                    tf_time = active_trans_time
                    is_valid_tf = True
                
                elif current_slope_fast < 0:
                    retest_ok = check_retest_validity(df, SLOPE_LOOKBACK_SLOW, "Sell")
                    if not retest_ok:
                        retest_ok = check_retest_validity(df, SLOPE_LOOKBACK_FAST, "Sell")

                    if retest_ok:
                        tf_notes.append("Trend Down (Retest Confirmed)")
                        is_valid_tf = True
                
                if cross_sig == "Bearish Cross":
                    tf_notes.append("DEATH CROSS")
                    tf_time = cross_time
                    is_valid_tf = True

        if is_valid_tf:
            note_str = " + ".join(tf_notes)
            confirmations.append(f"{tf}: {note_str}")
            time_logs.append(f"{tf}: {tf_time}")

    return confirmations, time_logs

def run_scanner(tickers):
    results = []
    print(f"Scanning {len(tickers)} tickers...")
    
    for ticker in tickers:
        print(f"Checking {ticker}...", end="\r")
        daily = analyze_daily_chart(ticker)
        
        if daily:
            time.sleep(1) # API pacing
            confs, times = analyze_lower_timeframes(ticker, daily['direction'])
            
            if confs:
                labels = []
                if "Squeeze" not in daily['setup_type'] and daily['is_squeeze']:
                    labels.append("Squeeze")
                labels.append(daily['setup_type'])
                final_setup = " + ".join(labels)
                
                full_notes = " | ".join(confs)
                time_notes = " | ".join(times)
                signal_type = "SUPER" if "CROSS" in full_notes else "Standard"
                
                results.append({
                    "Ticker": ticker,
                    "Signal": f"{signal_type} {daily['direction']}",
                    "Daily Setup": final_setup,
                    "Confirmations": full_notes,
                    "Switch Time": time_notes,
                    "Est. Price": round(daily['price'], 2)
                })
    
    print("\nScan Complete.")
    return pd.DataFrame(results)
