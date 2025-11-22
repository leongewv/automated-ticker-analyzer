import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# --- Configuration ---
SLOPE_LOOKBACK = 5  # Candles to calculate slope
EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0

def get_data(ticker, period="2y", interval="1d"):
    """
    Fetches data and calculates indicators.
    """
    if interval == "1h":
        period = "1y" 
    elif interval == "4h":
        period = "1y" 
        
    try:
        df = yf.Ticker(ticker).history(period=period, interval=interval)
        
        if df.empty or len(df) < 250:
            return None

        df.rename(columns={
            "Open": "open", "High": "high", "Low": "low", 
            "Close": "close", "Volume": "volume"
        }, inplace=True)

        # Indicators
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BBM_20'] = bbands['BB_MIDDLE']
        df['BBU_20'] = bbands['BB_UPPER']
        df['BBL_20'] = bbands['BB_LOWER']
        df['BB_WIDTH'] = (df['BBU_20'] - df['BBL_20']) / df['BBM_20']

        df.dropna(inplace=True)
        return df
    
    except Exception as e:
        # print(f"Error fetching {ticker}: {e}") # Optional: uncomment for debug
        return None

def get_slope(series, lookback=5):
    """Calculates linear regression slope of the last N values."""
    if len(series) < lookback: return 0
    y = series.iloc[-lookback:].values
    x = np.arange(lookback)
    slope, _ = np.polyfit(x, y, 1)
    return slope

def check_slope_transition(series, lookback=5):
    """
    Checks if the slope has shifted sign in the current window compared to the previous window.
    Returns: 'Pos->Neg', 'Neg->Pos', or None
    """
    if len(series) < (lookback * 2): return None

    # Slope of current window
    curr_slope = get_slope(series.iloc[-lookback:], lookback)
    
    # Slope of previous window (shifted back by 1 candle to catch immediate turns)
    prev_series = series.iloc[-(lookback+1):-1]
    prev_slope = get_slope(prev_series, lookback)
    
    if prev_slope < 0 and curr_slope > 0:
        return "Neg->Pos"
    if prev_slope > 0 and curr_slope < 0:
        return "Pos->Neg"
        
    return None

def check_crossover(df, lookback=3):
    """Checks for BBM crossing EMA_200."""
    if len(df) < lookback + 1: return None
    
    # Check the relationship lookback periods ago vs now
    prev_diff = df['BBM_20'].iloc[-lookback-1] - df['EMA_200'].iloc[-lookback-1]
    curr_diff = df['BBM_20'].iloc[-1] - df['EMA_200'].iloc[-1]

    if prev_diff < 0 and curr_diff > 0: return "Bullish Cross"
    if prev_diff > 0 and curr_diff < 0: return "Bearish Cross"
    return None

def analyze_daily_chart(ticker):
    """
    Step 1: Identify Potential on Daily.
    Scenarios:
    A) Squeeze
    B) Mean Reversion (Price near EMA) -> Sub-types: Bounce or Flip
    """
    df = get_data(ticker, period="2y", interval="1d")
    if df is None: return None

    last = df.iloc[-1]
    
    # 1. Mean Reversion (Within 2%)
    dist_pct = abs(last['BBM_20'] - last['EMA_200']) / last['EMA_200']
    is_mean_rev = dist_pct <= 0.02
    
    # 2. Squeeze (Bottom 20% width)
    lookback_squeeze = 126
    if len(df) > lookback_squeeze:
        recent_widths = df['BB_WIDTH'].iloc[-lookback_squeeze:]
        rank = (recent_widths < last['BB_WIDTH']).mean()
        is_squeeze = rank <= 0.20
    else:
        is_squeeze = False

    if not (is_mean_rev or is_squeeze):
        return None

    # 3. Determine Direction & Setup Type
    # Check if a crossover happened recently (last 5 days) to detect a "Flip"
    recent_cross = check_crossover(df, lookback=5)
    
    current_direction = "Buy" if last['BBM_20'] > last['EMA_200'] else "Sell"
    setup_type = ""

    if recent_cross:
        # If we just crossed, it's a Trend Flip
        if current_direction == "Buy" and recent_cross == "Bullish Cross":
            setup_type = "Trend Flip (Up)"
        elif current_direction == "Sell" and recent_cross == "Bearish Cross":
            setup_type = "Trend Flip (Down)"
        else:
            # Fallback if cross direction doesn't match current state (rare volatility)
            setup_type = "Mean Rev"
    else:
        # No recent cross, so it's a standard Mean Reversion / Squeeze
        setup_type = "Mean Rev / Squeeze"

    return {
        "ticker": ticker,
        "direction": current_direction,
        "setup_type": setup_type,
        "is_squeeze": is_squeeze,
        "is_mean_rev": is_mean_rev,
        "price": last['BBM_20']
    }

def analyze_lower_timeframes(ticker, daily_dir):
    """
    Step 2: Check 4H and 1H independently for confirmation.
    """
    timeframes = ["4h", "1h"]
    confirmations = []
    
    for tf in timeframes:
        df = get_data(ticker, period="1y", interval=tf)
        if df is None: continue
        
        last = df.iloc[-1]
        bbm = df['BBM_20']
        
        # 1. Check Slope & Transition
        current_slope = get_slope(bbm, SLOPE_LOOKBACK)
        transition = check_slope_transition(bbm, SLOPE_LOOKBACK)
        
        # 2. Check Crossover
        crossover = check_crossover(df)
        
        # 3. Check Position
        is_above = last['BBM_20'] > last['EMA_200']
        
        tf_notes = []
        is_valid_tf = False
        
        # --- Evaluate Logic based on Direction ---
        if daily_dir == "Buy":
            # Requirement: Must be Above EMA
            if is_above:
                # Check for specific triggers
                if transition == "Neg->Pos":
                    tf_notes.append("Slope Flip")
                    is_valid_tf = True
                elif current_slope > 0:
                    tf_notes.append("Trend Up")
                    is_valid_tf = True
                
                # Super Signal Check
                if crossover == "Bullish Cross":
                    tf_notes.append("GOLDEN CROSS")
                    is_valid_tf = True

        elif daily_dir == "Sell":
            # Requirement: Must be Below EMA
            if not is_above:
                if transition == "Pos->Neg":
                    tf_notes.append("Slope Flip")
                    is_valid_tf = True
                elif current_slope < 0:
                    tf_notes.append("Trend Down")
                    is_valid_tf = True
                
                if crossover == "Bearish Cross":
                    tf_notes.append("DEATH CROSS")
                    is_valid_tf = True

        if is_valid_tf:
            note_str = " + ".join(tf_notes)
            confirmations.append(f"{tf}: {note_str}")

    return confirmations

def run_scanner(tickers):
    results = []
    print(f"Scanning {len(tickers)} tickers...")
    
    for ticker in tickers:
        print(f"Checking {ticker}...", end="\r")
        daily = analyze_daily_chart(ticker)
        
        if daily:
            time.sleep(1) # API pacing
            confs = analyze_lower_timeframes(ticker, daily['direction'])
            
            # Valid if AT LEAST ONE lower timeframe confirms
            if confs:
                # Build Setup Label
                labels = []
                if daily['is_squeeze']: labels.append("Squeeze")
                labels.append(daily['setup_type'])
                final_setup = " + ".join(labels)
                
                # Determine Signal Strength
                full_notes = " | ".join(confs)
                signal_type = "SUPER" if "CROSS" in full_notes else "Standard"
                
                results.append({
                    "Ticker": ticker,
                    "Signal": f"{signal_type} {daily['direction']}",
                    "Daily Setup": final_setup,
                    "Confirmations": full_notes,
                    "Est. Price": round(daily['price'], 2)
                })
    
    print("\nScan Complete.")
    return pd.DataFrame(results)

if __name__ == "__main__":
    # Tickers
    ticker_list = [
        "NVDA", "TSLA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", # Tech
        "EURUSD=X", "GBPUSD=X", "USDJPY=X", # Forex
        "BTC-USD", "ETH-USD", "SOL-USD" # Crypto
    ]
    
    df_results = run_scanner(ticker_list)
    
    if not df_results.empty:
        print("\n=== SCAN RESULTS ===")
        pd.set_option('display.max_colwidth', None)
        print(df_results.to_string(index=False))
    else:
        print("\nNo matching setups found.")
