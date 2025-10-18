import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time

# --- Helper & Analysis Functions ---

def get_data(ticker, period="2y", interval="1d"):
    """Fetches and prepares historical market data for a given ticker."""
    # For shorter intervals, yfinance limits the period
    if interval not in ["1d", "1wk", "1mo"]:
        period = "730d" if interval in ["2h", "4h"] else "60d"

    data = yf.Ticker(ticker).history(period=period, interval=interval)
    
    # Ensure there's enough data to calculate a 200-period EMA
    if data.empty or len(data) < 200:
        return None

    # Standardize column names
    data.rename(columns={
        "Open": "open", "High": "high", "Low": "low", 
        "Close": "close", "Volume": "volume"}, inplace=True)

    # Calculate required indicators
    data['EMA_200'] = TA.EMA(data, period=200)
    bbands = TA.BBANDS(data, period=20)
    data['BBM_20'] = bbands['BB_MIDDLE']
    data['BBU_20'] = bbands['BB_UPPER']
    data['BBL_20'] = bbands['BB_LOWER']
    data['BB_WIDTH'] = (data['BBU_20'] - data['BBL_20']) / data['BBM_20']
    
    # Clean up by removing rows with missing indicator values
    data.dropna(inplace=True)
    return data

def check_trend_structure(series, lookback=120):
    """
    Analyzes the structure of a series (e.g., a moving average) for trends.
    Checks for waves of Higher Highs (HH) & Higher Lows (HL) for an uptrend,
    or Lower Lows (LL) & Lower Highs (LH) for a downtrend.
    """
    if len(series) < lookback:
        return "Indeterminate"

    recent_half = series.iloc[-int(lookback/2):]
    prior_half = series.iloc[-lookback:-int(lookback/2)]

    # Bullish trend: Recent highs and lows are higher than prior highs and lows
    is_bullish = recent_half.max() > prior_half.max() and recent_half.min() > prior_half.min()
    
    # Bearish trend: Recent lows and highs are lower than prior lows and highs
    is_bearish = recent_half.min() < prior_half.min() and recent_half.max() < prior_half.max()

    if is_bullish:
        return "Bullish"
    if is_bearish:
        return "Bearish"
        
    return "Indeterminate"

def analyze_instrument(df):
    """
    Performs the core analysis with a tiered signal system.
    1. Trend Structure (HH/HL or LL/LH on the 20 SMA)
    2. Bollinger Band Squeeze
    3. 'Strong' Signal: Squeeze is near the 200 EMA.
    4. 'Moderate' Signal: Squeeze is NOT near EMA, but is at a trend peak (HH/LL).
    
    *** NOW RETURNS (Signal, Setup, Trend) ***
    """
    if df is None or len(df) < 120: # Ensure enough data for lookbacks
        return "Insufficient Data", "N/A", "N/A"

    # 1. Check Trend Structure
    trend_lookback = 120
    trend_direction = check_trend_structure(df['BBM_20'], lookback=trend_lookback)

    latest = df.iloc[-1]
    
    # 2. Check for BB Squeeze
    squeeze_lookback = 120
    squeeze_percentile = 0.20 # Bottom 20% of BB Width values
    historical_bandwidth = df['BB_WIDTH'].iloc[-squeeze_lookback:-1]
    
    if historical_bandwidth.empty:
        return "Insufficient Data", "Not enough squeeze data", trend_direction
        
    squeeze_threshold = historical_bandwidth.quantile(squeeze_percentile)
    is_in_squeeze = latest['BB_WIDTH'] < squeeze_threshold

    if not is_in_squeeze:
        return "Hold", "Not in Squeeze", trend_direction
        
    # 3. Check Proximity of Squeeze to 200 EMA
    proximity_threshold = 0.03 # Within 3%
    is_near_ema = abs(latest['BBM_20'] - latest['EMA_200']) / latest['EMA_200'] < proximity_threshold

    # --- MAIN SIGNAL LOGIC ---

    if trend_direction == "Bullish":
        if is_near_ema:
            # Condition 1: Strong Buy (Trend + Squeeze + EMA Proximity)
            return "Strong Buy", "Bullish Trend + Squeeze at 200 EMA", trend_direction
        else:
            # Condition 2: Check for Moderate Buy (Trend + Squeeze + NOT near EMA + at Higher High)
            recent_half_sma = df['BBM_20'].iloc[-int(trend_lookback/2):]
            # Is the current SMA value near the peak of its recent run (within 2%)?
            is_at_higher_high = abs(latest['BBM_20'] - recent_half_sma.max()) / recent_half_sma.max() < 0.02 
            
            if is_at_higher_high:
                return "Moderate Buy", "Bullish Trend + Squeeze at Higher High", trend_direction

    elif trend_direction == "Bearish":
        if is_near_ema:
            # Condition 1: Strong Sell (Trend + Squeeze + EMA Proximity)
            return "Strong Sell", "Bearish Trend + Squeeze at 200 EMA", trend_direction
        else:
            # Condition 2: Check for Moderate Sell (Trend + Squeeze + NOT near EMA + at Lower Low)
            recent_half_sma = df['BBM_20'].iloc[-int(trend_lookback/2):]
            # Is the current SMA value near the bottom of its recent run (within 2%)?
            is_at_lower_low = abs(latest['BBM_20'] - recent_half_sma.min()) / recent_half_sma.min() < 0.02
            
            if is_at_lower_low:
                return "Moderate Sell", "Bearish Trend + Squeeze at Lower Low", trend_direction

    # Default case
    if trend_direction == "Indeterminate":
        return "Hold", "Indeterminate Trend", trend_direction
    
    return "Hold", "Squeeze conditions not met", trend_direction # Squeeze was not near EMA or at HH/LL

# --- Main Execution ---

def run_multi_timeframe_analysis(tickers_to_analyze, status_callback=None):
    """
    Runs the full analysis pipeline:
    - Analyzes the daily chart for a primary signal.
    - If a strong/moderate signal exists, seeks confirmation on lower timeframes.
    """
    results_list = []
    # Note: yfinance doesn't support '2h'. Using the closest available standard intervals.
    confirmation_timeframes = ["4h", "1h", "30m"]
    
    total_tickers = len(tickers_to_analyze)
    for i, ticker in enumerate(tickers_to_analyze):
        if status_callback:
            status_callback(f"Analyzing {ticker}... ({i+1}/{total_tickers})")
        
        # 1. Analyze the Daily Chart for the primary signal
        daily_df = get_data(ticker=ticker, interval="1d")
        
        # *** CHANGED: Now receives daily_trend as the 3rd value ***
        daily_signal, daily_setup, daily_trend = analyze_instrument(daily_df)
        
        final_signal = "Hold for now"
        confirmed_tfs = []

        # 2. If Daily chart shows a strong/moderate signal, check lower timeframes
        if "Strong" in daily_signal or "Moderate" in daily_signal:
            direction = "Buy" if "Buy" in daily_signal else "Sell"
            
            # Set the base signal (it might be "Moderate" or "Strong")
            final_signal = daily_signal
            
            for tf in confirmation_timeframes:
                # Add a small delay to avoid API rate limiting issues
                time.sleep(0.5) 
                
                intraday_df = get_data(ticker=ticker, interval=tf)
                
                # We only care about the signal from the sub-timeframe
                tf_signal, _, _ = analyze_instrument(intraday_df) 
                
                # Check if the intraday signal matches the daily signal's direction
                if direction in tf_signal: 
                    confirmed_tfs.append(tf)

            # 3. Upgrade the signal if there's at least one confirmation
            # Only upgrade if the daily signal was already "Strong"
            if "Strong" in daily_signal and confirmed_tfs:
                final_signal = f"Super Strong {direction}"
        
        # 4. Compile results for the report
        # *** CHANGED: Added "Trend": daily_trend to the dictionary ***
        results_list.append({
            "Instrument": ticker,
            "Trend": daily_trend, 
            "Signal": final_signal,
            "Daily Setup": daily_setup,
            "Confirmation TFs": ", ".join(confirmed_tfs) if confirmed_tfs else "None"
        })
        time.sleep(1) # Main delay between tickers

    # Define the final output columns
    # *** CHANGED: Added "Trend" to the column order ***
    column_order = ["Instrument", "Trend", "Signal", "Daily Setup", "Confirmation TFs"]
    return pd.DataFrame(results_list, columns=column_order)

# --- Example Usage (if you want to run this file directly) ---
if __name__ == '__main__':
    # List of stocks/forex pairs to analyze
    tickers = ["MSFT", "AAPL", "GOOGL", "EURUSD=X", "GBPUSD=X"]

    def print_status(message):
        print(message)

    # Run the analysis
    analysis_results = run_multi_timeframe_analysis(tickers, status_callback=print_status)
    
    # Print the results
    print("\n--- Trading Analysis Results ---")
    print(analysis_results.to_string())
