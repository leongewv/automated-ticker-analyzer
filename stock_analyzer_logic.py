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

def check_trend_structure(sma_series, ema_series, lookback=120):
    """
    Analyzes the trend structure of the 20 SMA (sma_series)
    and checks for flush-outs against the 200 EMA (ema_series).
    
    Returns: "Super Bullish", "Bullish", "Super Bearish", "Bearish", or "Indeterminate"
    """
    if len(sma_series) < lookback:
        return "Indeterminate"

    # Split the 20 SMA series
    recent_half_sma = sma_series.iloc[-int(lookback/2):]
    prior_half_sma = sma_series.iloc[-lookback:-int(lookback/2)]
    
    # Split the 200 EMA series for the same periods
    recent_half_ema = ema_series.iloc[-int(lookback/2):]
    prior_half_ema = ema_series.iloc[-lookback:-int(lookback/2)]

    # 1. Determine Base Trend
    is_bullish = recent_half_sma.max() > prior_half_sma.max() and recent_half_sma.min() > prior_half_sma.min()
    is_bearish = recent_half_sma.min() < prior_half_sma.min() and recent_half_sma.max() < prior_half_sma.max()
    
    # 2. Check for "Super" Trend conditions
    if is_bullish:
        # Check for a flush-out: 
        # 1. Did the SMA dip below the EMA in the prior wave?
        was_below_ema = (prior_half_sma < prior_half_ema).any()
        # 2. Has it now firmly reclaimed the EMA?
        is_above_now = sma_series.iloc[-1] > ema_series.iloc[-1]
        
        if was_below_ema and is_above_now:
            return "Super Bullish"
        return "Bullish"

    if is_bearish:
        # Check for a "fake-out" rally:
        # 1. Did the SMA spike above the EMA in the prior wave?
        was_above_ema = (prior_half_sma > prior_half_ema).any()
        # 2. Has it now firmly fallen back below the EMA?
        is_below_now = sma_series.iloc[-1] < ema_series.iloc[-1]
        
        if was_above_ema and is_below_now:
            return "Super Bearish"
        return "Bearish"
        
    return "Indeterminate"

def analyze_instrument(df):
    """
    Performs the core analysis with a tiered signal system.
    Returns: (Signal, Setup, Trend)
    """
    if df is None or len(df) < 120: # Ensure enough data for lookbacks
        return "Insufficient Data", "N/A", "N/A"

    # 1. Check Trend Structure
    trend_lookback = 120
    
    # *** CHANGED: Pass both SMA and EMA to check_trend_structure ***
    trend_direction = check_trend_structure(
        df['BBM_20'], df['EMA_200'], lookback=trend_lookback
    )

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

    if "Bullish" in trend_direction: # Catches "Bullish" and "Super Bullish"
        if is_near_ema:
            return "Strong Buy", f"{trend_direction} Trend + Squeeze at 200 EMA", trend_direction
        else:
            recent_half_sma = df['BBM_20'].iloc[-int(trend_lookback/2):]
            is_at_higher_high = abs(latest['BBM_20'] - recent_half_sma.max()) / recent_half_sma.max() < 0.02 
            
            if is_at_higher_high:
                return "Moderate Buy", f"{trend_direction} Trend + Squeeze at Higher High", trend_direction

    elif "Bearish" in trend_direction: # Catches "Bearish" and "Super Bearish"
        if is_near_ema:
            return "Strong Sell", f"{trend_direction} Trend + Squeeze at 200 EMA", trend_direction
        else:
            recent_half_sma = df['BBM_20'].iloc[-int(trend_lookback/2):]
            is_at_lower_low = abs(latest['BBM_20'] - recent_half_sma.min()) / recent_half_sma.min() < 0.02
            
            if is_at_lower_low:
                return "Moderate Sell", f"{trend_direction} Trend + Squeeze at Lower Low", trend_direction

    # Default case
    if trend_direction == "Indeterminate":
        return "Hold", "Indeterminate Trend", trend_direction
    
    return "Hold", "Squeeze conditions not met", trend_direction

# --- Main Execution ---

def run_multi_timeframe_analysis(tickers_to_analyze, status_callback=None):
    """
    Runs the full analysis pipeline:
    - Analyzes the daily chart for a primary signal.
    - If a strong/moderate signal exists, seeks confirmation on lower timeframes.
    """
    results_list = []
    confirmation_timeframes = ["4h", "1h", "30m"]
    
    total_tickers = len(tickers_to_analyze)
    for i, ticker in enumerate(tickers_to_analyze):
        if status_callback:
            status_callback(f"Analyzing {ticker}... ({i+1}/{total_tickers})")
        
        # 1. Analyze the Daily Chart
        daily_df = get_data(ticker=ticker, interval="1d")
        daily_signal, daily_setup, daily_trend = analyze_instrument(daily_df)
        
        final_signal = "Hold for now"
        confirmed_tfs = []

        # 2. If Daily chart shows a signal, check lower timeframes
        if "Strong" in daily_signal or "Moderate" in daily_signal:
            direction = "Buy" if "Buy" in daily_signal else "Sell"
            final_signal = daily_signal
            
            for tf in confirmation_timeframes:
                time.sleep(0.5) 
                intraday_df = get_data(ticker=ticker, interval=tf)
                tf_signal, _, _ = analyze_instrument(intraday_df) 
                
                if direction in tf_signal: 
                    confirmed_tfs.append(tf)

            # 3. Upgrade the signal if "Strong" and confirmed
            if "Strong" in daily_signal and confirmed_tfs:
                final_signal = f"Super Strong {direction}"
        
        # 4. Compile results
        results_list.append({
            "Instrument": ticker,
            "Trend": daily_trend, 
            "Signal": final_signal,
            "Daily Setup": daily_setup,
            "Confirmation TFs": ", ".join(confirmed_tfs) if confirmed_tfs else "None"
        })
        time.sleep(1) # Main delay between tickers

    # Define the final output columns
    column_order = ["Instrument", "Trend", "Signal", "Daily Setup", "Confirmation TFs"]
    return pd.DataFrame(results_list, columns=column_order)

# --- Example Usage (if you want to run this file directly) ---
if __name__ == '__main__':
    tickers = ["MSFT", "AAPL", "GOOGL", "EURUSD=X", "GBPUSD=X"]

    def print_status(message):
        print(message)

    analysis_results = run_multi_timeframe_analysis(tickers, status_callback=print_status)
    
    print("\n--- Trading Analysis Results ---")
    print(analysis_results.to_string())
