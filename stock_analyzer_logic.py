import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time

# --- Helper & Analysis Functions ---

def get_data(ticker, period="2y", interval="1d"):
    """Fetches and prepares historical market data for a given ticker."""
    if interval not in ["1d", "1wk", "1mo"]:
        period = "730d" if interval in ["2h", "4h"] else "60d"

    data = yf.Ticker(ticker).history(period=period, interval=interval)
    
    if data.empty or len(data) < 200:
        return None

    data.rename(columns={
        "Open": "open", "High": "high", "Low": "low", 
        "Close": "close", "Volume": "volume"}, inplace=True)

    data['EMA_200'] = TA.EMA(data, period=200)
    bbands = TA.BBANDS(data, period=20)
    data['BBM_20'] = bbands['BB_MIDDLE']
    data['BBU_20'] = bbands['BB_UPPER']
    data['BBL_20'] = bbands['BB_LOWER']
    data['BB_WIDTH'] = (data['BBU_20'] - data['BBL_20']) / data['BBM_20']
    
    data.dropna(inplace=True)
    return data

def check_trend_structure(sma_series, ema_series, lookback=120):
    """
    Analyzes the trend structure of the 20 SMA (sma_series)
    and checks for flush-outs against the 200 EMA (ema_series).
    """
    if len(sma_series) < lookback:
        return "Indeterminate"

    recent_half_sma = sma_series.iloc[-int(lookback/2):]
    prior_half_sma = sma_series.iloc[-lookback:-int(lookback/2)]
    
    recent_half_ema = ema_series.iloc[-int(lookback/2):]
    prior_half_ema = ema_series.iloc[-lookback:-int(lookback/2)]

    is_bullish = recent_half_sma.max() > prior_half_sma.max() and recent_half_sma.min() > prior_half_sma.min()
    is_bearish = recent_half_sma.min() < prior_half_sma.min() and recent_half_sma.max() < prior_half_sma.max()
    
    if is_bullish:
        was_below_ema = (prior_half_sma < prior_half_ema).any()
        is_above_now = sma_series.iloc[-1] > ema_series.iloc[-1]
        if was_below_ema and is_above_now:
            return "Super Bullish"
        return "Bullish"

    if is_bearish:
        was_above_ema = (prior_half_sma > prior_half_ema).any()
        is_below_now = sma_series.iloc[-1] < ema_series.iloc[-1]
        if was_above_ema and is_below_now:
            return "Super Bearish"
        return "Bearish"
        
    return "Indeterminate"

# *** UPDATED: Calculates Fibs based on the *previous* wave ***
def calculate_fib_extension(df, direction, trend_lookback=120):
    """
    Calculates A-B-C Fibonacci extension levels based on the *previous* wave.
    A/B points are from the prior wave, C point is the start of the current wave.
    Returns a dictionary of levels (including A,B,C) or None if invalid.
    """
    if len(df) < trend_lookback:
        return None
        
    # Define the two most recent "waves"
    prior_wave_data = df.iloc[-trend_lookback:-int(trend_lookback/2)]
    current_wave_data = df.iloc[-int(trend_lookback/2):]

    if prior_wave_data.empty or current_wave_data.empty:
        return None

    try:
        if direction == "Buy":
            # A: Lowest low of the previous wave
            a_price = prior_wave_data['low'].min()
            # B: Highest high of the previous wave
            b_price = prior_wave_data['high'].max()
            # C: Lowest low of the current wave (the pullback)
            c_price = current_wave_data['low'].min()

            # Check for valid uptrend structure (C > A and B > A)
            if c_price > a_price and b_price > a_price:
                trend_range = b_price - a_price
                return {
                    "fib_A": a_price,
                    "fib_B": b_price,
                    "fib_C": c_price,
                    "fib_0.786": c_price + trend_range * 0.786,
                    "fib_1.0":   c_price + trend_range * 1.0,
                    "fib_1.618": c_price + trend_range * 1.618,
                }

        elif direction == "Sell":
            # A: Highest high of the previous wave
            a_price = prior_wave_data['high'].max()
            # B: Lowest low of the previous wave
            b_price = prior_wave_data['low'].min()
            # C: Highest high of the current wave (the pullback)
            c_price = current_wave_data['high'].max()

            # Check for valid downtrend structure (C < A and B < A)
            if c_price < a_price and b_price < a_price:
                trend_range = a_price - b_price
                return {
                    "fib_A": a_price,
                    "fib_B": b_price,
                    "fib_C": c_price,
                    "fib_0.786": c_price - trend_range * 0.786,
                    "fib_1.0":   c_price - trend_range * 1.0,
                    "fib_1.618": c_price - trend_range * 1.618,
                }
    except Exception:
        return None
    
    return None # Invalid trend structure

def analyze_instrument(df):
    """
    Performs the core analysis with a tiered signal system.
    Returns (Signal, Setup, Trend, debug_data)
    """
    if df is None or len(df) < 120:
        return "Insufficient Data", "N/A", "N/A", {}

    # 1. Check Trend Structure
    trend_lookback = 120
    trend_direction = check_trend_structure(
        df['BBM_20'], df['EMA_200'], lookback=trend_lookback
    )

    latest = df.iloc[-1]
    
    debug_data = {
        'Price': latest['close'],
        'BBM_20': latest['BBM_20'],
        'EMA_200': latest['EMA_200'],
        'Low': latest['low'],
        'High': latest['high'],
    }
    
    # 2. Check for BB Squeeze
    squeeze_lookback = 120
    squeeze_percentile = 0.20
    historical_bandwidth = df['BB_WIDTH'].iloc[-squeeze_lookback:-1]
    
    if historical_bandwidth.empty:
        return "Insufficient Data", "Not enough squeeze data", trend_direction, debug_data
        
    squeeze_threshold = historical_bandwidth.quantile(squeeze_percentile)
    is_in_squeeze = latest['BB_WIDTH'] < squeeze_threshold

    debug_data['BB_Width'] = latest['BB_WIDTH']
    debug_data['Squeeze_Thresh'] = squeeze_threshold
    debug_data['Is_Squeeze'] = is_in_squeeze

    # 3. Check Proximity to 200 EMA
    proximity_pct = 0.03 # 3%
    is_near_ema = abs(latest['BBM_20'] - latest['EMA_200']) / latest['EMA_200'] < proximity_pct
    
    debug_data['SMA_Dist_EMA(%)'] = ((latest['BBM_20'] - latest['EMA_200']) / latest['EMA_200'])
    debug_data['Price_Dist_EMA_Low(%)'] = (latest['low'] - latest['EMA_200']) / latest['EMA_200']
    debug_data['Price_Dist_EMA_High(%)'] = (latest['high'] - latest['EMA_200']) / latest['EMA_200']

    # --- MAIN SIGNAL LOGIC ---

    # --- LOGIC BRANCH 1: SQUEEZE IS ACTIVE ---
    if is_in_squeeze:
        if "Bullish" in trend_direction:
            if is_near_ema:
                return "Strong Buy", f"{trend_direction} Trend + Squeeze at 200 EMA", trend_direction, debug_data
            else:
                recent_half_sma = df['BBM_20'].iloc[-int(trend_lookback/2):]
                is_at_higher_high = abs(latest['BBM_20'] - recent_half_sma.max()) / recent_half_sma.max() < 0.02 
                if is_at_higher_high:
                    setup_text = f"{trend_direction} Trend + Squeeze at Higher High"
                    
                    # *** UPDATED: Call with trend_lookback ***
                    fib_levels = calculate_fib_extension(df, "Buy", trend_lookback)
                    
                    if fib_levels:
                        debug_data["Fib_A"] = fib_levels["fib_A"]
                        debug_data["Fib_B"] = fib_levels["fib_B"]
                        debug_data["Fib_C"] = fib_levels["fib_C"]
                        debug_data["Fib_0.786"] = fib_levels["fib_0.786"]
                        debug_data["Fib_1.618"] = fib_levels["fib_1.618"]
                        
                        # *** UPDATED: is_extended logic checks if price > 0.786 ***
                        is_extended = (latest['close'] > fib_levels["fib_0.786"])
                        
                        if is_extended:
                            setup_text += " (Fib Extended)"
                    return "Moderate Buy", setup_text, trend_direction, debug_data

        elif "Bearish" in trend_direction:
            if is_near_ema:
                return "Strong Sell", f"{trend_direction} Trend + Squeeze at 200 EMA", trend_direction, debug_data
            else:
                recent_half_sma = df['BBM_20'].iloc[-int(trend_lookback/2):]
                is_at_lower_low = abs(latest['BBM_20'] - recent_half_sma.min()) / recent_half_sma.min() < 0.02
                if is_at_lower_low:
                    setup_text = f"{trend_direction} Trend + Squeeze at Lower Low"
                    
                    # *** UPDATED: Call with trend_lookback ***
                    fib_levels = calculate_fib_extension(df, "Sell", trend_lookback)
                    
                    if fib_levels:
                        debug_data["Fib_A"] = fib_levels["fib_A"]
                        debug_data["Fib_B"] = fib_levels["fib_B"]
                        debug_data["Fib_C"] = fib_levels["fib_C"]
                        debug_data["Fib_0.786"] = fib_levels["fib_0.786"]
                        debug_data["Fib_1.618"] = fib_levels["fib_1.618"]
                        
                        # *** UPDATED: is_extended logic checks if price < 0.786 ***
                        is_extended = (latest['close'] < fib_levels["fib_0.786"])
                        
                        if is_extended:
                            setup_text += " (Fib Extended)"
                    return "Moderate Sell", setup_text, trend_direction, debug_data

    # --- LOGIC BRANCH 2: SQUEEZE IS *NOT* ACTIVE (PULLBACK LOGIC) ---
    elif not is_in_squeeze:
        if is_near_ema:
            if "Bullish" in trend_direction:
                slope, _ = np.polyfit(np.arange(10), df['BBM_20'].iloc[-10:], 1)
                price_respects_support = latest['low'] > (latest['EMA_200'] * (1 - proximity_pct))
                
                if slope < 0 and price_respects_support:
                    return "Moderate Buy", f"{trend_direction} Pullback to 200 EMA", trend_direction, debug_data
            
            elif "Bearish" in trend_direction:
                slope, _ = np.polyfit(np.arange(10), df['BBM_20'].iloc[-10:], 1)
                price_respects_resistance = latest['high'] < (latest['EMA_200'] * (1 + proximity_pct))

                if slope > 0 and price_respects_resistance:
                    return "Moderate Sell", f"{trend_direction} Pullback to 200 EMA", trend_direction, debug_data

    # --- Default Case ---
    if trend_direction == "Indeterminate":
        return "Hold", "Indeterminate Trend", trend_direction, debug_data
    
    return "Hold", "Conditions Not Met", trend_direction, debug_data

# --- Main Execution ---

def run_multi_timeframe_analysis(tickers_to_analyze, status_callback=None):
    """
    Runs the full analysis pipeline.
    """
    results_list = []
    confirmation_timeframes = ["4h", "1h", "30m"]
    
    total_tickers = len(tickers_to_analyze)
    for i, ticker in enumerate(tickers_to_analyze):
        if status_callback:
            status_callback(f"Analyzing {ticker}... ({i+1}/{total_tickers})")
        
        # 1. Analyze the Daily Chart
        daily_df = get_data(ticker=ticker, interval="1d")
        daily_signal, daily_setup, daily_trend, debug_data = analyze_instrument(daily_df)
        
        final_signal = "Hold for now"
        confirmed_tfs = []

        # 2. Check lower timeframes
        if "Strong" in daily_signal or "Moderate" in daily_signal:
            direction = "Buy" if "Buy" in daily_signal else "Sell"
            final_signal = daily_signal
            
            for tf in confirmation_timeframes:
                time.sleep(0.5) 
                intraday_df = get_data(ticker=ticker, interval=tf)
                tf_signal, _, _, _ = analyze_instrument(intraday_df) 
                
                if direction in tf_signal: 
                    confirmed_tfs.append(tf)

            # 3. Upgrade the signal
            if "Strong" in daily_signal and confirmed_tfs:
                final_signal = f"Super Strong {direction}"
        
        # 4. Compile results
        result_row = {
            "Instrument": ticker,
            "Trend": daily_trend, 
            "Signal": final_signal,
            "Daily Setup": daily_setup,
            "Confirmation TFs": ", ".join(confirmed_tfs) if confirmed_tfs else "None"
        }
        
        # Format and add debug data
        formatted_debug_data = {}
        for k, v in debug_data.items():
            if isinstance(v, (float, np.floating)):
                if '%' in k:
                    formatted_debug_data[k] = f"{v * 100:.2f}%"
                else:
                    formatted_debug_data[k] = f"{v:.4f}"
            else:
                formatted_debug_data[k] = v
        
        result_row.update(formatted_debug_data)
        results_list.append(result_row)
        
        time.sleep(1) # Main delay between tickers

    # Column order is already correct from the last change
    column_order = [
        "Instrument", "Trend", "Signal", "Daily Setup", "Confirmation TFs",
        "Price", "BBM_20", "EMA_200", "Low", "High", "BB_Width", 
        "Squeeze_Thresh", "Is_Squeeze", "SMA_Dist_EMA(%)",
        "Price_Dist_EMA_Low(%)", "Price_Dist_EMA_High(%)",
        "Fib_A", "Fib_B", "Fib_C", "Fib_0.786", "Fib_1.618"
    ]
    
    return pd.DataFrame(results_list, columns=column_order)

# --- Example Usage (if you want to run this file directly) ---
if __name__ == '__main__':
    tickers = ["MSFT", "AAPL", "GOOGL", "EURUSD=X", "GBPUSD=X"]

    def print_status(message):
        print(message)

    analysis_results = run_multi_timeframe_analysis(tickers, status_callback=print_status)
    
    print("\n--- Trading Analysis Results ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    print(analysis_results)
