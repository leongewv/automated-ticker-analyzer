import yfinance as yf
import pandas_ta as ta
import pandas as pd
import numpy as np
import time

# --- Analysis Functions ---

def get_data(ticker, period="2y", interval="1d"):
    """Fetches and prepares data."""
    # For shorter intervals, yfinance limits the period
    if interval != "1d": 
        period = "60d" # Max period for intervals < 1d is 60 days
    data = yf.Ticker(ticker).history(period=period, interval=interval)
    # Require at least 200 bars for EMA calculation
    if data.empty or len(data) < 200: 
        return None
    
    data.ta.ema(length=200, append=True)
    data.ta.bbands(length=20, append=True)
    data.ta.atr(length=14, append=True)
    data['BB_WIDTH'] = (data['BBU_20_2.0'] - data['BBL_20_2.0']) / data['BBM_20_2.0']
    data = data.round(4)
    return data

def analyze_signal(df):
    """Analyzes a dataframe and returns the signal and setup type."""
    lookback_period = 120
    squeeze_percentile = 0.20

    if df is None or len(df) < lookback_period: return "Insufficient Data", "N/A"

    latest = df.iloc[-1]
    previous = df.iloc[-2]
    middle_bb = latest['BBM_20_2.0']
    ema_200 = latest['EMA_200']
    if pd.isna(middle_bb) or pd.isna(ema_200) or pd.isna(previous['BB_WIDTH']):
        return "Insufficient Data", "N/A"

    crossover_signal = "Hold"
    if middle_bb > ema_200: crossover_signal = "Buy"
    elif middle_bb < ema_200: crossover_signal = "Sell"

    historical_bandwidth = df['BB_WIDTH'].iloc[-lookback_period:-1]
    if historical_bandwidth.count() < lookback_period - 1: return crossover_signal, "Crossover"
    squeeze_threshold = historical_bandwidth.quantile(squeeze_percentile)
    is_squeeze_today = latest['BB_WIDTH'] < squeeze_threshold
    is_squeeze_yesterday = previous['BB_WIDTH'] < squeeze_threshold

    # Strong Signal Check 1: Squeeze Breakout
    if is_squeeze_yesterday and not is_squeeze_today:
        if crossover_signal == "Buy" and latest['Close'] > latest['BBU_20_2.0']:
            return "Strong Buy", "Breakout"
        if crossover_signal == "Sell" and latest['Close'] < latest['BBL_20_2.0']:
            return "Strong Sell", "Breakout"

    # Strong Signal Check 2: Squeeze Consolidation
    if is_squeeze_today:
        context_check_2 = False # Trend Slope
        trend_lookback = 60
        prices_for_trend = df.iloc[-trend_lookback:-1]
        time_index = np.arange(len(prices_for_trend))
        if crossover_signal == "Buy":
            slope, _ = np.polyfit(time_index, prices_for_trend['Low'], 1)
            if slope > 0: context_check_2 = True
        if crossover_signal == "Sell":
            slope, _ = np.polyfit(time_index, prices_for_trend['High'], 1)
            if slope < 0: context_check_2 = True
        
        context_check_3 = False # Pullback to 200 EMA
        is_near_ema = abs(middle_bb - ema_200) / ema_200 < 0.03

        if is_near_ema:
            past_price_period = df['Close'].iloc[-80:-20]
            # New "Buy" condition: previous high must be above the 200 EMA AND the current close.
            if crossover_signal == "Buy" and past_price_period.max() > ema_200 and past_price_period.max() > latest['Close']:
                context_check_3 = True
            # New "Sell" condition: previous low must be below the 200 EMA AND the current close.
            if crossover_signal == "Sell" and past_price_period.min() < ema_200 and past_price_period.min() < latest['Close']:
                context_check_3 = True
        
        # Check which consolidation conditions are met
        setup_details = []
        if context_check_2:
            setup_details.append("Trend Slope")
        if context_check_3:
            setup_details.append("Pullback to 200 EMA")

        # If any consolidation condition is met, create a descriptive signal
        if setup_details:
            signal = "Strong Buy" if crossover_signal == "Buy" else "Strong Sell"
            # Join the details for a descriptive setup type
            # e.g., "Consolidation (Pullback to 200 EMA)"
            setup_type = "Consolidation (" + " & ".join(setup_details) + ")"
            return signal, setup_type

    return crossover_signal, "Crossover"

def calculate_stop_loss(daily_df, direction):
    """Calculates a stop loss based on the 200 EMA, swing points, and ATR."""
    latest_ema_200 = daily_df['EMA_200'].iloc[-1]
    latest_atr = daily_df['ATRr_14'].iloc[-1]
    swing_lookback = 60
    
    if direction == "Buy":
        sl_ema = latest_ema_200 - latest_atr
        swing_low = daily_df['Low'].iloc[-swing_lookback:-1].min()
        return max(sl_ema, swing_low - latest_atr)
    elif direction == "Sell":
        sl_ema = latest_ema_200 + latest_atr
        swing_high = daily_df['High'].iloc[-swing_lookback:-1].max()
        return min(sl_ema, swing_high + latest_atr)
    return None

def calculate_take_profit(df, direction):
    """
    Calculates a take-profit level using Fibonacci Trend Extension.
    Identifies the three points (A, B, C) of a trend and projects the 161.8% level.
    """
    lookback = 90  # Lookback period to find the trend
    data = df.iloc[-lookback:]

    try:
        if direction == "Buy":
            # Point A: The start of the uptrend (lowest low)
            a_price = data['Low'].min()
            a_index = data['Low'].idxmin()

            # Point B: The peak of the uptrend (highest high after point A)
            b_data = data[a_index:]
            b_price = b_data['High'].max()
            b_index = b_data['High'].idxmin()

            # Point C: The retracement low (lowest low after point B)
            # This low must be higher than point A
            c_data = data[b_index:]
            c_price = c_data['Low'].min()
            
            if c_price > a_price:
                # Calculate the 1.618 extension level
                target = c_price + (b_price - a_price) * 1.618
                return f"{target:.4f}"
            else:
                return "N/A (Invalid Trend)"

        elif direction == "Sell":
            # Point A: The start of the downtrend (highest high)
            a_price = data['High'].max()
            a_index = data['High'].idxmax()

            # Point B: The bottom of the downtrend (lowest low after point A)
            b_data = data[a_index:]
            b_price = b_data['Low'].min()
            b_index = b_data['Low'].idxmin()

            # Point C: The retracement high (highest high after point B)
            # This high must be lower than point A
            c_data = data[b_index:]
            c_price = c_data['High'].max()

            if c_price < a_price:
                # Calculate the 1.618 extension level
                target = c_price - (a_price - b_price) * 1.618
                return f"{target:.4f}"
            else:
                return "N/A (Invalid Trend)"
                
    except Exception:
        # If any error occurs in finding points, return N/A
        return "N/A (Calc Error)"
        
    return "N/A"

def run_full_analysis(tickers_to_analyze, status_callback=None):
    """
    Runs the complete analysis for a list of tickers.
    :param tickers_to_analyze: A list of stock tickers.
    :param status_callback: An optional function to report progress (for Streamlit).
    :return: A pandas DataFrame with the analysis results.
    """
    results_list = []
    total_tickers = len(tickers_to_analyze)
    for i, ticker in enumerate(tickers_to_analyze):
        if status_callback:
            status_callback(f"Analyzing {ticker}... ({i+1}/{total_tickers})")
        
        daily_df = get_data(ticker=ticker, interval="1d")
        daily_signal, daily_setup_type = analyze_signal(daily_df)
        
        final_signal = daily_signal
        confirmation_status = "N/A"
        confirmation_setup_type = "N/A"
        entry_price = "N/A"
        stop_loss = "N/A"
        take_profit = "N/A" # New variable for take-profit
        
        if daily_signal in ["Strong Buy", "Strong Sell"]:
            intraday_df = get_data(ticker=ticker, interval="30m")
            
            if intraday_df is not None:
                confirmed_signal, confirmed_setup_type = analyze_signal(intraday_df)
                
                if (daily_signal == "Strong Buy" and confirmed_signal == "Strong Buy") or \
                   (daily_signal == "Strong Sell" and confirmed_signal == "Strong Sell"):
                    final_signal = "Super Strong Buy" if daily_signal == "Strong Buy" else "Super Strong Sell"
                    confirmation_status = "Pass"
                    confirmation_setup_type = confirmed_setup_type
                else:
                    confirmation_status = "Fail"
                
                direction = "Buy" if "Buy" in daily_signal else "Sell"
                entry_price = f"{intraday_df['Close'].iloc[-1]:.4f}"
                
                stop_loss_val = calculate_stop_loss(daily_df, direction)
                if stop_loss_val is not None:
                    stop_loss = f"{stop_loss_val:.4f}"
                
                # Calculate Take Profit
                take_profit = calculate_take_profit(daily_df, direction)

            else:
                confirmation_status = "30m Data Error"

        display_signal = final_signal if final_signal.startswith(("Super Strong", "Strong")) else "Hold for now"

        results_list.append({
            "Instrument": ticker,
            "Signal": display_signal,
            "Daily Setup": daily_setup_type,
            "Entry Price": entry_price,
            "Stop Loss": stop_loss,
            "Take Profit": take_profit, # New column
            "30m Confirmed": confirmation_status,
            "30m Setup": confirmation_setup_type
        })
        # yfinance has rate limits, a small delay is good practice for large lists
        time.sleep(1)
    
    return pd.DataFrame(results_list)