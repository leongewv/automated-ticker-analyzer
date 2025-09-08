import yfinance as yf
from finta import TA  # Replaced pandas_ta with finta
import pandas as pd
import numpy as np
import time

# --- Analysis Functions ---

def get_data(ticker, period="2y", interval="1d"):
    """Fetches and prepares data."""
    if interval != "1d":
        period = "60d"
    data = yf.Ticker(ticker).history(period=period, interval=interval)
    if data.empty or len(data) < 200:
        return None

    # --- INDICATOR CALCULATION UPDATED FOR FINTA ---
    # Finta requires lowercase column names
    data.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

    # Calculate EMA
    data['EMA_200'] = TA.EMA(data, period=200)

    # Calculate Bollinger Bands
    bbands = TA.BBANDS(data, period=20)
    # Rename finta's default columns to match the old pandas-ta names
    data['BBM_20_2.0'] = bbands['BB_MIDDLE']
    data['BBU_20_2.0'] = bbands['BB_UPPER']
    data['BBL_20_2.0'] = bbands['BB_LOWER']

    # Calculate ATR
    # Rename finta's default 'ATR' column to the old 'ATRr_14'
    data['ATRr_14'] = TA.ATR(data, period=14)

    # This calculation remains the same as it uses the columns created above
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
        if crossover_signal == "Buy" and latest['close'] > latest['BBU_20_2.0']:
            return "Strong Buy", "Breakout"
        if crossover_signal == "Sell" and latest['close'] < latest['BBL_20_2.0']:
            return "Strong Sell", "Breakout"

    # Strong Signal Check 2: Squeeze Consolidation
    if is_squeeze_today:
        context_check_2 = False # Trend Slope
        trend_lookback = 60
        prices_for_trend = df.iloc[-trend_lookback:-1]
        time_index = np.arange(len(prices_for_trend))
        if crossover_signal == "Buy":
            slope, _ = np.polyfit(time_index, prices_for_trend['low'], 1)
            if slope > 0: context_check_2 = True
        if crossover_signal == "Sell":
            slope, _ = np.polyfit(time_index, prices_for_trend['high'], 1)
            if slope < 0: context_check_2 = True
        
        context_check_3 = False # Pullback to 200 EMA
        is_near_ema = abs(middle_bb - ema_200) / ema_200 < 0.03

        if is_near_ema:
            past_price_period = df['close'].iloc[-80:-20]
            if crossover_signal == "Buy" and past_price_period.max() > ema_200 and past_price_period.max() > latest['close']:
                context_check_3 = True
            if crossover_signal == "Sell" and past_price_period.min() < ema_200 and past_price_period.min() < latest['close']:
                context_check_3 = True
        
        setup_details = []
        if context_check_2: setup_details.append("Trend Slope")
        if context_check_3: setup_details.append("Pullback to 200 EMA")

        if setup_details:
            signal = "Strong Buy" if crossover_signal == "Buy" else "Strong Sell"
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
        swing_low = daily_df['low'].iloc[-swing_lookback:-1].min()
        return max(sl_ema, swing_low - latest_atr)
    elif direction == "Sell":
        sl_ema = latest_ema_200 + latest_atr
        swing_high = daily_df['high'].iloc[-swing_lookback:-1].max()
        return min(sl_ema, swing_high + latest_atr)
    return None

def calculate_take_profit(df, direction):
    """
    Calculates multiple take-profit levels.
    TP1 (Structure) is independent. TP2-4 (Fibonacci) depend on a valid trend.
    """
    lookback = 90
    data = df.iloc[-lookback:]
    targets = {
        "TP1 (Structure)": "N/A", "TP2 (Fib 0.718)": "N/A",
        "TP3 (Fib 1.0)": "N/A", "TP4 (Fib 1.618)": "N/A",
    }
    try:
        if direction == "Buy":
            a_price_struct = data['low'].min()
            a_index_struct = data['low'].idxmin()
            b_data_struct = data[a_index_struct:]
            b_price_struct = b_data_struct['high'].max()
            targets["TP1 (Structure)"] = f"{b_price_struct:.4f}"
            b_index_fib = b_data_struct['high'].idxmin()
            c_data_fib = data[b_index_fib:]
            c_price_fib = c_data_fib['low'].min()
            if c_price_fib > a_price_struct:
                trend_range = b_price_struct - a_price_struct
                targets["TP2 (Fib 0.718)"] = f"{c_price_fib + trend_range * 0.718:.4f}"
                targets["TP3 (Fib 1.0)"] = f"{c_price_fib + trend_range * 1.0:.4f}"
                targets["TP4 (Fib 1.618)"] = f"{c_price_fib + trend_range * 1.618:.4f}"
        elif direction == "Sell":
            a_price_struct = data['high'].max()
            a_index_struct = data['high'].idxmax()
            b_data_struct = data[a_index_struct:]
            b_price_struct = b_data_struct['low'].min()
            targets["TP1 (Structure)"] = f"{b_price_struct:.4f}"
            b_index_fib = b_data_struct['low'].idxmin()
            c_data_fib = data[b_index_fib:]
            c_price_fib = c_data_fib['high'].max()
            if c_price_fib < a_price_struct:
                trend_range = a_price_struct - b_price_struct
                targets["TP2 (Fib 0.718)"] = f"{c_price_fib - trend_range * 0.718:.4f}"
                targets["TP3 (Fib 1.0)"] = f"{c_price_fib - trend_range * 1.0:.4f}"
                targets["TP4 (Fib 1.618)"] = f"{c_price_fib - trend_range * 1.618:.4f}"
    except Exception:
        return targets
    return targets

def confirm_30m_trend(df, direction):
    """
    Checks for a developing trend (reversal) on the 30-minute chart.
    Looks for higher highs & lows (for a buy) or lower lows & highs (for a sell).
    """
    if df is None or len(df) < 24:
        return False
    try:
        lookback = 24
        recent_half = df.iloc[-int(lookback/2):]
        prior_half = df.iloc[-lookback:-int(lookback/2)]
        if direction == "Buy":
            is_higher_high = recent_half['high'].max() > prior_half['high'].max()
            is_higher_low = recent_half['low'].min() > prior_half['low'].min()
            return is_higher_high and is_higher_low
        elif direction == "Sell":
            is_lower_high = recent_half['high'].max() < prior_half['high'].max()
            is_lower_low = recent_half['low'].min() < prior_half['low'].min()
            return is_lower_high and is_lower_low
    except Exception:
        return False
    return False

def run_full_analysis(tickers_to_analyze, status_callback=None):
    """
    Runs the complete analysis for a list of tickers.
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
        take_profit_levels = {
            "TP1 (Structure)": "N/A", "TP2 (Fib 0.718)": "N/A",
            "TP3 (Fib 1.0)": "N/A", "TP4 (Fib 1.618)": "N/A"
        }
        
        if daily_signal in ["Strong Buy", "Strong Sell"]:
            intraday_df = get_data(ticker=ticker, interval="30m")
            direction = "Buy" if "Buy" in daily_signal else "Sell"
            
            if intraday_df is not None:
                # Standard confirmation check
                confirmed_signal, confirmed_setup_type = analyze_signal(intraday_df)
                
                # --- NEW CONFIRMATION LOGIC ---
                # 1. Check for "Super Strong" signal (perfect alignment)
                if (daily_signal == "Strong Buy" and confirmed_signal == "Strong Buy") or \
                   (daily_signal == "Strong Sell" and confirmed_signal == "Strong Sell"):
                    final_signal = "Super Strong Buy" if direction == "Buy" else "Super Strong Sell"
                    confirmation_status = "Pass"
                    confirmation_setup_type = confirmed_setup_type
                
                # 2. Check for "Moderate Strong" signal (early reversal)
                elif confirm_30m_trend(intraday_df, direction):
                    final_signal = "Moderate Strong Buy" if direction == "Buy" else "Moderate Strong Sell"
                    confirmation_status = "Pass (Reversal)"
                    confirmation_setup_type = "Higher Highs/Lows" if direction == "Buy" else "Lower Lows/Highs"

                # 3. If neither, confirmation fails
                else:
                    confirmation_status = "Fail"
                
                entry_price = f"{intraday_df['close'].iloc[-1]:.4f}"
                stop_loss_val = calculate_stop_loss(daily_df, direction)
                if stop_loss_val is not None:
                    stop_loss = f"{stop_loss_val:.4f}"
                take_profit_levels = calculate_take_profit(daily_df, direction)
            else:
                confirmation_status = "30m Data Error"

        display_signal = final_signal if "Strong" in final_signal else "Hold for now"

        result_row = {"Instrument": ticker, "Signal": display_signal, "Daily Setup": daily_setup_type,
                      "Entry Price": entry_price, "Stop Loss": stop_loss}
        result_row.update(take_profit_levels)
        result_row.update({"30m Confirmed": confirmation_status, "30m Setup": confirmation_setup_type})
        results_list.append(result_row)
        
        time.sleep(1)
    
    column_order = ["Instrument", "Signal", "Daily Setup", "Entry Price", "Stop Loss", "TP1 (Structure)",
                    "TP2 (Fib 0.718)", "TP3 (Fib 1.0)", "TP4 (Fib 1.618)", "30m Confirmed", "30m Setup"]
    return pd.DataFrame(results_list, columns=column_order)
