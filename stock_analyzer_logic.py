import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
import time

# --- Analysis Functions ---

def get_data(ticker, period="2y", interval="1d"):
    """Fetches and prepares data."""
    if interval != "1d":
        period = "60d"
    data = yf.Ticker(ticker).history(period=period, interval=interval)
    
    min_length = 200 if interval == "1d" else 100
    if data.empty or len(data) < min_length:
        return None

    data.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
    data['EMA_200'] = TA.EMA(data, period=200)
    bbands = TA.BBANDS(data, period=20)
    data['BBM_20_2.0'] = bbands['BB_MIDDLE']
    data['BBU_20_2.0'] = bbands['BB_UPPER']
    data['BBL_20_2.0'] = bbands['BB_LOWER']
    data['ATRr_14'] = TA.ATR(data, period=14)
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

    if is_squeeze_yesterday and not is_squeeze_today:
        if crossover_signal == "Buy" and latest['close'] > latest['BBU_20_2.0']:
            return "Strong Buy", "Breakout"
        if crossover_signal == "Sell" and latest['close'] < latest['BBL_20_2.0']:
            return "Strong Sell", "Breakout"

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

def confirm_30m_reversal(df, direction):
    """Top-tier check for 'Crazy Strong' signal."""
    if df is None or len(df) < 50 or 'EMA_200' not in df.columns or df['EMA_200'].isna().all(): return False
    try:
        lookback_period = 48
        recent_data = df.iloc[-lookback_period:]
        latest_ema_200 = recent_data['EMA_200'].iloc[-1]
        
        if direction == "Buy":
            was_below_ema = (recent_data['low'] < recent_data['EMA_200']).any()
            recent_half = recent_data.iloc[-int(lookback_period/2):]
            prior_half = recent_data.iloc[-lookback_period:-int(lookback_period/2)]
            is_making_hh_hl = (recent_half['high'].max() > prior_half['high'].max()) and \
                              (recent_half['low'].min() > prior_half['low'].min())
            recent_high_reclaimed_ema = recent_half['high'].max() >= latest_ema_200
            return was_below_ema and is_making_hh_hl and recent_high_reclaimed_ema
            
        elif direction == "Sell":
            was_above_ema = (recent_data['high'] > recent_data['EMA_200']).any()
            recent_half = recent_data.iloc[-int(lookback_period/2):]
            prior_half = recent_data.iloc[-lookback_period:-int(lookback_period/2)]
            is_making_ll_lh = (recent_half['low'].min() < prior_half['low'].min()) and \
                              (recent_half['high'].max() < prior_half['high'].max())
            recent_low_breached_ema = recent_half['low'].min() <= latest_ema_200
            return was_above_ema and is_making_ll_lh and recent_low_breached_ema
            
    except Exception: return False
    return False

def confirm_30m_trend(df, direction):
    """Second-tier check for 'Moderate Strong' signal."""
    if df is None or len(df) < 24: return False
    try:
        lookback = 24
        recent_half = df.iloc[-int(lookback/2):]
        prior_half = df.iloc[-lookback:-int(lookback/2)]
        if direction == "Buy":
            return (recent_half['high'].max() > prior_half['high'].max()) and \
                   (recent_half['low'].min() > prior_half['low'].min())
        elif direction == "Sell":
            return (recent_half['low'].min() < prior_half['low'].min()) and \
                   (recent_half['high'].max() < prior_half['high'].max())
    except Exception: return False
    return False

def run_full_analysis(tickers_to_analyze, status_callback=None):
    """Runs the complete analysis with the new signal hierarchy."""
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
            direction = "Buy" if "Buy" in daily_signal else "Sell"
            intraday_df = get_data(ticker=ticker, interval="30m")
            
            if intraday_df is not None:
                # --- NEW CONFIRMATION HIERARCHY ---
                # 1. Check for "Crazy Strong" (Top Priority)
                if confirm_30m_reversal(intraday_df, direction):
                    final_signal = f"Crazy Strong {direction}"
                    confirmation_status = "Pass (Reversal)"
                    confirmation_setup_type = "30m Reclaimed EMA"
                else:
                    # 2. Check for "Super Strong" (Alignment)
                    confirmed_signal, confirmed_setup = analyze_signal(intraday_df)
                    if f"Strong {direction}" == confirmed_signal:
                        final_signal = f"Super Strong {direction}"
                        confirmation_status = "Pass (Alignment)"
                        confirmation_setup_type = confirmed_setup
                    # 3. Check for "Moderate Strong" (Early Trend)
                    elif confirm_30m_trend(intraday_df, direction):
                        final_signal = f"Moderate Strong {direction}"
                        confirmation_status = "Pass (Early Trend)"
                        confirmation_setup_type = "Higher Highs/Lows" if direction == "Buy" else "Lower Lows/Highs"
                    # 4. If none pass, confirmation fails
                    else:
                        confirmation_status = "Fail"
                
                # Calculate trade params if ANY confirmation passed
                if "Pass" in confirmation_status:
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
        
        time.sleep(1) # To avoid rate limiting
    
    column_order = ["Instrument", "Signal", "Daily Setup", "Entry Price", "Stop Loss", "TP1 (Structure)",
                    "TP2 (Fib 0.718)", "TP3 (Fib 1.0)", "TP4 (Fib 1.618)", "30m Confirmed", "30m Setup"]
    return pd.DataFrame(results_list, columns=column_order)
