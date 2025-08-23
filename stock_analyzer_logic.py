import yfinance as yf
import pandas_ta as ta
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
            if crossover_signal == "Buy" and past_price_period.max() > ema_200 and past_price_period.max() > latest['Close']:
                context_check_3 = True
            if crossover_signal == "Sell" and past_price_period.min() < ema_200 and past_price_period.min() < latest['Close']:
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
        swing_low = daily_df['Low'].iloc[-swing_lookback:-1].min()
        return max(sl_ema, swing_low - latest_atr)
    elif direction == "Sell":
        sl_ema = latest_ema_200 + latest_atr
        swing_high = daily_df['High'].iloc[-swing_lookback:-1].max()
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
            a_price_struct = data['Low'].min()
            a_index_struct = data['Low'].idxmin()
            b_data_struct = data[a_index_struct:]
            b_price_struct = b_data_struct['High'].max()
            targets["TP1 (Structure)"] = f"{b_price_struct:.4f}"
            b_index_fib = b_data_struct['High'].idxmin()
            c_data_fib = data[b_index_fib:]
            c_price_fib = c_data_fib['Low'].min()
            if c_price_fib > a_price_struct:
                trend_range = b_price_struct - a_price_struct
                targets["TP2 (Fib 0.718)"] = f"{c_price_fib + trend_range * 0.718:.4f}"
                targets["TP3 (Fib 1.0)"] = f"{c_price_fib + trend_range * 1.0:.4f}"
                targets["TP4 (Fib 1.618)"] = f"{c_price_fib + trend_range * 1.618:.4f}"
        elif direction == "Sell":
            a_price_struct = data['High'].max()
            a_index_struct = data['High'].idxmax()
            b_data_struct = data[a_index_struct:]
            b_price_struct = b_data_struct['Low'].min()
            targets["TP1 (Structure)"] = f"{b_price_struct:.4f}"
            b_index_fib = b_data_struct['Low'].idxmin()
            c_data_fib = data[b_index_fib:]
            c_price_fib = c_data_fib['High'].max()
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
            is_higher_high = recent_half['High'].max() > prior_half['High'].max()
            is_higher_low = recent_half['Low'].min() > prior_half['Low'].min()
            return is_higher_high and is_higher_low
        elif direction == "Sell":
            is_lower_high = recent_half['High'].max() < prior_half['High'].max()
            is_lower_low = recent_half['Low'].min() < prior_half['Low'].min()
            return is_lower_high and is_lower_low
    except Exception:
        return False
    return False

def run_full_analysis(tickers_to_analyze, status_callback=None):
    """
    Runs the full analysis pipeline for a list of tickers.
    """
    results_list = []
    column_order = [
        "Instrument", "Signal", "Daily Setup", "Entry Price", "Stop Loss",
        "TP1 (Structure)", "TP2 (Fib 0.718)", "TP3 (Fib 1.0)", "TP4 (Fib 1.618)",
        "30m Confirmed", "30m Setup"
    ]

    for i, ticker in enumerate(tickers_to_analyze):
        if status_callback:
            status_callback(f"Analyzing ({i+1}/{len(tickers_to_analyze)}): {ticker}...")

        try:
            daily_df = get_data(ticker, period="1y", interval="1d")
            thirty_m_df = get_data(ticker, period="60d", interval="30m")

            daily_signal, daily_setup = analyze_signal(daily_df)
            entry, sl, tp1, tp2, tp3, tp4 = (None,) * 6
            thirty_m_confirmed_status = "N/A"
            thirty_m_setup = "N/A"
            final_signal = daily_signal

            if "Strong" in daily_signal:
                entry, sl, tp1, tp2, tp3, tp4 = calculate_levels(daily_df, daily_signal)
                confirmed, thirty_m_setup = confirm_30m_trend(thirty_m_df, daily_signal)

                if confirmed:
                    if "Reversal" in thirty_m_setup:
                        final_signal = f"Moderate {daily_signal}"
                        thirty_m_confirmed_status = "Pass (Reversal)"
                    else:
                        final_signal = f"Super {daily_signal}"
                        thirty_m_confirmed_status = "Pass"
                else:
                    final_signal = daily_signal
                    thirty_m_confirmed_status = "Fail"
            
            display_signal = final_signal if "Strong" in final_signal else "Hold for now"

            result_row = {
                "Instrument": ticker, "Signal": display_signal, "Daily Setup": daily_setup,
                "Entry Price": entry, "Stop Loss": sl, "TP1 (Structure)": tp1,
                "TP2 (Fib 0.718)": tp2, "TP3 (Fib 1.0)": tp3, "TP4 (Fib 1.618)": tp4,
                "30m Confirmed": thirty_m_confirmed_status, "30m Setup": thirty_m_setup
            }
            results_list.append(result_row)

        except Exception as e:
            if status_callback:
                status_callback(f"  -> Error analyzing {ticker}: {e}")
            results_list.append({"Instrument": ticker, "Signal": "Error", "Daily Setup": str(e)})

    return pd.DataFrame(results_list, columns=column_order).fillna("N/A")
