import yfinance as yf
from finta import TA
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
EMA_PERIOD = 200
BB_PERIOD = 20
BB_MULTIPLIER = 2.0
ENTRY_MIN_BARS = 3   
ENTRY_MAX_BARS = 10  
EXIT_MIN_BARS = 10
SL_BUFFER = 0.01

def get_live_tick(ticker):
    """Robustly fetches the latest price tick, handling yfinance Multi-Index issues."""
    try:
        # We fetch 5 days of 1m data to ensure we catch the market open on Mondays
        data = yf.download(ticker, period="5d", interval="1m", progress=False, group_by='ticker')
        if not data.empty:
            # Handle Multi-Index columns if present
            if isinstance(data.columns, pd.MultiIndex):
                # Take the last Close value for the specific ticker
                price_series = data[ticker]['Close'].dropna()
            else:
                price_series = data['Close'].dropna()
            
            if not price_series.empty:
                val = price_series.iloc[-1]
                # Ensure we return a float, not a Series
                return float(val), "LIVE"
    except Exception:
        pass
    return None, "HIST"

def check_economic_danger(ticker, eco_df, current_time=None):
    if eco_df is None or eco_df.empty: return "-"
    if current_time is None: current_time = datetime.now()
    clean_ticker = ticker.replace("/", "").replace("-", "").upper().replace("=X", "")
    currencies = [clean_ticker[:3], clean_ticker[3:]] if len(clean_ticker) == 6 else [clean_ticker]
    start_window, end_window = current_time, current_time + timedelta(hours=24)
    if not pd.api.types.is_datetime64_any_dtype(eco_df['Start']):
        eco_df['Start'] = pd.to_datetime(eco_df['Start'], errors='coerce')
    mask = (eco_df['Start'] >= start_window) & (eco_df['Start'] <= end_window) & \
           (eco_df['Currency'].isin(currencies)) & (eco_df['Impact'].isin(['HIGH', 'MEDIUM']))
    danger_events = eco_df[mask]
    if danger_events.empty: return "Safe"
    return " | ".join([f"{r['Currency']} {r['Name']} ({r['Impact']}) at {r['Start'].strftime('%H:%M')}" for _, r in danger_events.iterrows()])

def get_data(ticker, interval):
    period_map = {"30m": "1mo", "1h": "1y", "4h": "2y", "1d": "5y", "1wk": "max", "1mo": "max"}
    try:
        df = yf.Ticker(ticker).history(period=period_map.get(interval, "2y"), interval=interval)
        if df.empty or len(df) < 250: return None 
        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'], df['BB_UPPER'], df['BB_LOWER'] = bbands['BB_MIDDLE'], bbands['BB_UPPER'], bbands['BB_LOWER']
        df.dropna(inplace=True); return df
    except: return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    return "Uptrend" if last['BB_MID'] > last['EMA_200'] else ("Downtrend" if last['BB_MID'] < last['EMA_200'] else "Neutral")

def calculate_exact_cross(p_sma, c_sma, p_ema, c_ema):
    denom = (p_sma - c_sma) - (p_ema - c_ema)
    return (p_sma * c_ema - c_sma * p_ema) / denom if denom != 0 else c_sma

def get_bars_since_cross(df, direction):
    limit = min(500, len(df))
    window_df = df.iloc[-limit:]
    bb_mid, ema_200, dates = window_df['BB_MID'].values, window_df['EMA_200'].values, window_df.index
    for i in range(len(bb_mid) - 1, 0, -1):
        found = (direction == "Uptrend" and bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]) or \
                (direction == "Downtrend" and bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i])
        if found:
            return (len(bb_mid)-1)-i, dates[i], calculate_exact_cross(bb_mid[i-1], bb_mid[i], ema_200[i-1], ema_200[i])
    return None, None, None

def find_previous_opposing_cross(df, current_direction, entry_price):
    target = "Golden" if current_direction == "Downtrend" else "Death"
    bb_mid, ema_200, skipped = df['BB_MID'].values, df['EMA_200'].values, False 
    for i in range(len(bb_mid) - 1, 0, -1):
        found = (target == "Golden" and bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]) or \
                (target == "Death" and bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i])
        if found:
            price = calculate_exact_cross(bb_mid[i-1], bb_mid[i], ema_200[i-1], ema_200[i])
            if (current_direction == "Uptrend" and price > entry_price) or (current_direction == "Downtrend" and price < entry_price):
                return round(float(price), 4), f"Prev {target}" + (" (Deep)" if skipped else "")
            skipped = True
    return None, None

def find_next_sr_level(ticker, current_tf, direction, current_price):
    tf_order = ["4h", "1d", "1wk", "1mo"]
    try: next_tf = tf_order[tf_order.index(current_tf) + 1] if tf_order.index(current_tf) < 3 else "1mo"
    except: next_tf = "1mo"
    df = get_data(ticker, next_tf)
    if df is None: return "N/A", "No Data"
    df['is_high'], df['is_low'] = df['high'].rolling(5, center=True).max() == df['high'], df['low'].rolling(5, center=True).min() == df['low']
    if direction == "Uptrend":
        cands = [p for p in df[df['is_high']]['high'] if p > current_price]
        return (round(float(min(cands)), 4), f"Res on {next_tf}") if cands else ("ATH", "ATH")
    else:
        cands = [p for p in df[df['is_low']]['low'] if p < current_price]
        return (round(float(max(cands)), 4), f"Supp on {next_tf}") if cands else ("ATL", "ATL")

def check_early_exit(ticker, signal_tf, direction):
    tfs, opp = {"1mo":["1wk","1d","4h","1h","30m"], "1wk":["1d","4h","1h","30m"], "1d":["4h","1h","30m"], "4h":["1h","30m"]}, ("Downtrend" if direction == "Uptrend" else "Uptrend")
    warns = []
    for ltf in tfs.get(signal_tf, []):
        df = get_data(ticker, ltf)
        if df is not None and get_trend_status(df) == opp:
            bars, _, _ = get_bars_since_cross(df, opp)
            if bars and bars >= EXIT_MIN_BARS: warns.append(f"{ltf} Opp ({bars}b)")
    return " | ".join(warns) if warns else "Safe"

def analyze_ticker(ticker):
    log_trace, fallback = [], None
    live_p, price_status = get_live_tick(ticker)
    
    for tf_name in ["4h", "1d", "1wk", "1mo"]:
        df = get_data(ticker, tf_name)
        if df is None: continue
        status = get_trend_status(df)
        if status == "Neutral": continue
        
        bars_ago, cross_time, cross_price = get_bars_since_cross(df, status)
        if cross_price is None: 
            log_trace.append(f"{tf_name}:NoRecentCross")
            continue

        current_price = live_p if live_p is not None else float(df.iloc[-1]['close'])
        sl = cross_price * (1 - SL_BUFFER if status == "Uptrend" else 1 + SL_BUFFER)
        tp_price, tp_note = find_previous_opposing_cross(df, status, current_price)
        if tp_price is None: tp_price, tp_note = find_next_sr_level(ticker, tf_name, status, current_price)
        
        is_fresh = (bars_ago is not None and ENTRY_MIN_BARS <= bars_ago <= ENTRY_MAX_BARS)
        cross_str = cross_time.strftime('%Y-%m-%d %H:%M') if isinstance(cross_time, pd.Timestamp) else str(cross_time)
        
        result = {
            "Ticker": ticker, "Signal": f"{'CONFIRMED' if is_fresh else 'EXISTING'} {status.upper()}",
            "Timeframe": tf_name, "Current Price": round(current_price, 4), "Stop Loss": round(float(sl), 4),
            "Take Profit": f"{tp_price} ({tp_note})", "Exit Warning": check_early_exit(ticker, tf_name, status),
            "Cross Time": cross_str, "Remarks": f"[{price_status}] Trace: {' | '.join(log_trace + [f'{tf_name}:{bars_ago if bars_ago else 'Act'}'])}"
        }
        if is_fresh: return result
        if not fallback: fallback = result
    return fallback if fallback else {"Signal": "No Signal", "Remarks": f"Trace: {' | '.join(log_trace)}"}

def run_scanner(tickers, eco_df=None):
    results = []
    for ticker in tickers:
        try:
            res = analyze_ticker(ticker)
            danger = check_economic_danger(ticker, eco_df) if eco_df is not None else "-"
            if res.get("Signal") != "No Signal":
                res["Remarks"] = f"{danger} | {res.get('Remarks', '')}"
                results.append(res)
            else:
                results.append({"Ticker": ticker, "Signal": "No Signal", "Remarks": f"{danger} | {res.get('Remarks','')}"})
        except Exception as e:
            results.append({"Ticker": ticker, "Signal": "Error", "Remarks": str(e)})
    return pd.DataFrame(results)
