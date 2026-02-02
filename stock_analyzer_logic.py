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

# --- Improved Helper for Real-Time Price ---
def get_live_tick(ticker):
    """Fetches the absolute latest price using fast_info or 1m data."""
    try:
        t = yf.Ticker(ticker)
        # 1. Try fast_info (Direct ticker feed)
        price = t.fast_info.get('lastPrice')
        if price and not np.isnan(price):
            return price, "Live"
            
        # 2. Fallback: 1m download
        data = yf.download(ticker, period="1d", interval="1m", progress=False)
        if not data.empty:
            # Handle possible MultiIndex from yf.download
            if isinstance(data.columns, pd.MultiIndex):
                return data[ticker]['Close'].iloc[-1], "Live(1m)"
            return data['Close'].iloc[-1], "Live(1m)"
    except:
        pass
    return None, "Stale"

# --- Economic Danger Logic (Preserved) ---
def check_economic_danger(ticker, eco_df, current_time=None):
    if eco_df is None or eco_df.empty: return "-"
    if current_time is None: current_time = datetime.now()
    clean_ticker = ticker.replace("/", "").replace("-", "").upper().replace("=X", "")
    currencies = [clean_ticker[:3], clean_ticker[3:]] if len(clean_ticker) == 6 else [clean_ticker]
    start_window = current_time
    end_window = current_time + timedelta(hours=24)
    if not pd.api.types.is_datetime64_any_dtype(eco_df['Start']):
        eco_df['Start'] = pd.to_datetime(eco_df['Start'], errors='coerce')
    mask = ((eco_df['Start'] >= start_window) & (eco_df['Start'] <= end_window) & 
            (eco_df['Currency'].isin(currencies)) & (eco_df['Impact'].isin(['HIGH', 'MEDIUM'])))
    danger_events = eco_df[mask]
    if danger_events.empty: return "Safe"
    warnings = [f"{r['Currency']} {r['Name']} ({r['Impact']}) at {r['Start'].strftime('%H:%M')}" for _, r in danger_events.iterrows()]
    return " | ".join(warnings)

# --- Data & Indicators ---
def get_data(ticker, interval):
    period_map = {"30m": "1mo", "1h": "1y", "4h": "2y", "1d": "5y", "1wk": "max", "1mo": "max"}
    try:
        df = yf.Ticker(ticker).history(period=period_map.get(interval, "2y"), interval=interval)
        if df.empty or len(df) < 250: return None 
        df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        df['EMA_200'] = TA.EMA(df, period=EMA_PERIOD)
        bbands = TA.BBANDS(df, period=BB_PERIOD, std_multiplier=BB_MULTIPLIER)
        df['BB_MID'], df['BB_UPPER'], df['BB_LOWER'] = bbands['BB_MIDDLE'], bbands['BB_UPPER'], bbands['BB_LOWER']
        df.dropna(inplace=True)
        return df
    except Exception: return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    return "Uptrend" if last['BB_MID'] > last['EMA_200'] else ("Downtrend" if last['BB_MID'] < last['EMA_200'] else "Neutral")

def calculate_exact_cross(prev_sma, curr_sma, prev_ema, curr_ema):
    denom = (prev_sma - curr_sma) - (prev_ema - curr_ema)
    return (prev_sma * curr_ema - curr_sma * prev_ema) / denom if denom != 0 else curr_sma

def get_bars_since_cross(df, direction):
    limit = min(500, len(df))
    window_df = df.iloc[-limit:]
    bb_mid, ema_200, dates = window_df['BB_MID'].values, window_df['EMA_200'].values, window_df.index
    for i in range(len(bb_mid) - 1, 0, -1):
        found = (direction == "Uptrend" and bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]) or \
                (direction == "Downtrend" and bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i])
        if found:
            return (len(bb_mid) - 1) - i, dates[i], calculate_exact_cross(bb_mid[i-1], bb_mid[i], ema_200[i-1], ema_200[i])
    return None, None, None

def find_previous_opposing_cross(df, current_direction, entry_price):
    target_type = "Golden" if current_direction == "Downtrend" else "Death"
    bb_mid, ema_200 = df['BB_MID'].values, df['EMA_200'].values
    skipped = False 
    for i in range(len(bb_mid) - 1, 0, -1):
        found = (target_type == "Golden" and bb_mid[i-1] <= ema_200[i-1] and bb_mid[i] > ema_200[i]) or \
                (target_type == "Death" and bb_mid[i-1] >= ema_200[i-1] and bb_mid[i] < ema_200[i])
        if found:
            price = calculate_exact_cross(bb_mid[i-1], bb_mid[i], ema_200[i-1], ema_200[i])
            valid = (current_direction == "Uptrend" and price > entry_price) or (current_direction == "Downtrend" and price < entry_price)
            if valid: return round(price, 4), f"Prev {target_type}" + (" (Deep)" if skipped else "")
            skipped = True
    return None, None

def find_next_sr_level(ticker, current_tf, direction, current_price):
    tf_order = ["4h", "1d", "1wk", "1mo"]
    try: next_tf = tf_order[tf_order.index(current_tf) + 1] if tf_order.index(current_tf) < 3 else "1mo"
    except: next_tf = "1mo"
    df = get_data(ticker, next_tf)
    if df is None: return "N/A", "No Data"
    df['is_high'] = df['high'].rolling(5, center=True).max() == df['high']
    df['is_low'] = df['low'].rolling(5, center=True).min() == df['low']
    if direction == "Uptrend":
        cands = [p for p in df[df['is_high']]['high'] if p > current_price]
        return (round(min(cands), 4), f"Res on {next_tf}") if cands else ("ATH", "ATH")
    else:
        cands = [p for p in df[df['is_low']]['low'] if p < current_price]
        return (round(max(cands), 4), f"Supp on {next_tf}") if cands else ("ATL", "ATL")

def check_early_exit(ticker, signal_tf, direction):
    lower_tf_map = {"1mo": ["1wk", "1d", "4h", "1h", "30m"], "1wk": ["1d", "4h", "1h", "30m"], "1d": ["4h", "1h", "30m"], "4h": ["1h", "30m"]}
    warns, opp = [], ("Downtrend" if direction == "Uptrend" else "Uptrend")
    for l_tf in lower_tf_map.get(signal_tf, []):
        df = get_data(ticker, l_tf)
        if df is not None and get_trend_status(df) == opp:
            bars, _, _ = get_bars_since_cross(df, opp)
            if bars and bars >= EXIT_MIN_BARS: warns.append(f"{l_tf} Opp ({bars}b)")
    return " | ".join(warns) if warns else "Safe"

# --- Core Scanner Logic ---
def analyze_ticker(ticker):
    log_trace, fallback = [], None
    # FETCH LIVE PRICE ONCE PER TICKER
    live_p, price_src = get_live_tick(ticker)
    
    for tf_name in ["4h", "1d", "1wk", "1mo"]:
        df = get_data(ticker, tf_name)
        if df is None: 
            log_trace.append(f"{tf_name}:NoData")
            continue
        status = get_trend_status(df)
        if status == "Neutral":
            log_trace.append(f"{tf_name}:Neut")
            continue
        bars, cross_time, cross_price = get_bars_since_cross(df, status)
        if cross_price is None:
            log_trace.append(f"{tf_name}:NoCross")
            continue
        log_trace.append(f"{tf_name}:{bars if bars is not None else 'Act'}")
        
        # USE LIVE PRICE IF AVAILABLE, ELSE FALLBACK TO BAR CLOSE
        current_price = live_p if live_p is not None else df.iloc[-1]['close']
        sl = round(cross_price * (1 - SL_BUFFER if status == "Uptrend" else 1 + SL_BUFFER), 4)
        tp_p, tp_n = find_previous_opposing_cross(df, status, current_price)
        if not tp_p: tp_p, tp_n = find_next_sr_level(ticker, tf_name, status, current_price)
        
        is_fresh = (bars is not None and ENTRY_MIN_BARS <= bars <= ENTRY_MAX_BARS)
        cross_str = cross_time.strftime('%Y-%m-%d %H:%M') if isinstance(cross_time, pd.Timestamp) else str(cross_time)
        
        result = {
            "Ticker": ticker, "Signal": f"{'CONFIRMED' if is_fresh else 'EXISTING'} {status.upper()}",
            "Timeframe": tf_name, "Current Price": round(current_price, 4), "Stop Loss": sl,
            "Take Profit": f"{tp_p} ({tp_n})", "Exit Warning": check_early_exit(ticker, tf_name, status),
            "Cross Time": cross_str, "PriceSource": price_src, "Trace": " | ".join(log_trace)
        }
        if is_fresh: return result
        if fallback is None: fallback = result

    return fallback if fallback else {"Signal": "No Signal", "Trace": " | ".join(log_trace), "PriceSource": price_src}

def run_scanner(tickers, eco_df=None):
    results = []
    for i, ticker in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Checking {ticker}...", end="\r")
        try: analysis = analyze_ticker(ticker)
        except Exception as e: analysis = {"Signal": "Error", "Trace": str(e)}
        
        danger = check_economic_danger(ticker, eco_df) if eco_df is not None else "-"
        p_src = analysis.get("PriceSource", "?")
        trace = analysis.get("Trace", "")
        
        if "Signal" in analysis and analysis["Signal"] != "No Signal":
            results.append({
                "Ticker": ticker, "Signal": analysis["Signal"], "Timeframe": analysis["Timeframe"],
                "Current Price": analysis["Current Price"], "Stop Loss": analysis["Stop Loss"],
                "Take Profit": analysis["Take Profit"], "Exit Warning": analysis.get("Exit Warning", "-"),
                "Cross Time": analysis["Cross Time"], "Remarks": f"[{p_src}] {danger} | Trace: {trace}"
            })
        else:
            results.append({"Ticker": ticker, "Signal": "No Signal", "Remarks": f"{danger} | Trace: {trace}"})
    return pd.DataFrame(results)
