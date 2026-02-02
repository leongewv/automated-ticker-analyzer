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

def get_live_price(ticker_str):
    """Fetches the most recent price using fast_info or 1m download."""
    try:
        t = yf.Ticker(ticker_str)
        # 1. Try fast_info for real-time tick
        price = t.fast_info['lastPrice']
        if price and not np.isnan(price):
            return price, "Live"
            
        # 2. Backup: 1m download
        live_data = yf.download(ticker_str, period="1d", interval="1m", progress=False)
        if not live_data.empty:
            return live_data['Close'].iloc[-1], "Live(1m)"
    except:
        pass
    return None, "Hist"

def check_economic_danger(ticker, eco_df, current_time=None):
    if eco_df is None or eco_df.empty: return "-"
    if current_time is None: current_time = datetime.now()
    clean_ticker = ticker.replace("/", "").replace("-", "").upper().replace("=X", "")
    currencies = [clean_ticker[:3], clean_ticker[3:]] if len(clean_ticker) == 6 else [clean_ticker]
    start_window = current_time
    end_window = current_time + timedelta(hours=24)
    if not pd.api.types.is_datetime64_any_dtype(eco_df['Start']):
        eco_df['Start'] = pd.to_datetime(eco_df['Start'], errors='coerce')
    mask = (
        (eco_df['Start'] >= start_window) & (eco_df['Start'] <= end_window) & 
        (eco_df['Currency'].isin(currencies)) & (eco_df['Impact'].isin(['HIGH', 'MEDIUM']))
    )
    danger_events = eco_df[mask]
    if danger_events.empty: return "Safe"
    warnings = [f"{r['Currency']} {r['Name']}({r['Impact']})@{r['Start'].strftime('%H:%M')}" for _, r in danger_events.iterrows()]
    return " | ".join(warnings)

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
    except: return None

def get_trend_status(df):
    if df is None or len(df) < 1: return "None"
    last = df.iloc[-1]
    return "Uptrend" if last['BB_MID'] > last['EMA_200'] else ("Downtrend" if last['BB_MID'] < last['EMA_200'] else "Neutral")

def calculate_exact_cross(p_sma, c_sma, p_ema, c_ema):
    denom = (p_sma - c_sma) - (p_ema - c_ema)
    return (p_sma * c_ema - c_sma * p_ema) / denom if denom != 0 else c_sma

def get_bars_since_cross(df, direction):
    limit = min(len(df), 500)
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
                return round(price, 4), f"Prev {target}" + (" (Deep)" if skipped else "")
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
    tfs = {"1mo":["1wk","1d","4h","1h","30m"], "1wk":["1d","4h","1h","30m"], "1d":["4h","1h","30m"], "4h":["1h","30m"]}
    opp, warns = ("Downtrend" if direction == "Uptrend" else "Uptrend"), []
    for ltf in tfs.get(signal_tf, []):
        df = get_data(ticker, ltf)
        if df is not None and get_trend_status(df) == opp:
            bars, _, _ = get_bars_since_cross(df, opp)
            if bars and bars >= EXIT_MIN_BARS: warns.append(f"{ltf}({bars}b)")
    return " | ".join(warns) if warns else "Safe"

def analyze_ticker(ticker):
    log_trace, fallback, live_price, price_src = [], None, *get_live_price(ticker)
    for tf in ["4h", "1d", "1wk", "1mo"]:
        df = get_data(ticker, tf)
        if df is None: 
            log_trace.append(f"{tf}:NoData")
            continue
        status = get_trend_status(df)
        if status == "Neutral":
            log_trace.append(f"{tf}:Neut")
            continue
        bars, cross_time, cross_price = get_bars_since_cross(df, status)
        log_trace.append(f"{tf}:{bars if bars else 'Act'}")
        
        rep_price = live_price if live_price else df.iloc[-1]['close']
        sl = round(cross_price * (1 - SL_BUFFER if status == "Uptrend" else 1 + SL_BUFFER), 4)
        tp_p, tp_n = find_previous_opposing_cross(df, status, rep_price)
        if not tp_p: tp_p, tp_n = find_next_sr_level(ticker, tf, status, rep_price)
        
        is_fresh = bars is not None and ENTRY_MIN_BARS <= bars <= ENTRY_MAX_BARS
        res = {
            "Ticker": ticker, "Signal": f"{'CONFIRMED' if is_fresh else 'EXISTING'} {status.upper()}",
            "Timeframe": tf, "Current Price": round(rep_price, 4), "Stop Loss": sl,
            "Take Profit": f"{tp_p} ({tp_n})", "Exit Warning": check_early_exit(ticker, tf, status),
            "Cross Time": cross_time.strftime('%Y-%m-%d %H:%M') if cross_time else "-",
            "Trace": " | ".join(log_trace), "PriceSrc": price_src
        }
        if is_fresh: return res
        if not fallback: fallback = res
    return fallback if fallback else {"Signal": "No Signal", "Trace": " | ".join(log_trace)}

def run_scanner(tickers, eco_df=None):
    results = []
    for ticker in tickers:
        try: 
            res = analyze_ticker(ticker)
            eco = check_economic_danger(ticker, eco_df) if eco_df is not None else "-"
            if "Signal" in res and res["Signal"] != "No Signal":
                res["Remarks"] = f"[{res.get('PriceSrc', 'H')}] {eco} | Trace: {res.get('Trace','')}"
                results.append(res)
            else:
                results.append({"Ticker": ticker, "Signal": "No Signal", "Remarks": f"{eco} | Trace: {res.get('Trace','')}"})
        except Exception as e:
            results.append({"Ticker": ticker, "Signal": "Error", "Remarks": str(e)})
    return pd.DataFrame(results)
