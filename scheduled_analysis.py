import pandas as pd
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import stock_analyzer_logic as logic

# --- CONFIGURATION ---
DATA_DIR = "data/incoming"
TICKER_SOURCE_DIR = "data/ticker_sources"
OUTPUT_FILE = f"Trend_Signals_{datetime.now().strftime('%Y%m%d')}.csv"

# Email Config
EMAIL_SENDER = os.environ.get("SENDER_EMAIL")
EMAIL_PASSWORD = os.environ.get("SENDER_PASSWORD")
EMAIL_RECEIVER = os.environ.get("RECEIVER_EMAIL")

# --- MASTER FALLBACK LIST (Abbreviated) ---
FULL_TICKER_LIST = [
    "GBPUSD=X", "EURUSD=X", "JPY=X", "GBPCAD=X", "AUDUSD=X", "NZDUSD=X",
    "EURGBP=X", "GBPJPY=X", "EURJPY=X", "USDCHF=X", "USDCAD=X", "AUDJPY=X",
    "GBPAUD=X", "GBPNZD=X", "EURAUD=X", "EURCAD=X", "EURNZD=X", "AUDNZD=X",
    "AUDCHF=X", "CADJPY=X", "USDJPY=X", "AUDCAD=X",
    "XAUUSD=X", "XAGUSD=X", "SPY", "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
    "DOGE-USD", "SHIB-USD", "LTC-USD", "BCH-USD", "BSV-USD", "DASH-USD",
    "ZEC-USD", "XMR-USD", "XLM-USD", "XEM-USD", "XEC-USD", "XNO-USD",
    "TSLA", "AMZN", "GOOG", "GOOGL", "META", "COIN", "ASML", "ISRG", "CELH",
    "CPRT", "FTNT", "GEHC", "ANML", "ARRR", "CC", "DCR", "DGB", "DINGO", 
    "ELON", "EURI", "ETN", "FIRO", "FORTH", "GAS", "GRS", "HUAHUA", 
    "KAS", "MAY", "MDT", "MTL", "NAV", "OMG", "PIVX", "QUAI", "RSR", "RVN", 
    "SXP", "THE", "USDUC", "VEX", "VOLT", "WBTC", "ZANO", "XEP"
]

# --- Helper Functions ---
def load_tickers_from_source(source_dir):
    tickers = set()
    if not os.path.exists(source_dir):
        print(f"Warning: Source dir '{source_dir}' missing. Using Master List.")
        return FULL_TICKER_LIST

    print(f"Loading tickers from {source_dir}...")
    found = False
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        if os.path.isfile(filepath):
            found = True
            try:
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(filepath)
                    cols = [c for c in df.columns if c.lower() in ['ticker', 'symbol', 'code']]
                    target = cols[0] if cols else df.columns[0]
                    tickers.update(df[target].dropna().astype(str).str.strip().tolist())
                else:
                    with open(filepath, 'r') as f:
                        lines = [l.strip() for l in f if l.strip() and not l.startswith('#')]
                        tickers.update(lines)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    
    return sorted(list(tickers)) if found else FULL_TICKER_LIST

def auto_find_calendar(data_dir):
    if not os.path.exists(data_dir): return None
    req = {'START', 'CURRENCY', 'IMPACT'}
    for f in os.listdir(data_dir):
        if f.endswith(".csv") and "Trade_Signals" not in f:
            try:
                df = pd.read_csv(os.path.join(data_dir, f), nrows=0)
                if req.issubset({c.strip().upper() for c in df.columns}):
                    return os.path.join(data_dir, f)
            except: continue
    return None

def load_economic_data(filepath):
    if not filepath: return None
    try:
        df = pd.read_csv(filepath)
        df.columns = [c.strip().title() for c in df.columns]
        df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
        if 'Impact' in df.columns: df['Impact'] = df['Impact'].astype(str).str.upper()
        if 'Currency' in df.columns: df['Currency'] = df['Currency'].astype(str).str.upper()
        return df
    except: return None

def send_email(subject, body, attachment_path=None, is_html=False):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("Credentials missing. Email skipped.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html' if is_html else 'plain'))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Email failed: {e}")

# --- Main Execution ---

def main():
    print(f"--- Trend Climber Analysis: {datetime.now().strftime('%Y-%m-%d')} ---")
    
    tickers = load_tickers_from_source(TICKER_SOURCE_DIR)
    eco_file = auto_find_calendar(DATA_DIR)
    eco_df = load_economic_data(eco_file)
    
    final_df = logic.run_scanner(tickers, eco_df=eco_df)

    if not final_df.empty:
        # --- SORTING LOGIC: CONFIRMED > EXISTING > No Signal ---
        def sort_key(signal):
            if "CONFIRMED" in signal: return 1
            if "EXISTING" in signal: return 2
            return 3 # No Signal or Error
        
        # Apply sort
        final_df['SortOrder'] = final_df['Signal'].apply(sort_key)
        final_df = final_df.sort_values(by=['SortOrder', 'Ticker'])
        final_df = final_df.drop(columns=['SortOrder']) # Cleanup

        cols = ['Ticker', 'Signal', 'Timeframe', 'Current Price', 'Stop Loss', 'Take Profit', 'Exit Warning', 'Cross Time', 'Remarks']
        available_cols = [c for c in cols if c in final_df.columns]
        final_df = final_df[available_cols]
        
        if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        final_df.to_csv(out_path, index=False)
        print(f"Saved: {out_path}")
        
        # Filter Active Signals (Confirmed & Existing)
        active_signals = final_df[final_df['Signal'] != "No Signal"]
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        css = """
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; font-size: 14px; color: #333; }
            h2 { color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 10px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 13px; }
            th { background-color: #2c3e50; color: white; padding: 12px; text-align: left; }
            td { border-bottom: 1px solid #ddd; padding: 10px; vertical-align: middle; }
            tr:nth-child(even) { background-color: #f8f9fa; }
            tr:hover { background-color: #e9ecef; }
            .buy { color: #27ae60; font-weight: bold; }
            .sell { color: #c0392b; font-weight: bold; }
            .risk { color: #e74c3c; font-weight: bold; }
            .warning { color: #d35400; font-weight: bold; } 
        </style>
        """
        
        if not active_signals.empty:
            email_df = active_signals.copy()
            
            def format_signal(val):
                if "UPTREND" in str(val) or "BUY" in str(val): return f'<span class="buy">{val}</span>'
                if "DOWNTREND" in str(val) or "SELL" in str(val): return f'<span class="sell">{val}</span>'
                return val

            def format_remarks(val):
                if "HIGH" in str(val) or "WARNING" in str(val): return f'<span class="risk">{val}</span>'
                return val
                
            def format_exit(val):
                if "Opposing" in str(val): return f'<span class="warning">{val}</span>'
                return val

            if 'Signal' in email_df.columns: email_df['Signal'] = email_df['Signal'].apply(format_signal)
            if 'Remarks' in email_df.columns: email_df['Remarks'] = email_df['Remarks'].apply(format_remarks)
            if 'Exit Warning' in email_df.columns: email_df['Exit Warning'] = email_df['Exit Warning'].apply(format_exit)
            
            table_html = email_df.to_html(index=False, border=0, escape=False)
            
            body = f"""
            <html>
            <head>{css}</head>
            <body>
                <h2>Trend Climber Signals - {current_date}</h2>
                <p>The following tickers have confirmed <b>Golden/Death Crosses</b> on stable trends.</p>
                {table_html}
                <div class="footer">
                    <p style="font-size: 11px; color: #666; margin-top: 20px;">
                        *Stop Loss: 1% from Cross Price (Exact intersection).<br>
                        *Take Profit: Previous profitable opposing cross (Smart Scan).<br>
                        *Exit Warning: Sustained reversal (>10 bars) on lower TFs down to 30m.
                    </p>
                </div>
            </body>
            </html>
            """
        else:
            body = f"""
            <html>
            <head>{css}</head>
            <body>
                <h2>Trend Climber Signals - {current_date}</h2>
                <p><b>No active signals found today.</b></p>
                <p>Scanned {len(final_df)} tickers.</p>
            </body>
            </html>
            """
            
        send_email(f"Trend Climber Alerts - {current_date}", body, out_path, is_html=True)

if __name__ == "__main__":
    main()

