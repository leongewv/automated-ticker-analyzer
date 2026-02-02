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

# --- MASTER FALLBACK LIST ---
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

def load_tickers_from_source(source_dir):
    tickers = set()
    if not os.path.exists(source_dir): return FULL_TICKER_LIST
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        if os.path.isfile(filepath):
            try:
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(filepath)
                    cols = [c for c in df.columns if c.lower() in ['ticker', 'symbol', 'code']]
                    target = cols[0] if cols else df.columns[0]
                    tickers.update(df[target].dropna().astype(str).str.strip().tolist())
                else:
                    with open(filepath, 'r') as f:
                        tickers.update([l.strip() for l in f if l.strip() and not l.startswith('#')])
            except: continue
    return sorted(list(tickers)) if tickers else FULL_TICKER_LIST

def auto_find_calendar(data_dir):
    if not os.path.exists(data_dir): return None
    for f in os.listdir(data_dir):
        if f.endswith(".csv") and "Trade_Signals" not in f:
            try:
                df = pd.read_csv(os.path.join(data_dir, f), nrows=0)
                if {'START', 'CURRENCY', 'IMPACT'}.issubset({c.strip().upper() for c in df.columns}): return os.path.join(data_dir, f)
            except: continue
    return None

def load_economic_data(filepath):
    if not filepath: return None
    try:
        df = pd.read_csv(filepath)
        df.columns = [c.strip().title() for c in df.columns]
        df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
        for c in ['Impact', 'Currency']: df[c] = df[c].astype(str).str.upper()
        return df
    except: return None

def send_email(subject, body, attachment_path=None, is_html=False):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER: return
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = EMAIL_SENDER, EMAIL_RECEIVER, subject
    msg.attach(MIMEText(body, 'html' if is_html else 'plain'))
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream"); part.set_payload(f.read())
        encoders.encode_base64(part); part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}"); msg.attach(part)
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587); server.starttls(); server.login(EMAIL_SENDER, EMAIL_PASSWORD); server.send_message(msg); server.quit()
    except: pass

def main():
    print(f"--- Discovery Mode: {datetime.now().strftime('%Y-%m-%d')} ---")
    tickers = load_tickers_from_source(TICKER_SOURCE_DIR)
    eco_df = load_economic_data(auto_find_calendar(DATA_DIR))
    final_df = logic.run_scanner(tickers, eco_df=eco_df)

    if not final_df.empty:
        # Sorting
        def sort_key(s): return 1 if "CONFIRMED" in str(s) else (2 if "EXISTING" in str(s) else 3)
        final_df['SortOrder'] = final_df['Signal'].apply(sort_key)
        final_df = final_df.sort_values(by=['SortOrder', 'Ticker']).drop(columns=['SortOrder'])

        # SEPARATED COLUMNS FOR READABILITY
        cols = ['Ticker', 'Signal', 'Timeframe', 'Stop Loss', 'Take Profit', 'Exit Warning', 'Cross Time', 'Eco Calendar', 'Bar Trace']
        final_df = final_df[[c for c in cols if c in final_df.columns]]
        
        if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        final_df.to_csv(out_path, index=False)
        
        active = final_df[final_df['Signal'] != "No Signal"]
        css = "<style>body{font-family:sans-serif;font-size:13px}table{border-collapse:collapse;width:100%}th{background:#2c3e50;color:white;padding:12px}td{border-bottom:1px solid #ddd;padding:10px;vertical-align:top}.buy{color:#27ae60;font-weight:bold}.sell{color:#c0392b;font-weight:bold}.risk{color:#e74c3c;font-weight:bold}</style>"
        
        if not active.empty:
            email_df = active.copy()
            email_df['Signal'] = email_df['Signal'].apply(lambda v: f'<span class="{"buy" if "UP" in v else "sell"}">{v}</span>')
            email_df['Eco Calendar'] = email_df['Eco Calendar'].apply(lambda v: f'<span class="risk">{v}</span>' if v and v != "Safe" and v != "-" else v)
            table_html = email_df.to_html(index=False, border=0, escape=False)
            body = f"<html><head>{css}</head><body><h2>Discovery Signals - {datetime.now().strftime('%Y-%m-%d')}</h2>{table_html}</body></html>"
        else: body = f"<html><body><h2>No signals found today.</h2></body></html>"
        send_email(f"Trend Discovery - {datetime.now().strftime('%Y-%m-%d')}", body, out_path, is_html=True)

if __name__ == "__main__": main()

