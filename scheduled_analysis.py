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
OUTPUT_FILE = f"Diagnostic_Signals_{datetime.now().strftime('%Y%m%d')}.csv"

# Email Config
EMAIL_SENDER = os.environ.get("SENDER_EMAIL")
EMAIL_PASSWORD = os.environ.get("SENDER_PASSWORD")
EMAIL_RECEIVER = os.environ.get("RECEIVER_EMAIL")

# --- MASTER FALLBACK LIST ---
FULL_TICKER_LIST = [
    "GBPUSD=X", "EURUSD=X", "JPY=X", "GBPCAD=X", "AUDUSD=X", "NZDUSD=X",
    "EURGBP=X", "GBPJPY=X", "EURJPY=X", "USDCHF=X", "USDCAD=X", "AUDJPY=X",
    "GBPAUD=X", "GBPNZD=X", "EURAUD=X", "EURCAD=X", "EURNZD=X", "AUDNZD=X",
    "XAUUSD=X", "SPY", "BTC-USD", "ETH-USD", "TSLA", "AMZN", "GOOG", "META",
    "AUDCHF=X", "NZDCHF=X", "GBPCHF=X", "EURCHF=X"
]

def load_tickers_from_source(source_dir):
    tickers = set()
    if not os.path.exists(source_dir): return FULL_TICKER_LIST
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
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

def send_email(subject, body, attachment_path=None, is_html=False):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("Email credentials missing.")
        return
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = EMAIL_SENDER, EMAIL_RECEIVER, subject
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
        print("Email sent.")
    except Exception as e:
        print(f"Email failed: {e}")

def main():
    print(f"--- Diagnostic Analysis: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---")
    tickers = load_tickers_from_source(TICKER_SOURCE_DIR)
    
    # Run scanner
    final_df = logic.run_scanner(tickers)

    if not final_df.empty:
        # Sorting
        def sort_key(s):
            if "TREND" in str(s): return 1
            if "CONTRARIAN" in str(s): return 2
            return 3
        
        final_df['SortOrder'] = final_df['Signal'].apply(sort_key)
        final_df = final_df.sort_values(by=['SortOrder', 'Ticker']).drop(columns=['SortOrder'])

        # Save CSV
        if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        final_df.to_csv(out_path, index=False)
        
        css = """
        <style>
            body{font-family:sans-serif;font-size:12px;color:#333;}
            table{border-collapse:collapse;width:100%;}
            th{background:#34495e;color:white;padding:8px;text-align:left;}
            td{border-bottom:1px solid #eee;padding:8px;vertical-align:top;}
            .trend{color:#27ae60;font-weight:bold;}
            .contra{color:#2980b9;font-weight:bold;}
            .trace{color:#888;font-family:monospace;font-size:11px;}
        </style>
        """
        
        # Prepare HTML Table
        email_df = final_df.copy()
        
        # Style the Signal Column
        def style_sig(v):
            if "TREND" in v: return f'<span class="trend">{v}</span>'
            if "CONTRARIAN" in v: return f'<span class="contra">{v}</span>'
            return v
            
        email_df['Signal'] = email_df['Signal'].apply(style_sig)
        # Style the Trace Column
        email_df['Trace'] = email_df['Trace'].apply(lambda x: f'<span class="trace">{x}</span>')
        
        table_html = email_df.to_html(index=False, border=0, escape=False)
        
        body = f"""
        <html>
        <head>{css}</head>
        <body>
            <h3>Market Diagnostic Report - {datetime.now().strftime('%Y-%m-%d')}</h3>
            <p>Full scan results including internal logic traces:</p>
            {table_html}
        </body>
        </html>
        """
            
        send_email(f"Diagnostic Report - {datetime.now().strftime('%Y-%m-%d')}", body, out_path, is_html=True)

if __name__ == "__main__":
    main()

