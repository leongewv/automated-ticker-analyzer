import pandas as pd
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import stock_analyzer_logic as logic

# --- Configuration ---
DATA_DIR, TICKER_SOURCE_DIR = "data/incoming", "data/ticker_sources"
OUTPUT_FILE = f"Trend_Signals_{datetime.now().strftime('%Y%m%d')}.csv"
EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER = os.environ.get("SENDER_EMAIL"), os.environ.get("SENDER_PASSWORD"), os.environ.get("RECEIVER_EMAIL")

FULL_TICKER_LIST = [
    "GBPUSD=X", "EURUSD=X", "JPY=X", "GBPCAD=X", "AUDUSD=X", "NZDUSD=X",
    "EURGBP=X", "GBPJPY=X", "EURJPY=X", "USDCHF=X", "USDCAD=X", "AUDJPY=X",
    "GBPAUD=X", "GBPNZD=X", "EURAUD=X", "EURCAD=X", "EURNZD=X", "AUDNZD=X",
    "AUDCHF=X", "CADJPY=X", "USDJPY=X", "AUDCAD=X", "XAUUSD=X", "XAGUSD=X", 
    "SPY", "BTC-USD", "ETH-USD", "TSLA", "AMZN", "GOOG", "META"
]

def send_email(subject, body, attachment_path=None, is_html=False):
    if not all([EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER]): return
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = EMAIL_SENDER, EMAIL_RECEIVER, subject
    msg.attach(MIMEText(body, 'html' if is_html else 'plain'))
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part); part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}"); msg.attach(part)
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as s:
            s.starttls(); s.login(EMAIL_SENDER, EMAIL_PASSWORD); s.send_message(msg)
        print("Email sent.")
    except Exception as e: print(f"Email failed: {e}")

def main():
    print(f"--- Scan Start: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---")
    
    tickers = FULL_TICKER_LIST 
    eco_df = None # Load from file if available in your DATA_DIR

    # Run Scanner
    final_df = logic.run_scanner([t.strip().upper() for t in tickers], eco_df=eco_df)

    if not final_df.empty:
        final_df['SortOrder'] = final_df['Signal'].apply(lambda x: 1 if "CONFIRMED" in str(x) else (2 if "EXISTING" in str(x) else 3))
        final_df = final_df.sort_values(['SortOrder', 'Ticker']).drop(columns=['SortOrder', 'PriceSrc'], errors='ignore')
        
        if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        
        cols = ['Ticker', 'Signal', 'Timeframe', 'Current Price', 'Stop Loss', 'Take Profit', 'Exit Warning', 'Cross Time', 'Remarks']
        final_df = final_df[[c for c in cols if c in final_df.columns]]
        final_df.to_csv(out_path, index=False)
        
        # Email formatting
        active = final_df[final_df['Signal'] != "No Signal"]
        css = "<style>body{font-family:sans-serif;font-size:13px}table{border-collapse:collapse;width:100%}th{background:#2c3e50;color:white;padding:10px;text-align:left}td{border-bottom:1px solid #ddd;padding:8px}.buy{color:#27ae60;font-weight:bold}.sell{color:#c0392b;font-weight:bold}</style>"
        
        if not active.empty:
            email_df = active.copy()
            email_df['Signal'] = email_df['Signal'].apply(lambda v: f'<span class="{"buy" if "UP" in v else "sell"}">{v}</span>')
            body = f"<html><head>{css}</head><body><h2>Signals - {datetime.now().strftime('%Y-%m-%d')}</h2>{email_df.to_html(index=False, border=0, escape=False)}</body></html>"
        else:
            body = f"<html><head>{css}</head><body><h2>No signals found today.</h2><p>Scanned {len(final_df)} tickers.</p></body></html>"
        
        send_email(f"Trend Alerts - {datetime.now().strftime('%Y-%m-%d')}", body, out_path, is_html=True)

if __name__ == "__main__": main()
