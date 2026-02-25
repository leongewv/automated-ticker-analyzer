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
OUTPUT_FILE = f"Trade_Report_{datetime.now().strftime('%Y%m%d')}.csv"

# Email Config (Ensure these are set in your Environment Variables)
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
        print("Email configuration missing. Check environment variables.")
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
        print("Report emailed successfully.")
    except Exception as e:
        print(f"SMTP Error: {e}")

def main():
    print(f"--- Hierarchical Market Scan: {datetime.now().strftime('%Y-%m-%d %H:%M')} ---")
    tickers = load_tickers_from_source(TICKER_SOURCE_DIR)
    
    # Run the hierarchical scanner logic
    final_df = logic.run_scanner(tickers)

    if not final_df.empty:
        # Sorting Priority: Trend trades first, then Contrarian, then No Signal
        def sort_key(s):
            if "TREND" in str(s): return 1
            if "CONTRARIAN" in str(s): return 2
            return 3
        
        final_df['SortOrder'] = final_df['Signal'].apply(sort_key)
        final_df = final_df.sort_values(by=['SortOrder', 'Ticker']).drop(columns=['SortOrder'])

        # Organize columns for the CSV output
        desired_cols = ['Ticker', 'Signal', 'TF', 'Price', 'Stop Loss', 'Bars Ago', 'Status', 'Trace']
        final_df = final_df[[c for c in desired_cols if c in final_df.columns]]

        # Save the master CSV
        if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        final_df.to_csv(out_path, index=False)
        
        # Email Formatting
        css = """
        <style>
            body{font-family:sans-serif;font-size:12px;color:#222;}
            table{border-collapse:collapse;width:100%;margin-top:15px;}
            th{background:#2c3e50;color:#ecf0f1;padding:10px;text-align:left;border:1px solid #34495e;}
            td{border:1px solid #bdc3c7;padding:8px;vertical-align:top;}
            .buy{color:#27ae60;font-weight:bold;}
            .sell{color:#c0392b;font-weight:bold;}
            .sl{color:#e67e22;font-weight:bold;}
            .trace{color:#95a5a6;font-family:monospace;font-size:10px;}
            .header-info{margin-bottom:20px;padding:10px;background:#f9f9f9;border-left:5px solid #3498db;}
        </style>
        """
        
        # Filter active signals for the email table body
        active_signals = final_df[final_df['Signal'] != "No Signal"].copy()
        
        if not active_signals.empty:
            # Apply color coding to Signal column
            active_signals['Signal'] = active_signals['Signal'].apply(
                lambda v: f'<span class="{"buy" if "UP" in v or "BUY" in v else "sell"}">{v}</span>'
            )
            # Highlight Stop Loss
            active_signals['Stop Loss'] = active_signals['Stop Loss'].apply(lambda v: f'<span class="sl">{v}</span>')
            # Format Trace
            active_signals['Trace'] = active_signals['Trace'].apply(lambda v: f'<span class="trace">{v}</span>')
            
            table_html = active_signals.to_html(index=False, border=0, escape=False)
            
            body = f"""
            <html>
            <head>{css}</head>
            <body>
                <div class="header-info">
                    <h2>Hierarchical Signal Report: {datetime.now().strftime('%d %b %Y')}</h2>
                    <p>Analysis Tiers: 4H/Daily, Daily/Weekly, Weekly/Monthly.<br>
                    <i>Requirement: Signal TF Cross + Higher TF Bollinger Expansion.</i></p>
                </div>
                {table_html}
                <p><small>Calculated Stop Loss includes a 1% buffer from the mathematical cross price.</small></p>
            </body>
            </html>
            """
        else:
            body = "<html><body><h3>Scan Complete: No hierarchical signals identified today.</h3></body></html>"
            
        send_email(f"Market Scan Report - {datetime.now().strftime('%Y-%m-%d')}", body, out_path, is_html=True)
    else:
        print("No tickers were successfully processed.")

if __name__ == "__main__":
    main()

