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
OUTPUT_FILE = f"Trade_Signals_{datetime.now().strftime('%Y%m%d')}.csv"

# Email Config
EMAIL_SENDER = os.environ.get("SENDER_EMAIL")
EMAIL_PASSWORD = os.environ.get("SENDER_PASSWORD")
EMAIL_RECEIVER = os.environ.get("RECEIVER_EMAIL")

# --- MASTER FALLBACK LIST (Restored from your logs) ---
# Used if data/ticker_sources is missing
FULL_TICKER_LIST = [
    # Forex
    "GBPUSD=X", "EURUSD=X", "JPY=X", "GBPCAD=X", "AUDUSD=X", "NZDUSD=X",
    "EURGBP=X", "GBPJPY=X", "EURJPY=X", "USDCHF=X", "USDCAD=X", "AUDJPY=X",
    "GBPAUD=X", "GBPNZD=X", "EURAUD=X", "EURCAD=X", "EURNZD=X", "AUDNZD=X",
    "AUDCHF=X", "CADJPY=X", "USDJPY=X", "AUDCAD=X",
    
    # Commodities / Indices
    "XAUUSD=X", "XAGUSD=X", "SPY", "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
    
    # Crypto (Yahoo often needs -USD for crypto)
    "DOGE-USD", "SHIB-USD", "LTC-USD", "BCH-USD", "BSV-USD", "DASH-USD",
    "ZEC-USD", "XMR-USD", "XLM-USD", "XEM-USD", "XEC-USD", "XNO-USD",
    
    # Stocks & Other Assets (from your logs)
    "TSLA", "AMZN", "GOOG", "GOOGL", "META", "COIN", "ASML", "ISRG", "CELH",
    "CPRT", "FTNT", "GEHC", "ANML", "ARRR", "CC", "DCR", "DGB", "DINGO", 
    "ELON", "EURI", "ETN", "FIRO", "FORTH", "GAS", "GRIN", "GRS", "HUAHUA", 
    "KAS", "MAY", "MDT", "MTL", "NAV", "OMG", "PIVX", "QUAI", "RSR", "RVN", 
    "SXP", "THE", "USDUC", "VEX", "VOLT", "WBTC", "ZANO", "XEP"
]

# --- Helper Functions ---

def load_tickers_from_source(source_dir):
    """
    Reads all files in the source directory to build the master ticker list.
    If directory is missing, returns the FULL_TICKER_LIST.
    """
    tickers = set()
    
    if not os.path.exists(source_dir):
        print(f"Warning: Ticker source directory '{source_dir}' does not exist.")
        print(f"-> Falling back to HARDCODED MASTER LIST ({len(FULL_TICKER_LIST)} tickers).")
        return FULL_TICKER_LIST

    print(f"Loading tickers from {source_dir}...")
    
    found_files = False
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        if os.path.isfile(filepath):
            found_files = True
            try:
                # 1. Try CSV
                if filename.lower().endswith('.csv'):
                    df = pd.read_csv(filepath)
                    # Smart column detection
                    possible_cols = [c for c in df.columns if c.lower() in ['ticker', 'symbol', 'code']]
                    target_col = possible_cols[0] if possible_cols else df.columns[0]
                    
                    file_tickers = df[target_col].dropna().astype(str).str.strip().tolist()
                    tickers.update(file_tickers)
                    print(f"  -> Added {len(file_tickers)} from {filename}")
                
                # 2. Text / Other files (Assume one ticker per line)
                else:
                    with open(filepath, 'r') as f:
                        lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                        tickers.update(lines)
                        print(f"  -> Added {len(lines)} from {filename}")
                        
            except Exception as e:
                print(f"  -> Error reading {filename}: {e}")
    
    if not found_files:
        print("  -> Directory exists but is empty. Using MASTER FALLBACK LIST.")
        return FULL_TICKER_LIST

    sorted_tickers = sorted(list(tickers))
    print(f"Total unique tickers loaded: {len(sorted_tickers)}")
    return sorted_tickers

def auto_find_calendar(data_dir):
    if not os.path.exists(data_dir):
        print(f"Directory not found: {data_dir}")
        return None
    
    required_cols = {'START', 'CURRENCY', 'IMPACT'} 
    print(f"Scanning {data_dir} for economic calendar files...")
    
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv") and "Trade_Signals" not in filename:
            filepath = os.path.join(data_dir, filename)
            try:
                df = pd.read_csv(filepath, nrows=0)
                file_cols = {c.strip().upper() for c in df.columns}
                if required_cols.issubset(file_cols):
                    print(f" -> Auto-Detected Economic Calendar: {filename}")
                    return filepath
            except Exception: continue
    
    print(" -> No Economic Calendar file found. Proceeding without economic analysis.")
    return None

def load_economic_data(filepath):
    if not filepath or not os.path.exists(filepath): return None
    try:
        df = pd.read_csv(filepath)
        df.columns = [c.strip().title() for c in df.columns] 
        df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
        if 'Impact' in df.columns: df['Impact'] = df['Impact'].astype(str).str.upper()
        if 'Currency' in df.columns: df['Currency'] = df['Currency'].astype(str).str.upper()
        return df
    except Exception as e:
        print(f"Error reading calendar: {e}")
        return None

def send_email(subject, body, attachment_path=None, is_html=False):
    """
    Sends email with optional attachment. 
    is_html: If True, sends as HTML MIME type. If False, sends as Plain Text.
    """
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("Skipping email: Credentials not set.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject
    
    if is_html:
        msg.attach(MIMEText(body, 'html'))
    else:
        msg.attach(MIMEText(body, 'plain'))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(attachment_path)}")
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"Email sent to {EMAIL_RECEIVER}")
    except Exception as e:
        print(f"Failed to send email: {e}")

# --- Main Execution ---

def main():
    print(f"--- Starting Analysis for {datetime.now().strftime('%Y-%m-%d')} ---")
    
    # 1. Load Tickers (Dynamic or Fallback)
    tickers = load_tickers_from_source(TICKER_SOURCE_DIR)
    
    # 2. Load Economic Calendar
    eco_file = auto_find_calendar(DATA_DIR)
    eco_df = load_economic_data(eco_file)
    
    # 3. Run Scanner
    # Pass the loaded eco_df to the logic function
    final_df = logic.run_scanner(tickers, eco_df=eco_df)

    if not final_df.empty:
        # Reorder to ensure Remarks is last
        if 'Remarks' in final_df.columns:
            cols = [c for c in final_df.columns if c != 'Remarks'] + ['Remarks']
            final_df = final_df[cols]
        
        # Save CSV
        if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        final_df.to_csv(out_path, index=False)
        print(f"Results saved to {out_path}")
        
        # Filter for "Active Signals" (Exclude 'No Signal')
        active_signals = final_df[final_df['Signal'] != "No Signal"]
        
        current_date = datetime.now().strftime('%Y-%m-%d')

        # --- HTML EMAIL GENERATION ---
        
        # CSS Styles for proper table formatting
        css_style = """
        <style>
            body { font-family: Arial, sans-serif; font-size: 14px; color: #333; }
            h2 { color: #2c3e50; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 12px; }
            th { background-color: #2c3e50; color: white; padding: 10px; text-align: left; }
            td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            tr:hover { background-color: #e6f7ff; }
            .footer { margin-top: 20px; font-size: 12px; color: #777; }
        </style>
        """

        if not active_signals.empty:
            # Select key columns for email readability
            display_cols = ['Ticker', 'Signal', 'Current Price', 'Daily Setup', 'Confirmations']
            if 'Remarks' in active_signals.columns: display_cols.append('Remarks')
            
            # Convert DataFrame to HTML Table
            table_html = active_signals[display_cols].to_html(index=False, border=0)
            
            email_body = f"""
            <html>
            <head>{css_style}</head>
            <body>
                <h2>Daily Trade Signals - {current_date}</h2>
                <p><b>--- ACTIVE SIGNALS ---</b></p>
                {table_html}
                <div class="footer">
                    <p>(See attached CSV for full details including "No Signal" tickers)</p>
                </div>
            </body>
            </html>
            """
        else:
            email_body = f"""
            <html>
            <head>{css_style}</head>
            <body>
                <h2>Daily Trade Signals - {current_date}</h2>
                <p>No active trade setups found today.</p>
                <p>Monitoring {len(final_df)} tickers.</p>
            </body>
            </html>
            """

        send_email(
            subject=f"Daily Trade Signals - {current_date}",
            body=email_body,
            attachment_path=out_path,
            is_html=True
        )
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()
