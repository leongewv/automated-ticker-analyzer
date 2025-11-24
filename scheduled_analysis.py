import os
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pandas as pd

# --- Import the Analysis Logic ---
from stock_analyzer_logic import run_scanner

# --- Configuration ---
HISTORY_FILE = 'analysis_history.csv' 
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

def send_email_notification(subject, html_body):
    """Sends an email with the given subject and HTML body."""
    if not all([SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL]):
        print("Error: Email credentials not set in environment variables.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL

    msg.attach(MIMEText(html_body, "html"))

    try:
        print("Connecting to email server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL.split(','), msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def generate_recommendations(current_df, previous_df):
    """
    Compares current signals with history to generate actionable text.
    """
    if current_df.empty:
        return current_df

    # 1. Prepare Previous Data
    if previous_df.empty:
        # If no history, everything is new
        current_df['Recommendation'] = current_df['Signal'].apply(
            lambda x: f"🔥 New Signal: {x}" if "No Signal" not in x else "Monitoring"
        )
        return current_df
    
    # Ensure Ticker is the key
    if 'Instrument' in previous_df.columns and 'Ticker' not in previous_df.columns:
        previous_df.rename(columns={'Instrument': 'Ticker'}, inplace=True)

    # 2. Merge
    merged_df = pd.merge(
        current_df,
        previous_df[['Ticker', 'Signal']],
        on='Ticker',
        how='left',
        suffixes=('', '_prev')
    ).fillna({'Signal_prev': 'None'})

    # 3. Logic: Compare Signals
    def get_recommendation(row):
        current = str(row['Signal'])
        previous = str(row['Signal_prev'])
        
        if current == previous:
            return "No change." if "No Signal" not in current else "Monitoring"
        
        if "No Signal" in current:
            if "Buy" in previous or "Sell" in previous:
                return f"📉 Signal Lost (Was {previous})"
            return "Monitoring"
        
        if previous == 'None' or previous == 'nan' or "No Signal" in previous:
             return f"🔥 New Signal: {current}"

        if 'Standard' in previous and 'SUPER' in current:
            return f"🚀 UPGRADE: Standard -> SUPER ({current})"
            
        if 'SUPER' in previous and 'Standard' in current:
            return f"⚠️ Downgrade: SUPER -> Standard"
            
        if ('Buy' in previous and 'Sell' in current) or ('Sell' in previous and 'Buy' in current):
            return f"🔄 FLIP: {previous} -> {current}"

        return f"Update: {current}"

    merged_df['Recommendation'] = merged_df.apply(get_recommendation, axis=1)
    
    # 4. Select Columns for Report
    cols_to_use = [
        "Ticker", "Signal", "Recommendation", "Daily Setup", 
        "Failure Reason", "Confirmations", "Switch Time", 
        "Current 20d SMA Level", "Current Price" # UPDATED: Replaced Est. Price with new columns
    ]
    
    # Filter to exist only
    final_cols = [col for col in cols_to_use if col in merged_df.columns]
    
    return merged_df[final_cols]

def main():
    print(f"Starting scheduled analysis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 1. Load History ---
    try:
        previous_results_df = pd.read_csv(HISTORY_FILE)
        print(f"Loaded history from '{HISTORY_FILE}'")
    except FileNotFoundError:
        print("No history found. First run.")
        previous_results_df = pd.DataFrame()

    # --- 2. Load Tickers ---
    source_folder = 'ticker_sources'
    if not os.path.exists(source_folder):
        os.makedirs(source_folder)
        print(f"Created '{source_folder}'. Please add CSV files with tickers there.")
        return

    csv_files = glob.glob(os.path.join(source_folder, '*.csv'))
    all_tickers = []
    
    if csv_files:
        print(f"Reading tickers from {len(csv_files)} files...")
        for file in csv_files:
            try:
                df = pd.read_csv(file)
                tickers_from_file = df.iloc[:, 0].dropna().tolist()
                all_tickers.extend(tickers_from_file)
            except Exception as e:
                print(f"Skipping {file}: {e}")
    else:
        print("No CSV files found in 'ticker_sources'. Using default test list.")
        all_tickers = ["NVDA", "BTC-USD", "EURUSD=X", "AAPL"]

    tickers_to_analyze = sorted(list(set(all_tickers)))
    
    # --- 3. Run Analysis ---
    full_results_df = run_scanner(tickers_to_analyze)
    
    # --- 4. Save History ---
    if not full_results_df.empty:
        try:
            full_results_df.to_csv(HISTORY_FILE, index=False)
            print(f"✅ History successfully updated and saved to '{HISTORY_FILE}'.")
        except PermissionError:
            print(f"❌ ERROR: Could not save to '{HISTORY_FILE}'. Is the file open in Excel? Please close it.")
            return
    else:
        full_results_df.to_csv(HISTORY_FILE, index=False)
        print("No signals found. History cleared.")
        return

    # --- 5. Generate Report ---
    actionable_df = generate_recommendations(full_results_df.copy(), previous_results_df)

    # Sort: Signals at the top, No Signal at the bottom
    # We sort by 'Failure Reason' where 'None' (meaning Success) comes first
    actionable_df = actionable_df.sort_values(
        by='Failure Reason', 
        key=lambda x: x != 'None'
    )

    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # --- 6. Email Logic ---
    if not actionable_df.empty:
        subject = f"Trading Signals & Report - {today_str}"
        
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
                th, td {{ border: 1px solid #ddd; padding: 6px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                tr:hover {{ background-color: #ddd; }}
            </style>
        </head>
        <body>
            <h2>Strategy Scan Results ({today_str})</h2>
            <p><strong>Note:</strong> Rows with 'No Signal' did not meet strict strategy criteria.</p>
            {actionable_df.to_html(index=False)}
            <p><small>Automated Report. Not financial advice.</small></p>
        </body>
        </html>
        """
        
        send_email_notification(subject, html_body)
        
        # Also print to console for verification
        print("\n--- Report Generated ---")
        # Print first few columns to console for quick check
        print(actionable_df[['Ticker', 'Signal', 'Failure Reason']].to_string(index=False))
    else:
        print("No results generated at all (empty input?).")

if __name__ == "__main__":
    main()
