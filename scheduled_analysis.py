import os
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pandas as pd

# --- Import the Analysis Logic ---
# Updated to match the restored filename 'stock_analyzer_logic.py'
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
    Adapts to 'Standard' and 'SUPER' signal terminology.
    """
    if current_df.empty:
        return current_df

    # 1. Prepare Previous Data
    if previous_df.empty:
        # If no history, everything is new
        current_df['Recommendation'] = current_df['Signal'].apply(
            lambda x: f"🔥 New Signal: {x}"
        )
        return current_df
    
    # Ensure Ticker is the key (Old script used 'Instrument', new uses 'Ticker')
    # If loading old history file with 'Instrument', rename it
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

    # 3. Logic: Compare 'Standard/SUPER' (New) vs 'Standard/SUPER' (Old)
    def get_recommendation(row):
        current = str(row['Signal'])
        previous = str(row['Signal_prev'])
        
        if current == previous:
            return "No change."
        
        # New Signal detection
        if previous == 'None' or previous == 'nan':
             return f"🔥 New Signal: {current}"

        # Upgrades
        if 'Standard' in previous and 'SUPER' in current:
            return f"🚀 UPGRADE: Standard -> SUPER ({current})"
            
        # Downgrades / Changes
        if 'SUPER' in previous and 'Standard' in current:
            return f"⚠️ Downgrade: SUPER -> Standard"
            
        # Direction flip (Buy to Sell or vice versa)
        if ('Buy' in previous and 'Sell' in current) or ('Sell' in previous and 'Buy' in current):
            return f"🔄 FLIP: {previous} -> {current}"

        return f"Update: {current}"

    merged_df['Recommendation'] = merged_df.apply(get_recommendation, axis=1)
    
    # 4. Select Columns for Report
    # Matches the output of the new trend_scanner.py
    cols_to_use = [
        "Ticker", "Signal", "Recommendation", "Daily Setup", 
        "Confirmations", "Est. Price"
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
        os.makedirs(source_folder) # Create if missing to prevent crash
        print(f"Created '{source_folder}'. Please add CSV files with tickers there.")
        return

    csv_files = glob.glob(os.path.join(source_folder, '*.csv'))
    all_tickers = []
    
    if csv_files:
        print(f"Reading tickers from {len(csv_files)} files...")
        for file in csv_files:
            try:
                df = pd.read_csv(file)
                # Assumes tickers are in the first column
                tickers_from_file = df.iloc[:, 0].dropna().tolist()
                all_tickers.extend(tickers_from_file)
            except Exception as e:
                print(f"Skipping {file}: {e}")
    else:
        # Fallback for testing if no CSVs provided
        print("No CSV files found in 'ticker_sources'. Using default test list.")
        all_tickers = ["NVDA", "BTC-USD", "EURUSD=X", "AAPL"]

    tickers_to_analyze = sorted(list(set(all_tickers)))
    
    # --- 3. Run Analysis (Updated Function Call) ---
    # Note: No status_callback arg needed anymore
    full_results_df = run_scanner(tickers_to_analyze)
    
    if full_results_df.empty:
        print("No signals found this run.")
        # We still save an empty history to record that we ran it
        full_results_df.to_csv(HISTORY_FILE, index=False)
        return

    # --- 4. Generate Report ---
    actionable_df = generate_recommendations(full_results_df.copy(), previous_results_df)

    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # --- 5. Email Logic ---
    if not actionable_df.empty:
        subject = f"Trading Signals - {today_str}"
        
        # Style the HTML table
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .super {{ color: green; font-weight: bold; }}
            </style>
        </head>
        <body>
            <h2>Strategy Scan Results ({today_str})</h2>
            {actionable_df.to_html(index=False)}
            <p><small>Automated Report. Not financial advice.</small></p>
        </body>
        </html>
        """
        
        send_email_notification(subject, html_body)
    else:
        print("No actionable signals to email.")

    # --- 6. Save History ---
    # Save the raw results (without recommendation text) for next comparison
    full_results_df.to_csv(HISTORY_FILE, index=False)
    print("History updated.")

if __name__ == "__main__":
    main()
