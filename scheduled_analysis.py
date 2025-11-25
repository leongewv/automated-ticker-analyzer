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
    Compares current signals with history to generate actionable text with REASONS.
    """
    if current_df.empty:
        return current_df

    # 1. Prepare Previous Data
    if previous_df.empty:
        # If no history, everything is new
        current_df['Recommendation'] = current_df.apply(
            lambda row: f"🔥 New Signal: {row['Signal']} ({row['Daily Setup']})" 
            if "No Signal" not in row['Signal'] else "Monitoring", axis=1
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

    # 3. Logic: Compare Signals with Explanations
    def get_recommendation(row):
        current_sig = str(row['Signal'])
        prev_sig = str(row['Signal_prev'])
        
        # Grab the technical reasons calculated by the logic script
        fail_reason = str(row['Failure Reason'])
        setup_type = str(row['Daily Setup'])
        
        # --- Case A: No Change ---
        if current_sig == prev_sig:
            return "No change." if "No Signal" not in current_sig else "Monitoring"
        
        # --- Case B: Downgrade (Signal Lost) ---
        if "No Signal" in current_sig:
            if "Buy" in prev_sig or "Sell" in prev_sig:
                # Include the specific technical failure reason
                return f"📉 Signal Lost (Was {prev_sig}) -> Why: {fail_reason}"
            return "Monitoring"
        
        # --- Case C: New Signal (Upgrade) ---
        if prev_sig == 'None' or prev_sig == 'nan' or "No Signal" in prev_sig:
             return f"🔥 New Signal: {current_sig} (Setup: {setup_type})"

        # --- Case D: Strength Change (Standard <-> SUPER) ---
        if 'Standard' in prev_sig and 'SUPER' in current_sig:
            return f"🚀 UPGRADE: Standard -> SUPER ({current_sig})"
            
        if 'SUPER' in prev_sig and 'Standard' in current_sig:
            return f"⚠️ Strength Drop: SUPER -> Standard (Check confirmations)"
            
        # --- Case E: Direction Flip ---
        if ('Buy' in prev_sig and 'Sell' in current_sig) or ('Sell' in prev_sig and 'Buy' in current_sig):
            return f"🔄 FLIP: {prev_sig} -> {current_sig}"

        return f"Update: {current_sig}"

    merged_df['Recommendation'] = merged_df.apply(get_recommendation, axis=1)
    
    # 4. Select Columns for Report
    cols_to_use = [
        "Ticker", "Signal", "Recommendation", "Daily Setup", 
        "Failure Reason", "Confirmations", "Switch Time", "Est. Price"
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
        
        # UPDATED CSS FOR BETTER READABILITY OF LONG REASONS
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
                h2 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; font-size: 13px; table-layout: fixed; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; word-wrap: break-word; }}
                th {{ background-color: #2c3e50; color: white; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                tr:hover {{ background-color: #f1f1f1; }}
                
                /* Column Specific Widths to prevent squeezing */
                th:nth-child(1) {{ width: 8%; }}  /* Ticker */
                th:nth-child(2) {{ width: 10%; }} /* Signal */
                th:nth-child(3) {{ width: 25%; }} /* Recommendation (Has detailed reason) */
                th:nth-child(5) {{ width: 20%; }} /* Failure Reason (Raw) */
            </style>
        </head>
        <body>
            <h2>Strategy Scan Results ({today_str})</h2>
            <p><strong>Note:</strong> Rows with 'No Signal' did not meet strict strategy criteria.</p>
            {actionable_df.to_html(index=False, classes='table')}
            <p><small>Automated Report. Not financial advice.</small></p>
        </body>
        </html>
        """
        
        send_email_notification(subject, html_body)
        
        # Also print to console for verification
        print("\n--- Report Generated ---")
        print(actionable_df[['Ticker', 'Signal', 'Recommendation']].to_string(index=False))
    else:
        print("No results generated at all (empty input?).")

if __name__ == "__main__":
    main()
