import os
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pandas as pd
import numpy as np # Often needed for pandas

# Import the main analysis function from our logic file
# Make sure your logic file is named 'stock_analyzer_logic.py'
from stock_analyzer_logic import run_multi_timeframe_analysis

# --- Configuration ---
HISTORY_FILE = 'analysis_history.csv'  # File to store the last run's results

# --- Email Configuration ---
# Load credentials from environment variables for security
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL") # Can be a comma-separated list
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


def send_email_notification(subject, html_body):
    """Sends an email with the given subject and HTML body."""
    if not all([SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL]):
        print("Error: Email credentials or recipient not set in environment variables.")
        print("Set SENDER_EMAIL, SENDER_PASSWORD, and RECEIVER_EMAIL to run.")
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
    Compares signals and preserves all data columns for the report.
    """
    if previous_df.empty:
        current_df['Recommendation'] = current_df['Signal'].apply(
            lambda x: f"🔥 New Signal: {x}" if 'Strong' in x or 'Moderate' in x else "No Signal"
        )
        return current_df

    # Merge, keeping all columns from the current_df
    merged_df = pd.merge(
        current_df,
        previous_df[['Instrument', 'Signal']],
        on='Instrument',
        how='left',
        suffixes=('', '_prev')
    ).fillna({'Signal_prev': 'Hold for now'})

    def get_recommendation(row):
        current, previous = row['Signal'], row['Signal_prev']
        
        if current == previous:
            return "No change."
        if 'Hold' in current and ('Strong' in previous or 'Moderate' in previous):
            return f"📉 Signal Lost: Was '{previous}'."
        if 'Moderate' in current:
            if 'Hold' in previous: return f"⚠️ New Signal: '{current}'."
            if 'Strong' in previous: return f"📉 Downgrade: From '{previous}' to '{current}'."
            if 'Super Strong' in previous: return f"📉 Downgrade: From '{previous}' to '{current}'."
        if 'Strong' in current and 'Super Strong' not in current:
            if 'Hold' in previous: return f"🔥 New Signal: '{current}'."
            if 'Moderate' in previous: return f"📈 Upgrade: From '{previous}' to '{current}'."
            if 'Super Strong' in previous: return f"📉 Downgrade: From '{previous}' to '{current}'."
        if 'Super Strong' in current and 'Super Strong' not in previous:
            return f"🚀🚀 Ultimate Upgrade: New '{current}' signal!"
        return "Monitor signal change."

    merged_df['Recommendation'] = merged_df.apply(get_recommendation, axis=1)
    
    # *** NEW: Added Fib A, B, C to the list of columns to keep ***
    cols_to_use = [
        "Instrument", "Trend", "Signal", "Recommendation", "Daily Setup", "Confirmation TFs",
        "Price", "BBM_20", "EMA_200", "Low", "High", "BB_Width", 
        "Squeeze_Thresh", "Is_Squeeze", "SMA_Dist_EMA(%)",
        "Price_Dist_EMA_Low(%)", "Price_Dist_EMA_High(%)",
        "Fib_A", "Fib_B", "Fib_C", "Fib_0.786", "Fib_1.618"
    ]
    
    # Filter to only the columns that actually exist in the dataframe
    final_cols = [col for col in cols_to_use if col in merged_df.columns]
    
    return merged_df[final_cols]


def main():
    """Main function to run the analysis and send the report."""
    print(f"Starting scheduled analysis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # --- 1. Load Previous Analysis State ---
    try:
        previous_results_df = pd.read_csv(HISTORY_FILE)
        print(f"Successfully loaded previous analysis from '{HISTORY_FILE}'")
    except FileNotFoundError:
        print("History file not found. This must be the first run.")
        previous_results_df = pd.DataFrame()

    # --- 2. Load Tickers ---
    source_folder = 'ticker_sources'
    csv_files = glob.glob(os.path.join(source_folder, '*.csv'))

    if not csv_files:
        print(f"Error: No CSV files found in the '{source_folder}' directory.")
        return

    all_tickers = []
    print(f"Reading tickers from {len(csv_files)} CSV file(s)...")
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            tickers_from_file = df.iloc[:, 0].dropna().tolist()
            all_tickers.extend(tickers_from_file)
            print(f"  - Loaded {len(tickers_from_file)} tickers from {os.path.basename(file)}")
        except Exception as e:
            print(f"  - Could not read file {os.path.basename(file)}. Error: {e}")

    tickers_to_analyze = sorted(list(set(all_tickers)))
    if not tickers_to_analyze:
        print("No tickers loaded. Exiting.")
        return
    
    print(f"\nFound a total of {len(tickers_to_analyze)} unique tickers to analyze.")

    # --- 3. Run New Analysis ---
    full_results_df = run_multi_timeframe_analysis(tickers_to_analyze, status_callback=print)
    
    # --- 4. Generate Contextual Recommendations ---
    results_with_recs_df = generate_recommendations(full_results_df.copy(), previous_results_df)

    # Filter for actionable signals (Moderate or Strong)
    actionable_df = results_with_recs_df[
        results_with_recs_df['Signal'].str.contains('Strong|Moderate', na=False)
    ].reset_index(drop=True)

    today_str = datetime.now().strftime('%Y-%m-%d')

    # --- 5. Send Email Report ---
    if not actionable_df.empty:
        subject = f"Trading Signals & Recommendations - {today_str}"
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: sans-serif; }}
                table {{ border-collapse: collapse; width: auto; font-size: 12px; }}
                th, td {{ border: 1px solid #dddddd; text-align: left; padding: 6px; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
            </style>
        </head>
        <body>
            <h2>Actionable Trading Signals & Recommendations</h2>
            <p>Analysis completed on {today_str}. The following signals were identified:</p>
            {actionable_df.to_html(index=False, float_format='{:,.4f}'.format)}
            <br>
            <p><em>This is an automated report. Please perform your own due diligence.</em></p>
        </body>
        </html>
        """
        send_email_notification(subject, html_body)
    else:
        subject = f"No Actionable Trading Signals Found - {today_str}"
        html_body = f"<html><body><h2>No actionable signals (Moderate, Strong, or Super Strong) were found for the monitored tickers on {today_str}.</h2></body></html>"
        print("No actionable signals found.")
        send_email_notification(subject, html_body)

    # --- 6. Save Current State for Next Run ---
    try:
        full_results_df.to_csv(HISTORY_FILE, index=False)
        print(f"Successfully saved current analysis to '{HISTORY_FILE}' for the next run.")
    except Exception as e:
        print(f"Error saving analysis history: {e}")


if __name__ == "__main__":
    main()
