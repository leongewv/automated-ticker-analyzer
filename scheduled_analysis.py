# File: scheduled_analysis.py

import os
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pandas as pd

# Import the main analysis function from our logic file
from stock_analyzer_logic import run_full_analysis

# --- Configuration ---
HISTORY_FILE = 'analysis_history.csv'  # File to store the last run's results

# --- Email Configuration ---
# Load credentials from environment variables for security
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


def send_email_notification(subject, html_body):
    """Sends an email with the given subject and HTML body."""
    if not all([SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL]):
        print("Error: Email credentials or recipient not set in environment variables.")
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
    """Compares current signals with previous ones to generate new recommendations."""
    if previous_df.empty:
        current_df['Recommendation'] = 'First run, no prior data to compare.'
        return current_df

    merged_df = pd.merge(
        current_df,
        previous_df[['Instrument', 'Signal']],
        on='Instrument',
        how='left',
        suffixes=('', '_prev')
    ).fillna({'Signal_prev': 'N/A'})

    def get_recommendation(row):
        current = row['Signal']
        previous = row['Signal_prev']

        if not isinstance(current, str):
            return "Invalid signal data."
        
        # Explicitly handle all scenarios where the signal has NOT changed.
        if current == previous:
            if current == 'Hold for now':
                return "No change."
            else:
                # This now handles 'Super Strong' -> 'Super Strong', etc.
                return "Monitor signal change."

        direction = 'long' if 'Buy' in current else 'short'

        # Logic for when the signal HAS changed
        if 'Super Strong' in previous and current in ['Strong Buy', 'Strong Sell']:
            return f"📉 Degradation: Consider reducing {direction} positions."

        if 'Strong' in previous and 'Moderate Strong' in current:
            return f"📈 Improvement: Consider re-entering {direction} positions."
        
        if 'Moderate Strong' in previous and 'Super Strong' in current:
            return f"🚀 Alignment: Accumulate {direction} positions."
            
        if 'Strong' in previous and 'Super Strong' in current:
            return f"🔥 Strengthening: Accumulate {direction} positions."

        if 'Hold' in previous or previous == 'N/A':
             return f"New Signal: {current}"
        
        # Default for any other unhandled signal CHANGE
        return "Monitor signal change."

    merged_df['Recommendation'] = merged_df.apply(get_recommendation, axis=1)
    
    all_cols = list(merged_df.columns)
    signal_index = all_cols.index('Signal')
    rec_col = merged_df.pop('Recommendation')
    merged_df.insert(signal_index + 1, 'Recommendation', rec_col)
    
    merged_df.drop(columns=['Signal_prev'], inplace=True)
    return merged_df


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

    # Define the folder where you store your CSV files
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
        print("No tickers were loaded. Exiting.")
        return
    
    print(f"\nFound a total of {len(tickers_to_analyze)} unique tickers to analyze.")

    # --- 2. Run New Analysis ---
    full_results_df = run_full_analysis(tickers_to_analyze, status_callback=print)
    
    # --- 3. Generate Contextual Recommendations ---
    results_with_recs_df = generate_recommendations(full_results_df.copy(), previous_results_df)

    actionable_df = results_with_recs_df[results_with_recs_df['Signal'] != 'Hold for now'].reset_index(drop=True)

    today_str = datetime.now().strftime('%Y-%m-%d')

    if not actionable_df.empty:
        subject = f"Stock Signals & Recommendations - {today_str}"
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #dddddd; text-align: left; padding: 8px; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
            </style>
        </head>
        <body>
            <h2>High-Conviction Stock Signals & Recommendations</h2>
            <p>Analysis completed on {today_str}. The following signals were identified:</p>
            {actionable_df.to_html(index=False)}
            <br>
            <p><em>This is an automated report. Please perform your own due diligence.</em></p>
        </body>
        </html>
        """
        send_email_notification(subject, html_body)
    else:
        subject = f"No Actionable Stock Signals Found - {today_str}"
        html_body = f"<html><body><h2>No actionable signals were found for the monitored tickers on {today_str}.</h2></body></html>"
        print("No actionable signals found.")
        send_email_notification(subject, html_body)

    # --- 4. Save Current State for Next Run ---
    try:
        full_results_df.to_csv(HISTORY_FILE, index=False)
        print(f"Successfully saved current analysis to '{HISTORY_FILE}' for the next run.")
    except Exception as e:
        print(f"Error saving analysis history: {e}")


if __name__ == "__main__":
    main()
