# File: run_scheduled_analysis.py

import os
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pandas as pd

# Import the main analysis function from our logic file
from stock_analyzer_logic import run_full_analysis

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

    # Attach the HTML body
    msg.attach(MIMEText(html_body, "html"))

    try:
        print("Connecting to email server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Secure the connection
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL.split(','), msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")


def main():
    """Main function to run the analysis and send the report."""
    print(f"Starting scheduled analysis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Define the folder where you store your CSV files
    source_folder = 'ticker_sources'
    # Find all files ending with .csv inside that folder
    csv_files = glob.glob(os.path.join(source_folder, '*.csv'))

    if not csv_files:
        print(f"Error: No CSV files found in the '{source_folder}' directory.")
        return

    all_tickers = []
    print(f"Reading tickers from {len(csv_files)} CSV file(s)...")
    for file in csv_files:
        try:
            # Read the CSV file using pandas
            df = pd.read_csv(file)
            # IMPORTANT: Assumes tickers are in the FIRST column.
            # If your ticker column is named 'Symbol', use: df['Symbol'].dropna().tolist()
            tickers_from_file = df.iloc[:, 0].dropna().tolist()
            all_tickers.extend(tickers_from_file)
            print(f"  - Loaded {len(tickers_from_file)} tickers from {os.path.basename(file)}")
        except Exception as e:
            print(f"  - Could not read file {os.path.basename(file)}. Error: {e}")

    # Create a unique, sorted list of tickers to avoid duplicates
    tickers_to_analyze = sorted(list(set(all_tickers)))

    if not tickers_to_analyze:
        print("No tickers were loaded from the CSV files. Exiting.")
        return
    
    print(f"\nFound a total of {len(tickers_to_analyze)} unique tickers to analyze.")

    # Run the analysis (status_callback prints progress to the console/log)
    full_results_df = run_full_analysis(tickers_to_analyze, status_callback=print)
    
    # Filter for actionable signals
    actionable_df = full_results_df[full_results_df['Signal'] != 'Hold for now'].reset_index(drop=True)

    today_str = datetime.now().strftime('%Y-%m-%d')

    if not actionable_df.empty:
        subject = f"Stock Signals Found - {today_str}"
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
            <h2>High-Conviction Stock Signals</h2>
            <p>Analysis completed on {today_str}. The following signals were identified:</p>
            {actionable_df.to_html(index=False)}
            <br>
            <p><em>This is an automated report. Please perform your own due diligence.</em></p>
        </body>
        </html>
        """
        send_email_notification(subject, html_body)
    else:
        subject = f"No Stock Signals Found - {today_str}"
        html_body = f"<html><body><h2>No Strong or Super Strong signals were found for the monitored tickers on {today_str}.</h2></body></html>"
        print("No actionable signals found.")
        send_email_notification(subject, html_body)


if __name__ == "__main__":
    main()