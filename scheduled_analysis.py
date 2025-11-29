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
INPUT_TICKERS = [
    "GBPUSD=X", 
    "EURUSD=X", 
    "JPY=X", 
    "GBPCAD=X", 
    "AUDUSD=X", 
    "NZDUSD=X"
]

DATA_DIR = "data/incoming"
OUTPUT_FILE = f"Trade_Signals_{datetime.now().strftime('%Y%m%d')}.csv"

# Email Config (Secrets from GitHub)
EMAIL_SENDER = os.environ.get("EMAIL_SENDER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER")

# --- NEW: Helper Functions for Economic Calendar ---

def auto_find_calendar(data_dir):
    """
    Scans the data directory for any CSV that looks like an economic calendar.
    Returns the path of the first valid match.
    """
    if not os.path.exists(data_dir):
        print(f"Directory not found: {data_dir}")
        return None

    # Columns expected in the manual event list
    required_cols = {'START', 'CURRENCY', 'IMPACT'} 

    print(f"Scanning {data_dir} for economic calendar files...")
    
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv") and "Trade_Signals" not in filename:
            filepath = os.path.join(data_dir, filename)
            try:
                # Read header only to be fast
                df = pd.read_csv(filepath, nrows=0)
                file_cols = {c.strip().upper() for c in df.columns}
                
                # Check if it contains the required columns
                if required_cols.issubset(file_cols):
                    print(f" -> Auto-Detected Economic Calendar: {filename}")
                    return filepath
            except Exception:
                continue
    
    print(" -> No Economic Calendar file found. Proceeding without economic analysis.")
    return None

def load_economic_data(filepath):
    """Loads and standardizes the economic calendar CSV."""
    if not filepath or not os.path.exists(filepath):
        return None
    
    try:
        df = pd.read_csv(filepath)
        # Standardize Columns
        df.columns = [c.strip().title() for c in df.columns] 
        # Parse Dates (US Format MM/DD/YYYY typical for calendars)
        df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
        # Clean Text
        df['Impact'] = df['Impact'].astype(str).str.upper()
        df['Currency'] = df['Currency'].astype(str).str.upper()
        return df
    except Exception as e:
        print(f"Error reading calendar: {e}")
        return None

# --- Existing Email Function ---

def send_email(subject, body, attachment_path=None):
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        print("Skipping email: Credentials not set.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {os.path.basename(attachment_path)}",
        )
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
    
    # 1. Load Economic Calendar (NEW)
    eco_file = auto_find_calendar(DATA_DIR)
    eco_df = load_economic_data(eco_file)
    
    all_results = []

    # 2. Run Scanner
    for ticker in INPUT_TICKERS:
        print(f"Scanning {ticker}...")
        try:
            # Pass the loaded eco_df to the logic function
            df_result = logic.run_scanner([ticker], eco_df=eco_df)
            
            if not df_result.empty:
                all_results.append(df_result)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # 3. Process & Save Results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        
        # Ensure 'Remarks' is the last column if it exists
        if 'Remarks' in final_df.columns:
            cols = [c for c in final_df.columns if c != 'Remarks'] + ['Remarks']
            final_df = final_df[cols]
        
        # Save CSV
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
            
        out_path = os.path.join(DATA_DIR, OUTPUT_FILE)
        final_df.to_csv(out_path, index=False)
        print(f"Results saved to {out_path}")
        
        # Send Email
        # Customize email body to include top signals
        email_body = f"Trade signals generated for {datetime.now().strftime('%Y-%m-%d')}.\n\n"
        email_body += "Top Signals:\n"
        email_body += final_df[['Ticker', 'Signal', 'Current Price']].to_string(index=False)
        if 'Remarks' in final_df.columns:
             email_body += "\n\nNote: Check 'Remarks' column for High Impact Economic Events."
        
        send_email(
            subject=f"Daily Trade Signals - {datetime.now().strftime('%Y-%m-%d')}",
            body=email_body,
            attachment_path=out_path
        )
    else:
        print("No signals found today.")
        send_email(
            subject=f"No Trade Signals - {datetime.now().strftime('%Y-%m-%d')}",
            body="No tickers met the entry criteria today."
        )

if __name__ == "__main__":
    main()
