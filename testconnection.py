import yfinance as yf
import pandas as pd

print("--- DIAGNOSTIC: STARTING CONNECTION TEST ---")
try:
    # Attempt to fetch SPY data
    print("Attempting to fetch data for 'SPY'...")
    ticker = "SPY"
    df = yf.Ticker(ticker).history(period="5d", interval="1d")
    
    if df.empty:
        print("❌ FAILURE: Connection successful, but returned EMPTY data.")
        print("   -> Possible Rate Limit or IP Ban on this Runner.")
    else:
        print("✅ SUCCESS: Data received!")
        print(f"   -> Retrieved {len(df)} rows.")
        print(df.head())

except Exception as e:
    print(f"❌ CRITICAL FAILURE: Could not connect to Yahoo Finance.")
    print(f"   -> Error details: {e}")

print("--- DIAGNOSTIC: TEST COMPLETE ---")
