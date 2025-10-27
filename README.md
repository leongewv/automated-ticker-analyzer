## 📂 File Structure & Purpose

Here is a breakdown of all the files in this repository and what they do.

### `.github/workflows/`
This folder contains the GitHub Actions automation scripts.

* `run_analysis.yml`
    * **What it does:** This is the main automation workflow. It runs `scheduled_analysis.py` on a set schedule (e.g., every 4 hours, or once a day).
    * **How to customize:** Edit this file to change the `cron` schedule for how often the analysis runs.

* `test-schedule.yml`
    * **What it does:** A secondary workflow, likely for testing new schedules or script changes without interrupting the main `run_analysis.yml` workflow.
    * **How to customize:** You can safely ignore this or use it to test your own changes.

### `ticker_sources/`
This is the **most important folder for you to customize.** It holds all the instrument lists you want to analyze.

* `Crypto universe.csv`, `Yellow list.csv`, `forex.csv`, etc.
    * **What they are:** These are your ticker lists. The script will automatically read **every `.csv` file** in this folder.
    * **How to customize:** **Add, delete, or edit any `.csv` files in this folder.** The script only reads the **first column** of each file, so make sure your tickers are in column A.

### Root Directory Files

* `scheduled_analysis.py`
    * **What it does:** This is the **main coordinator script** that the GitHub Action runs. It does the following:
        1.  Loads all tickers from the `ticker_sources/` folder.
        2.  Calls the `run_multi_timeframe_analysis()` function from the `stock_analyzer_logic.py` script.
        3.  Loads the old results from `analysis_history.csv` to compare.
        4.  Generates the "Recommendation" column (e.g., "New Signal," "Upgrade").
        5.  Sends the final HTML email report.
    * **How to customize:** Edit this file if you want to change the email's HTML formatting or the logic for comparing new vs. old signals.

* `stock_analyzer_logic.py`
    * **What it does:** This is the **"brains" of the operation.** It contains all the core financial analysis and trading strategy. It fetches data from `yfinance` and performs all the technical analysis (Bollinger Bands, EMA, Fibonacci, etc.).
    * **How to customize:** Edit this file to change the trading strategy itself (e.g., change `trend_lookback = 120` or `proximity_pct = 0.03`) or to use a different data source.

* `analysis_history.csv`
    * **What it does:** This is a **state file** created and managed by the script. It stores the full results from the *previous* run. This is essential for the script to know if a signal is "New" or a "Downgrade."
    * **How to customize:** **Do not edit or delete this file.** The script needs it for comparison. If you delete it, the next run will simply report all signals as "New."

* `stock_analyzer_ui.py`
    * **What it does:** This is a helper script for running the analysis **on your local computer.** It provides a simple user interface (likely Streamlit or Tkinter) for testing.
    * **How to customize:** This file is **not used by the automated GitHub Action.** You can use it locally to test changes to your strategy in `stock_analyzer_logic.py` before committing them.

* `.gitignore`
    * **What it does:** A standard Git file that tells the repository which files to ignore (e.g., Python cache files).
    * **How to customize:** You can safely ignore this unless you have specific local files you want to prevent from being uploaded.

* `README.md`
    * **What it does:** This file! It provides instructions and documentation for the project.

---

## ⚙️ Setup for Your Own Repository

To use this template, you only need to configure the email notifications. The stock analysis logic works automatically without any API keys.

Go to your new repository's `Settings > Secrets and variables > Actions` and add the following three secrets:

* `SENDER_EMAIL`
  * **What it is:** Your "from" email address (e.g., your Gmail address).

* `SENDER_PASSWORD`
  * **What it is:** The password for your email.
  * **⚠️ Important:** If you use Gmail, you must generate a 16-digit **"App Password"** from your Google Account security settings. Your normal login password will not work.

* `RECEIVER_EMAIL`
  * **What it is:** The email address(es) you want to send the report to.
  * **Note:** For multiple emails, separate them with a comma (e.g., `email1@gmail.com,email2@yahoo.com`).

---

## 📊 Data Source

This project uses the **`yfinance`** Python library to fetch free stock, forex, and crypto data from Yahoo Finance.

* **File:** `stock_analyzer_logic.py`
* **Library:** `yfinance`
* **Cost:** Free
* **Requires API Key:** No

### How to Customize the Data Source

If you want to use a different data provider (e.g., Alpha Vantage, Polygon.io, or your own private API):

1.  You would need to edit the `get_data()` function inside the `stock_analyzer_logic.py` file.
2.  You would replace the `yf.Ticker(ticker).history(...)` call with your new provider's API calls.
3.  If your new provider requires an API key, you would need to add it as a new GitHub Secret (e.g., `STOCK_API_KEY`) and update the Python code to read it using `os.getenv("STOCK_API_KEY")`.
