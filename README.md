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
