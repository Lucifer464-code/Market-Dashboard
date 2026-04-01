import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date

# -----------------------
# Fetch crypto data
# -----------------------
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 15,
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "1w,1m,3m,6m,1y"
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()

df = pd.DataFrame(data)

columns_map = {
    "name": "Crypto",
    "symbol": "Ticker",
    "market_cap": "Market Cap (USD)",
    "current_price": "Price (USD)",
    "price_change_percentage_7d_in_currency": "1W %",
    "price_change_percentage_30d_in_currency": "1M %",
    "price_change_percentage_90d_in_currency": "3M %",
    "price_change_percentage_180d_in_currency": "6M %",
    "price_change_percentage_1y_in_currency": "1Y %",
}

# Ensure all expected columns exist
for col in columns_map:
    if col not in df.columns:
        df[col] = None

df = df[list(columns_map.keys())]
df.rename(columns=columns_map, inplace=True)

df["Date"] = date.today().isoformat()

df.columns = [
    "Crypto",
    "Ticker",
    "Market Cap (USD)",
    "Price (USD)",
    "1W %",
    "1M %",
    "3M %",
    "6M %",
    "1Y %",
]

df["Date"] = date.today().isoformat()

# -----------------------
# Google Sheets auth
# -----------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file(
    "service_account.json",
    scopes=SCOPES
)

client = gspread.authorize(creds)

sheet = client.open("Dataset for Market Report").worksheet("Crypto")

sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())
sheet.update("AA1", [[date.today().isoformat()]])

print("Crypto data successfully updated")
