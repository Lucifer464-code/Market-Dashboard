import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date

# -----------------------
# Helper: convert AUM text → numeric
# -----------------------
def parse_aum(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    try:
        if value.endswith("B"):
            return float(value.replace("B", "")) * 1_000_000_000
        if value.endswith("M"):
            return float(value.replace("M", "")) * 1_000_000
        return float(value.replace(",", ""))
    except ValueError:
        return None


# -----------------------
# Fetch ETF US data
# -----------------------
url = "https://etfdb.com/etfs/asset-class/equity/"
tables = pd.read_html(url)

df = tables[0]
df.columns = [c.strip() for c in df.columns]

# Defensive column presence
required_cols = [
    "Symbol",
    "ETF Name",
    "AUM",
    "Price",
    "1W",
    "1M",
    "3M",
    "6M",
    "1Y",
    "3Y",
]

for col in required_cols:
    if col not in df.columns:
        df[col] = None

df = df[required_cols]

# -----------------------
# Build final schema
# -----------------------
df.rename(
    columns={
        "Symbol": "Ticker",
        "ETF Name": "Name",
        "AUM": "AUM (formatted)",
        "Price": "Price",
        "1W": "1W",
        "1M": "1M",
        "3M": "3M",
        "6M": "6M",
        "1Y": "1Y",
        "3Y": "3Y",
    },
    inplace=True,
)

# Create numeric AUM column (for sorting)
df["AUM (numeric)"] = df["AUM (formatted)"].apply(parse_aum)

# Reorder columns exactly as requested
df = df[
    [
        "Ticker",
        "Name",
        "AUM (numeric)",
        "AUM (formatted)",
        "Price",
        "1W",
        "1M",
        "3M",
        "6M",
        "1Y",
        "3Y",
    ]
]

# Sort by numeric AUM (descending)
df.sort_values("AUM (numeric)", ascending=False, inplace=True)

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
    scopes=SCOPES,
)

client = gspread.authorize(creds)

sheet = client.open("Dataset for Market Report").worksheet("ETFs US")

sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())
sheet.update("AA1", [[date.today().isoformat()]])

print("ETFs US data successfully updated")
