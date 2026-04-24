"""
Google Sheets data loader.

Every public function is cached for 8 hours (TTL = 28800 s).
The service account JSON is read from st.secrets["GOOGLE_SERVICE_ACCOUNT"].
"""

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

SHEET_ID = "1uJoD2JRvzRpn2KHJa80aZADQ2DfRwm2qbZKMuv0PKBM"
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


# ── Connection ────────────────────────────────────────────

@st.cache_resource
def _client():
    info  = dict(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def _ws(name: str):
    return _client().open_by_key(SHEET_ID).worksheet(name)


# ── Range → DataFrame ─────────────────────────────────────

def _range_to_df(ws, range_str: str, header_idx: int | None = None) -> pd.DataFrame:
    """
    Read a cell range from a worksheet.
    First non-empty row is treated as headers unless header_idx is specified.
    Completely empty rows are dropped.
    """
    values = ws.get(range_str)
    if not values:
        return pd.DataFrame()

    # Use forced header index or auto-detect
    if header_idx is None:
        header_idx = 0
        for i, row in enumerate(values):
            if sum(1 for c in row if str(c).strip()) >= 2:
                header_idx = i
                break

    headers   = [str(c).strip() for c in values[header_idx]]
    data_rows = values[header_idx + 1 :]

    # Drop completely empty rows
    data_rows = [r for r in data_rows if any(str(c).strip() for c in r)]
    if not data_rows:
        return pd.DataFrame()

    # Normalise row width
    n         = len(headers)
    data_rows = [list(r)[:n] + [""] * max(0, n - len(r)) for r in data_rows]

    # Deduplicate / fill blank header names
    seen  = {}
    clean = []
    for h in headers:
        if not h:
            h = f"_col{len(clean)}"
        if h in seen:
            seen[h] += 1
            h = f"{h}_{seen[h]}"
        else:
            seen[h] = 0
        clean.append(h)

    df = pd.DataFrame(data_rows, columns=clean)
    # Drop filler columns created for unnamed sheet columns
    df = df.loc[:, ~df.columns.str.startswith("_col")]
    return df


# ── Last updated ──────────────────────────────────────────

@st.cache_data(ttl=28800)
def load_last_updated() -> str:
    """Returns the IST time when this cache was last populated.
    Cached for the same 8h TTL as all data — only updates when data refreshes."""
    from datetime import datetime, timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)
    return f"{now.strftime('%b')} {now.day}, {now.strftime('%Y')} {now.strftime('%I:%M %p').lstrip('0')} IST"


# ── Stocks metadata (price as of + updated at) ────────────

@st.cache_data(ttl=28800)
def load_stocks_metadata(sheet_name: str) -> tuple:
    """
    Reads A1 (price_as_of) and A2 (updated_at) written by stocks_data.py.
    Returns (price_as_of, updated_at) strings.
    """
    ws   = _ws(sheet_name)
    vals = ws.get("A1:A2")
    price_as_of = vals[0][0] if vals and vals[0] else ""
    updated_at  = vals[1][0] if len(vals) > 1 and vals[1] else ""
    return price_as_of, updated_at


# ── Section loaders ───────────────────────────────────────

@st.cache_data(ttl=28800)
def load_sp500_sectors():
    ws = _ws("S&P500 Sectors")
    return _range_to_df(ws, "B3:K14")


@st.cache_data(ttl=28800)
def load_global_indices():
    ws = _ws("Global Indices")
    t1 = _range_to_df(ws, "B4:K17")
    t2 = _range_to_df(ws, "B22:K80")
    return t1, t2


@st.cache_data(ttl=28800)
def load_nifty_indices():
    ws = _ws("NIFTY Indices")
    t1 = _range_to_df(ws, "B3:K17")
    t2 = _range_to_df(ws, "B20:K28")
    return t1, t2


@st.cache_data(ttl=28800)
def load_nifty_sectors():
    ws = _ws("NIFTY Sectors")
    return _range_to_df(ws, "B3:K17")


@st.cache_data(ttl=28800)
def load_nifty_momentum_50():
    ws = _ws("NIFTY500Moment.50")
    return _range_to_df(ws, "B4:L54")


@st.cache_data(ttl=28800)
def load_nifty500_sectors():
    ws = _ws("NIFTY500Moment.50")
    return _range_to_df(ws, "B66:E103")


@st.cache_data(ttl=28800)
def load_nifty_momentum_sectors():
    ws = _ws("NIFTY500Moment.50")
    return _range_to_df(ws, "H66:K83")


@st.cache_data(ttl=28800)
def load_etfs_us():
    ws = _ws("ETFs US")
    return _range_to_df(ws, "B2:M210")


@st.cache_data(ttl=28800)
def load_leveraged_funds():
    ws = _ws("Biggest Leveraged Funds ")   # trailing space is intentional
    return _range_to_df(ws, "A6:M122", header_idx=0)


@st.cache_data(ttl=28800)
def load_etfs_india():
    ws = _ws("ETFs India")
    return _range_to_df(ws, "B6:L100", header_idx=0)


@st.cache_data(ttl=28800)
def load_crypto():
    ws = _ws("Crypto")
    return _range_to_df(ws, "A103:K118", header_idx=0)


@st.cache_data(ttl=28800)
def load_mutual_funds():
    ws = _ws("Mutual Funds India")
    return _range_to_df(ws, "B2:G67")


@st.cache_data(ttl=28800)
def load_gl_us():
    ws      = _ws("Top G&L US")
    gainers = _range_to_df(ws, "A4:D19")
    losers  = _range_to_df(ws, "F4:I19")
    return gainers, losers


@st.cache_data(ttl=28800)
def load_gl_india():
    ws      = _ws("Top G&L India")
    gainers = _range_to_df(ws, "A4:D19")
    losers  = _range_to_df(ws, "F4:I19")
    return gainers, losers


@st.cache_data(ttl=28800)
def load_ath_us():
    ws = _ws("ATH US")
    return _range_to_df(ws, "A4:M200")


@st.cache_data(ttl=28800)
def load_ath_india():
    ws = _ws("ATH India")
    return _range_to_df(ws, "A4:M200")


@st.cache_data(ttl=28800)
def load_investor_holdings():
    ws = _ws("Indian Investor Update")
    return _range_to_df(ws, "B2:F500")


@st.cache_data(ttl=28800)
def load_hedge_funds():
    ws = _ws("Hedge Funds ")   # trailing space is intentional
    return _range_to_df(ws, "A4:G15")


@st.cache_data(ttl=28800)
def load_top_hedge_fund_investments():
    ws = _ws("Top Hedge Fund Investments")
    return _range_to_df(ws, "B1:I11")
