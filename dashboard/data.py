"""
Google Sheets data loader.

Every public function is cached for 8 hours (TTL = 28800 s).
The service account JSON is read from st.secrets["GOOGLE_SERVICE_ACCOUNT"].
"""

import gspread
import pandas as pd
import streamlit as st
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

SHEET_ID = "1uJoD2JRvzRpn2KHJa80aZADQ2DfRwm2qbZKMuv0PKBM"
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


# ── Connection ────────────────────────────────────────────

@st.cache_resource
def _client():
    info  = dict(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def _http_session():
    """Authorized HTTP session for direct Sheets v4 API calls (needed for
    hyperlink metadata, which gspread.get() strips)."""
    info  = dict(st.secrets["GOOGLE_SERVICE_ACCOUNT"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return AuthorizedSession(creds)


def _ws(name: str):
    return _client().open_by_key(SHEET_ID).worksheet(name)


def _fetch_links(sheet_title: str, range_str: str) -> list[str | None]:
    """Return per-row hyperlinks for the FIRST column of `range_str`.
    Pass a single-column range (e.g. 'B3:B17') to fetch links for that column.
    Length matches the row span of the range. Cells without a hyperlink
    return None.
    """
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
    resp = _http_session().get(
        url,
        params={
            "ranges": f"'{sheet_title}'!{range_str}",
            "includeGridData": "true",
            "fields": "sheets(data(rowData(values(hyperlink))))",
        },
    )
    resp.raise_for_status()
    data = resp.json()
    try:
        rows = data["sheets"][0]["data"][0].get("rowData", [])
    except (KeyError, IndexError):
        return []
    out: list[str | None] = []
    for row in rows:
        cells = row.get("values") or [{}]
        out.append(cells[0].get("hyperlink"))
    return out


# ── Range → DataFrame ─────────────────────────────────────

def _expected_col_count(range_str: str) -> int | None:
    """Parse a sheet range like 'B3:L17' and return the column span (11)."""
    import re
    m = re.match(r"^([A-Za-z]+)\d+:([A-Za-z]+)\d+$", range_str.strip())
    if not m:
        return None
    def col_to_num(s):
        n = 0
        for c in s.upper():
            n = n * 26 + (ord(c) - 64)
        return n
    return col_to_num(m.group(2)) - col_to_num(m.group(1)) + 1


def _range_to_df(ws, range_str: str, header_idx: int | None = None,
                 keep_blank_cols: bool = False) -> pd.DataFrame:
    """
    Read a cell range from a worksheet.
    First non-empty row is treated as headers unless header_idx is specified.
    Completely empty rows are dropped.
    keep_blank_cols=True preserves columns whose header cell is blank
    (instead of silently dropping them) — useful when a sheet column may
    have a missing header but the data is still wanted.
    """
    values = ws.get(range_str)
    if not values:
        return pd.DataFrame()

    # gspread trims trailing blank cells per row, so a row of width 11 with
    # an empty last cell comes back as length 10. Pad every row up to the
    # column span implied by range_str so the rightmost column survives.
    expected = _expected_col_count(range_str)
    if expected:
        values = [list(r) + [""] * max(0, expected - len(r)) for r in values]

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
    if not keep_blank_cols:
        # Drop filler columns created for unnamed sheet columns
        df = df.loc[:, ~df.columns.str.startswith("_col")]
    return df


def _attach_col_links(df: pd.DataFrame, sheet_title: str, full_range: str,
                      link_cols: list[tuple[str, str]],
                      header_idx: int = 0) -> pd.DataFrame:
    """Augment `df` with one sidecar `__link__<header>` column per entry in
    `link_cols`. Each entry is (sheet_column_letter, df_header_name) — the
    sheet column whose hyperlinks should populate the sidecar for the given
    DataFrame column.

    `full_range` is the range that was passed to `_range_to_df` (e.g.
    'B3:M17') — used only to determine which sheet rows correspond to which
    DataFrame rows after blank-row filtering.
    """
    if df.empty or not link_cols:
        return df

    # Re-fetch the full range's text once so we can detect blank rows the
    # same way `_range_to_df` did, keeping link rows aligned with df rows.
    ws = _ws(sheet_title)
    raw_values = ws.get(full_range)[header_idx + 1:]   # drop header line

    # Derive the row span from the full range (e.g. "B3:M17" -> rows 3..17)
    import re
    m = re.match(r"^([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)$", full_range.strip())
    if not m:
        return df
    start_row = int(m.group(2)) + header_idx + 1
    end_row   = int(m.group(4))

    df = df.copy()
    for sheet_col, df_header in link_cols:
        col_range = f"{sheet_col}{start_row}:{sheet_col}{end_row}"
        links = _fetch_links(sheet_title, col_range)

        aligned: list[str | None] = []
        for row, link in zip(raw_values, links):
            if any(str(c).strip() for c in row):
                aligned.append(link)
            if len(aligned) == len(df):
                break
        while len(aligned) < len(df):
            aligned.append(None)

        df[f"__link__{df_header}"] = aligned

    return df


# ── Last updated ──────────────────────────────────────────

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
    df = _range_to_df(ws, "B3:K14")
    # Attach hyperlinks for the Sector name col (B)
    links = [("B", df.columns[0])] if not df.empty else []
    return _attach_col_links(df, "S&P500 Sectors", "B3:K14", links)


@st.cache_data(ttl=28800)
def load_global_indices():
    ws = _ws("Global Indices")
    t1 = _range_to_df(ws, "B4:K17")
    t2 = _range_to_df(ws, "B22:K80")
    return t1, t2




@st.cache_data(ttl=28800)
def load_nifty_indices():
    ws = _ws("NIFTY Indices")
    t1 = _range_to_df(ws, "B3:M17", keep_blank_cols=True)
    t2 = _range_to_df(ws, "B20:M28", keep_blank_cols=True)
    # Attach hyperlinks for the Index Name col (B) and the Tradingview col (M)
    t1_links = [("B", t1.columns[0]), ("M", "Tradingview")] if not t1.empty else []
    t2_links = [("B", t2.columns[0]), ("M", "Tradingview")] if not t2.empty else []
    t1 = _attach_col_links(t1, "NIFTY Indices", "B3:M17", t1_links)
    t2 = _attach_col_links(t2, "NIFTY Indices", "B20:M28", t2_links)
    return t1, t2


@st.cache_data(ttl=28800)
def load_nifty_sectors():
    ws = _ws("NIFTY Sectors")
    df = _range_to_df(ws, "B3:M17", keep_blank_cols=True)
    links = [("B", df.columns[0]), ("M", "Tradingview")] if not df.empty else []
    return _attach_col_links(df, "NIFTY Sectors", "B3:M17", links)


@st.cache_data(ttl=28800)
def load_nifty_momentum_50():
    ws = _ws("NIFTY500Moment.50")
    # Consolidated momentum table: header row 4, data rows 5-29.
    # Cols: Name, Mar Cap Rs.Cr., P/E, 1D%, 5D%, 1M%, 3M%, 6M%, 1Y%, 3Y%.
    return _range_to_df(ws, "O4:X40")


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
def load_commodity_etfs():
    ws = _ws("Commodity ETFs")
    # Header row 3, data from row 4. Generous lower bound so new rows appear.
    return _range_to_df(ws, "B3:N40", header_idx=0)


@st.cache_data(ttl=28800)
def load_leveraged_commodity_funds():
    ws = _ws("Biggest Leveraged Funds(Com.)")
    # Header row 4, data from row 5. Generous lower bound so new rows appear.
    return _range_to_df(ws, "A4:N40", header_idx=0)


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
