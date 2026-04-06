import yfinance as yf
import gspread
import pandas as pd
import numpy as np
import os
import pickle
import requests
import difflib
import time
from zoneinfo import ZoneInfo

from kiteconnect import KiteConnect
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed


def _is_market_open(market: str) -> bool:
    """Return True if the given market is currently in its regular session."""
    if market == "US":
        tz      = ZoneInfo("America/New_York")
        now     = datetime.now(tz)
        open_t  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
        close_t = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    else:  # IN
        tz      = ZoneInfo("Asia/Kolkata")
        now     = datetime.now(tz)
        open_t  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return now.weekday() < 5 and open_t <= now <= close_t


def _make_metadata(market: str):
    """
    Build (price_as_of, updated_at) strings for sheet A1/A2 metadata cells.
    market: "US", "IN", "CRYPTO", or "NAV"
    """
    ist     = ZoneInfo("Asia/Kolkata")
    now_ist = datetime.now(ist)
    updated_at = (
        f"Updated {now_ist.strftime('%b')} {now_ist.day}, {now_ist.year}"
        f"  ·  {int(now_ist.strftime('%I'))}:{now_ist.strftime('%M %p')} IST"
    )

    if market == "NAV":
        price_as_of = f"NAV as on {now_ist.strftime('%b')} {now_ist.day}, {now_ist.year}  (End of Day)"
    elif market == "CRYPTO":
        price_as_of = (
            f"Price as on {now_ist.strftime('%b')} {now_ist.day}, {now_ist.year}"
            f"  ·  {int(now_ist.strftime('%I'))}:{now_ist.strftime('%M %p')} IST"
            f"  (Live)"
        )
    elif market == "GLOBAL":
        # Mixed timezones — show (Live) only when US market is open, no (Close) label
        now = datetime.now(ZoneInfo("America/New_York"))
        if _is_market_open("US"):
            price_as_of = (
                f"Price as on {now.strftime('%b')} {now.day}, {now.year}"
                f"  ·  {int(now.strftime('%I'))}:{now.strftime('%M %p')} ET"
                f"  (Live)"
            )
        else:
            price_as_of = f"Price as on {now.strftime('%b')} {now.day}, {now.year}"
    elif _is_market_open(market):
        tz_name  = "America/New_York" if market == "US" else "Asia/Kolkata"
        tz_label = "ET" if market == "US" else "IST"
        now      = datetime.now(ZoneInfo(tz_name))
        price_as_of = (
            f"Price as on {now.strftime('%b')} {now.day}, {now.year}"
            f"  ·  {int(now.strftime('%I'))}:{now.strftime('%M %p')} {tz_label}"
            f"  (Live)"
        )
    else:
        tz_name = "America/New_York" if market == "US" else "Asia/Kolkata"
        now     = datetime.now(ZoneInfo(tz_name))
        price_as_of = f"Price as on {now.strftime('%b')} {now.day}, {now.year}  (Close)"

    return price_as_of, updated_at


# ======================================================
# CONFIGURATION
# ======================================================

class Config:
    SHEET_ID = "1uJoD2JRvzRpn2KHJa80aZADQ2DfRwm2qbZKMuv0PKBM"
    ZERODHA_API_KEY = "gajj389620ihaoue"
    SERVICE_FILE = "service_account.json"
    ZERODHA_TOKEN_FILE = "zerodha_access_token.txt"


# ======================================================
# GOOGLE SHEETS CLIENT
# ======================================================

class GoogleSheetClient:

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(self, config: Config):
        creds = Credentials.from_service_account_file(
            config.SERVICE_FILE,
            scopes=self.SCOPES,
        )
        self.client = gspread.authorize(creds)
        self.sheet_id = config.SHEET_ID

    def get_worksheet(self, name):
        return self.client.open_by_key(self.sheet_id).worksheet(name)

    def batch_update(self, worksheet, updates):
        if updates:
            worksheet.batch_update(updates)


# ======================================================
# RETURN CALCULATOR
# ======================================================

class ReturnCalculator:

    @staticmethod
    def last_confirmed_close(series):
        """
        Return the last available price from the series.
        During market hours this is the live intraday price; outside hours it
        is the most recent confirmed close. yf.download() includes today's
        live candle when the market is open, so s.iloc[-1] is always correct.
        """
        s = series.dropna()
        if s.empty:
            return None
        return float(s.iloc[-1])

    @staticmethod
    def clean(values):
        cleaned = []
        for v in values:
            if isinstance(v, (float, np.floating)):
                if np.isnan(v) or np.isinf(v):
                    cleaned.append("NA")
                else:
                    cleaned.append(round(float(v), 2))
            else:
                cleaned.append(v)
        return cleaned

    @staticmethod
    def calculate(close_series, current_price, open_series=None):

        if close_series is None or close_series.empty or current_price is None:
            return ["NA"] * 8

        close_series = close_series.dropna().sort_index()

        # Strip timezone and force to datetime64[ns] to avoid comparison errors
        idx = close_series.index
        if hasattr(idx, "tz") and idx.tz is not None:
            idx_naive = pd.to_datetime(idx.tz_localize(None))
        else:
            idx_naive = pd.to_datetime(idx)
        s_naive = pd.Series(close_series.values, index=idx_naive)

        today = pd.Timestamp.now().normalize()

        def price_at(target_date):
            target = pd.Timestamp(target_date)
            # For historical lookbacks: step forward to next trading day (matches Google Finance)
            # but only look within the historical data, not beyond the last available date
            eligible_fwd = s_naive[(s_naive.index >= target) & (s_naive.index <= s_naive.index[-1])]
            if not eligible_fwd.empty:
                return eligible_fwd.iloc[0]
            # Fallback: step backward
            eligible_bwd = s_naive[s_naive.index <= target]
            if eligible_bwd.empty:
                return None
            return eligible_bwd.iloc[-1]

        def price_n_trading_days_ago(n):
            # Anchor to the last available price (live or EOD) — always s_naive[-1].
            # When market is open, s_naive[-1] = live price, so n=1 correctly
            # gives yesterday's close as the base for 1D return.
            last_confirmed_idx = len(s_naive) - 1
            target_idx = last_confirmed_idx - n
            if target_idx < 0 or last_confirmed_idx < 0:
                return None
            return float(s_naive.iloc[target_idx])

        def ret(target_date):
            past_price = price_at(target_date)
            if past_price is None or past_price == 0:
                return "NA"
            return (current_price / past_price - 1) * 100

        def ret_by_trading_days(n):
            past_price = price_n_trading_days_ago(n)
            if past_price is None or past_price == 0:
                return "NA"
            return (current_price / past_price - 1) * 100

        return [
            current_price,
            ret_by_trading_days(1),
            ret_by_trading_days(5),
            ret(today - pd.DateOffset(months=1)),
            ret(today - pd.DateOffset(months=3)),
            ret(today - pd.DateOffset(months=6)),
            ret(today - pd.DateOffset(years=1)),
            ret(today - pd.DateOffset(years=3)),
        ]


# ======================================================
# YFINANCE CLOSE EXTRACTION HELPER
# ======================================================

def _extract_close(data, symbol, symbols):
    """
    Extract a Close price Series from a yf.download() result.

    Handles two MultiIndex layouts produced by different yfinance versions:
      - Newer yfinance (≥ 0.2.38): level0 = price field ("Close", "Open"…),
        level1 = ticker  →  data["Close"][ticker]
      - Older yfinance with group_by="ticker": level0 = ticker, level1 = price
        field  →  data[ticker]["Close"]
      - Flat DataFrame (single-ticker, some versions): data["Close"]

    Returns a pd.Series or None.
    """
    if data is None or (hasattr(data, "empty") and data.empty):
        return None
    try:
        if isinstance(data.columns, pd.MultiIndex):
            level0 = data.columns.get_level_values(0).unique().tolist()
            level1 = data.columns.get_level_values(1).unique().tolist()

            # Newer yfinance: level0 = price field, level1 = ticker
            if "Close" in level0:
                if symbol in level1:
                    return _adjust_for_unrecorded_splits(data["Close"][symbol].dropna())
                if len(level1) == 1:
                    return _adjust_for_unrecorded_splits(data["Close"][level1[0]].dropna())

            # Older yfinance (group_by="ticker"): level0 = ticker, level1 = price field
            if symbol in level0:
                return _adjust_for_unrecorded_splits(data[symbol]["Close"].dropna())
            if len(level0) == 1:
                return _adjust_for_unrecorded_splits(data[level0[0]]["Close"].dropna())
        else:
            # Flat DataFrame (single-ticker in some yfinance versions)
            if "Close" in data.columns:
                return _adjust_for_unrecorded_splits(data["Close"].dropna())
    except Exception:
        pass
    return None


def _adjust_for_unrecorded_splits(close_series, threshold=3.0):
    """
    Fix reverse stock splits that Yahoo Finance hasn't recorded.
    Detects day-over-day price jumps >= threshold (default 3x) and scales
    all prices before that date by the same factor, making the series
    consistent with the post-split price.
    """
    s = close_series.copy().sort_index()
    ratios = s / s.shift(1)
    for date, ratio in ratios[ratios >= threshold].items():
        s[s.index < date] *= ratio
    return s


# ======================================================
# YAHOO DATA ENGINE
# ======================================================

class YahooDataEngine:

    def __init__(self, sheet_client: GoogleSheetClient):
        self.sheet_client = sheet_client

    def update_sheet(self, sheet_name, ticker_range, start_row, output_start_col, market="US"):

        print(f"Updating {sheet_name}...")

        worksheet = self.sheet_client.get_worksheet(sheet_name)
        rows = worksheet.get(ticker_range)

        tickers = []
        row_pointer = start_row

        for r in rows:
            symbol = r[0].strip() if r else ""
            if symbol:
                tickers.append((symbol, row_pointer))
            row_pointer += 1

        if not tickers:
            return

        symbols = [t[0] for t in tickers]

        end_date   = datetime.now()
        start_date = end_date - relativedelta(years=4)

        data = yf.download(
            symbols,
            start       = start_date,
            auto_adjust = True,
            repair      = True,
            progress    = False,
        )

        start_col_index = ord(output_start_col) - ord("A")
        end_col_letter  = chr(start_col_index + 7 + ord("A"))

        updates = []

        for symbol, sheet_row in tickers:
            try:
                close_series = _extract_close(data, symbol, symbols)
                if close_series is None or close_series.empty:
                    raise ValueError(f"no data for {symbol}")
                current_price = ReturnCalculator.last_confirmed_close(close_series)
                returns       = ReturnCalculator.calculate(close_series, current_price)
            except Exception as e:
                print(f"  [WARN] {symbol} price fetch failed: {e}")
                returns = ["NA"] * 8

            returns = ReturnCalculator.clean(returns)

            updates.append({
                "range":  f"{output_start_col}{sheet_row}:{end_col_letter}{sheet_row}",
                "values": [returns],
            })

        self.sheet_client.batch_update(worksheet, updates)

        price_as_of, updated_at = _make_metadata(market)
        self.sheet_client.batch_update(worksheet, [
            {"range": "A1", "values": [[price_as_of]]},
            {"range": "A2", "values": [[updated_at]]},
        ])

        print(f"{sheet_name} updated OK\n")


# ======================================================
# ZERODHA DATA ENGINE
# ======================================================

class ZerodhaDataEngine:

    CACHE_FILE   = "nse_instruments.pkl"
    KITE_WORKERS = 8   # parallel historical_data calls

    def __init__(self, config: Config, sheet_client: GoogleSheetClient):
        self.config       = config
        self.sheet_client = sheet_client
        self.kite         = self._init_kite()
        self.index_token_map = self._load_instrument_cache()

    def _init_kite(self):
        kite = KiteConnect(api_key=self.config.ZERODHA_API_KEY)
        with open(self.config.ZERODHA_TOKEN_FILE, "r") as f:
            kite.set_access_token(f.read().strip())
        return kite

    def _load_instrument_cache(self):

        if os.path.exists(self.CACHE_FILE):
            with open(self.CACHE_FILE, "rb") as f:
                return pickle.load(f)

        instruments = self.kite.instruments("NSE")

        token_map = {
            inst["tradingsymbol"].upper(): inst["instrument_token"]
            for inst in instruments
            if inst["segment"] == "INDICES"
        }

        with open(self.CACHE_FILE, "wb") as f:
            pickle.dump(token_map, f)

        return token_map

    def _fetch_index_returns(self, ticker, start_date, end_date, open_col=None):
        """
        Fetch historical data for one index and return calculated returns.
        Designed to be called from a thread pool.
        Returns (returns_list,) — caller adds sheet_row from context.
        Kite historical_data("day") never returns a live intraday candle, so
        we override current_price with kite.ltp() when the India market is open.
        """
        token = self.index_token_map.get(ticker)
        if not token:
            return ["NA"] * 8
        try:
            candles = self.kite.historical_data(token, start_date, end_date, "day")
            if not candles:
                return ["NA"] * 8
            df = pd.DataFrame(candles)
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            close_series  = df["close"]
            current_price = ReturnCalculator.last_confirmed_close(close_series)
            open_series   = df["open"] if open_col else None

            # Override with live price when India market is open
            if _is_market_open("IN"):
                try:
                    ltp_key  = f"NSE:{ticker}"
                    ltp_data = self.kite.ltp(ltp_key)
                    ltp      = ltp_data.get(ltp_key, {}).get("last_price")
                    if ltp:
                        current_price = float(ltp)
                except Exception:
                    pass   # fall back to last historical close

            return ReturnCalculator.calculate(close_series, current_price, open_series)
        except Exception:
            return ["NA"] * 8

    def get_returns(self, zerodha_ticker: str) -> list:
        """Fetch returns for one index without writing to any sheet."""
        end_date   = datetime.now()
        start_date = end_date - relativedelta(years=4)
        return self._fetch_index_returns(zerodha_ticker, start_date, end_date)

    def update_nifty_indices(self):

        print("Updating NSE Indices...")

        worksheet  = self.sheet_client.get_worksheet("NIFTY Indices")
        ranges     = [("C4:C17", 4), ("C21:C27", 21)]
        index_rows = []

        for cell_range, start_row in ranges:
            rows        = worksheet.get(cell_range)
            row_pointer = start_row
            for r in rows:
                ticker = r[0].strip().upper() if r else ""
                if ticker:
                    index_rows.append((ticker, row_pointer))
                row_pointer += 1

        if not index_rows:
            return

        end_date   = datetime.now()
        start_date = end_date - relativedelta(years=4)

        # Parallel fetch across all indices
        updates = []
        with ThreadPoolExecutor(max_workers=self.KITE_WORKERS) as pool:
            future_to_row = {
                pool.submit(self._fetch_index_returns, ticker, start_date, end_date): sheet_row
                for ticker, sheet_row in index_rows
            }
            for future in as_completed(future_to_row):
                sheet_row = future_to_row[future]
                returns   = ReturnCalculator.clean(future.result())
                updates.append({
                    "range":  f"D{sheet_row}:K{sheet_row}",
                    "values": [returns],
                })

        self.sheet_client.batch_update(worksheet, updates)

        price_as_of, updated_at = _make_metadata("IN")
        self.sheet_client.batch_update(worksheet, [
            {"range": "A1", "values": [[price_as_of]]},
            {"range": "A2", "values": [[updated_at]]},
        ])

        print("NSE Indices updated OK\n")

    def update_nifty_sectors(self):

        print("Updating NIFTY Sectors...")

        worksheet   = self.sheet_client.get_worksheet("NIFTY Sectors")
        rows        = worksheet.get("C4:C17")
        sector_rows = []
        row_pointer = 4

        for r in rows:
            ticker = r[0].strip().upper() if r else ""
            if ticker:
                sector_rows.append((ticker, row_pointer))
            row_pointer += 1

        if not sector_rows:
            return

        end_date   = datetime.now()
        start_date = end_date - relativedelta(years=4)

        # Parallel fetch across all sectors (pass open_col=True to include open series)
        updates = []
        with ThreadPoolExecutor(max_workers=self.KITE_WORKERS) as pool:
            future_to_row = {
                pool.submit(self._fetch_index_returns, ticker, start_date, end_date, open_col=True): sheet_row
                for ticker, sheet_row in sector_rows
            }
            for future in as_completed(future_to_row):
                sheet_row = future_to_row[future]
                returns   = ReturnCalculator.clean(future.result())
                updates.append({
                    "range":  f"D{sheet_row}:K{sheet_row}",
                    "values": [returns],
                })

        self.sheet_client.batch_update(worksheet, updates)

        price_as_of, updated_at = _make_metadata("IN")
        self.sheet_client.batch_update(worksheet, [
            {"range": "A1", "values": [[price_as_of]]},
            {"range": "A2", "values": [[updated_at]]},
        ])

        print("NIFTY Sectors updated OK\n")


# ======================================================
# GLOBAL INDICES ENGINE
# ======================================================

class GlobalIndicesEngine:

    # ── Table 1: Major indices — hardcoded yfinance ticker map ──
    TABLE1_TICKER_MAP = {
        # Primary names
        "KOSPI":                "^KS11",
        "CAC 40 INDEX":         "^FCHI",
        "CAC 40":               "^FCHI",
        "FTSE 100":             "^FTSE",
        "DAX INDEX":            "^GDAXI",
        "DAX":                  "^GDAXI",
        "Nasdaq Composite":     "^IXIC",
        "Hang Seng":            "^HSI",
        "S&P 500":              "^GSPC",
        "ASX 200 INDEX":        "^AXJO",
        "ASX 200":              "^AXJO",
        "Dow Jones (DJIA)":     "^DJI",
        "Dow Jones":            "^DJI",
        "NIFTY 50":             "^NSEI",
        "SENSEX":               "^BSESN",
        "Nikkei 225":           "^N225",
        "Shanghai Composite":   "000001.SS",
    }

    # ── Table 2: Corrected ticker map keyed by index name ──────
    # Overrides whatever is in column C — all corrections applied here
    TABLE2_TICKER_OVERRIDES = {
        "TAIEX":                        "^TWII",
        "Jakarta Composite":            "^JKSE",
        "PSEi":                         "PSI20.LS",
        "BUX":                          None,               # delisted from yfinance
        "SET Index":                    "^SET.BK",
        "XU100 (BIST 100)":             "XU100.IS",
        "TA-35":                        "TA35.TA",
        "TA-125":                       "^TA125.TA",
        "ASX 50":                       "^AFLI",
        "BEL 20":                       "^BFX",
        "SMI":                          "^SSMI",
        "All Ordinaries":               "^AORD",
        "TSX Composite":                "^GSPTSE",
        "IBOV (IBOVESPA)":              "^BVSP",
        "KSE 100":                      None,               # delisted from yfinance
        "MEXBOL (IPC)":                 "^MXX",
        "NYSE Composite":               "^NYA",
        "NIFTY Bank":                   "^NSEBANK",
        "NIFTY Next 50":                "^NSMIDCP",
        "NASDAQ 100":                   "^NDX",
        "ATX":                          "^ATX",
        "NZX 50":                       "^NZ50",
        "WIG20":                        "WIG20.WA",
        "IBEX 35":                      "^IBEX",
        "FTSEMIB":                      "FTSEMIB.MI",
        "FTSE MIB":                     "FTSEMIB.MI",
        "OBX":                          "OSEBX.OL",
        "TASI":                         "^TASI.SR",
        "MOEX Russia":                  None,           # sanctioned/delisted — skip
        "STOXX Europe 600":             "^STOXX",
        "AMX":                          "^AMX",
        "SZSE Component":               "399001.SZ",
        "S&P MidCap 400":               "^SP400",
        "SX5E (Eurozone)":              "^STOXX50E",
        "SX5E / EURO STOXX 50":         "^STOXX50E",
        "EURO STOXX 50":                "^STOXX50E",
        "CSI 300":                      "000300.SS",
        "Russell 2000":                 "^RUT",
        "SDAX":                         "^SDAXI",
        "Wilshire 5000":                "^W5000",
        "KOSDAQ":                       None,               # delisted from yfinance
        "MDAX":                         "^MDAXI",
        "NIFTY FMCG":                   "^CNXFMCG",
        "Straits Times":                "^STI",
        "OMX Stockholm 30":             "^OMX",
        "AEX":                          "^AEX",
        "FTSE 350":                     "^FTLC",
        "TecDAX":                       "^TECDAX",
        "CAC Next 20":                  "^CN20",
        "OMX Helsinki 25":              "^OMXH25",
        "SA40 (JSE Top 40)":            "^J200.JO",
        "SA40 / JSE Top 40":            "^J200.JO",
        "KLCI":                         "^KLSE",
        "Hang Seng Tech":               "3033.HK",
        "Hang Seng China Enterprises":  "^HSCE",
        "COLCAP":                       None,               # delisted from yfinance
        "mWIG40":                       "mWIG40.WA",
        "OMX Copenhagen 25":            "^OMXC25",
        "ISEQ 20":                      "^ISEQ",
        "FTSE Developed Markets":       None,           # no yfinance equivalent — skip
        "FTSE All World":               "^VXUS",
        "Dow Jones Global Titans 50":   "^DJGT",
    }

    def __init__(self, sheet_client: GoogleSheetClient):
        self.sheet_client = sheet_client

    # ── Shared bulk fetch helper ───────────────────────────────
    def _fetch_data(self, symbols):
        """
        Download 4Y of daily closes and live prices for a list
        of yfinance symbols. Returns (price_data, current_prices).
        """
        end_date   = datetime.now()
        start_date = end_date - relativedelta(years=4)

        price_data = yf.download(
            symbols,
            start       = start_date,
            auto_adjust = True,
            repair      = True,
            progress    = False,
        )

        last_closes = {}
        for sym in symbols:
            try:
                s = _extract_close(price_data, sym, symbols)
                last_closes[sym] = ReturnCalculator.last_confirmed_close(s) if s is not None else None
            except Exception:
                last_closes[sym] = None

        return price_data, last_closes

    # ── Collect ticker rows from each table ───────────────────

    @staticmethod
    def _is_valid_index_name(name: str) -> bool:
        """Guard against junk rows — reject sentences masquerading as index names."""
        return bool(name) and len(name) <= 60 and name.count(" ") <= 6

    def _collect_table1_tickers(self, worksheet):
        rows        = worksheet.get("B5:B17")
        ticker_rows = []
        row_pointer = 5
        for r in rows:
            name = r[0].strip() if r else ""
            if name and self._is_valid_index_name(name):
                ticker = self.TABLE1_TICKER_MAP.get(name)
                if ticker:
                    ticker_rows.append((ticker, row_pointer))
                else:
                    print(f"  Skipping unmapped index: {name}")
            elif name:
                print(f"  Skipping junk row: {name[:60]!r}")
            row_pointer += 1
        return ticker_rows

    def _collect_table2_tickers(self, worksheet):
        rows        = worksheet.get("B23:B80")
        ticker_rows = []
        row_pointer = 23
        for r in rows:
            name = r[0].strip() if r else ""
            if name and self._is_valid_index_name(name):
                ticker = self.TABLE2_TICKER_OVERRIDES.get(name)
                if ticker is None:
                    if name in self.TABLE2_TICKER_OVERRIDES:
                        print(f"  Skipping {name} (no yfinance equivalent)")
                    else:
                        print(f"  Skipping unmapped index: {name}")
                else:
                    ticker_rows.append((ticker, row_pointer))
            elif name:
                print(f"  Skipping junk row: {name[:50]!r}")
            row_pointer += 1
        return ticker_rows

    # ── Build update dicts from pre-fetched data ──────────────

    def _build_updates(self, ticker_rows, price_data, live_prices, all_symbols, range_fn, overrides=None):
        overrides = overrides or {}
        updates   = []
        for ticker, sheet_row in ticker_rows:
            if ticker in overrides:
                # Use pre-fetched returns from another engine (e.g. Zerodha for NIFTY 50)
                returns = ReturnCalculator.clean(overrides[ticker]) + ["NA"]
            else:
                try:
                    # Pass all_symbols (the full download list) so _extract_close
                    # correctly identifies single- vs multi-ticker DataFrames.
                    close_series  = _extract_close(price_data, ticker, all_symbols)
                    if close_series is None or close_series.empty:
                        raise ValueError(f"no data for {ticker}")
                    current_price = live_prices.get(ticker)
                    returns       = ReturnCalculator.calculate(close_series, current_price)
                except Exception as e:
                    print(f"  [WARN] {ticker}: {e}")
                    returns = ["NA"] * 8
                returns = ReturnCalculator.clean(returns) + ["NA"]   # pad 5Y column
            updates.append({"range": range_fn(sheet_row), "values": [returns]})
        return updates

    # ── Single entry point ────────────────────────────────────

    def update_global_indices(self, overrides=None):
        print("Updating Global Indices...")
        worksheet = self.sheet_client.get_worksheet("Global Indices")

        t1_rows = self._collect_table1_tickers(worksheet)
        t2_rows = self._collect_table2_tickers(worksheet)

        # Merge all symbols → single yf.download() instead of two
        all_symbols = list({t[0] for t in t1_rows + t2_rows})
        if not all_symbols:
            print("  No valid tickers found.")
            return

        price_data, live_prices = self._fetch_data(all_symbols)

        t1_updates = self._build_updates(t1_rows, price_data, live_prices, all_symbols, lambda r: f"D{r}:L{r}", overrides)
        t2_updates = self._build_updates(t2_rows, price_data, live_prices, all_symbols, lambda r: f"D{r}:L{r}", overrides)

        self.sheet_client.batch_update(worksheet, t1_updates + t2_updates)

        price_as_of, updated_at = _make_metadata("GLOBAL")
        self.sheet_client.batch_update(worksheet, [
            {"range": "A1", "values": [[price_as_of]]},
            {"range": "A2", "values": [[updated_at]]},
        ])

        print(f"  Table 1 updated — {len(t1_updates)} indices OK")
        print(f"  Table 2 updated — {len(t2_updates)} indices OK")
        print("Global Indices updated OK\n")


# ======================================================
# ETFDB SCRAPER ENGINE
# ======================================================

class ETFdbEngine:
    """
    Scrapes ETF metadata (ticker, name, AUM) from the ETFdb screener API,
    then enriches each ETF with price / return data from yfinance.

    Sheet layout written by this engine:
        "Biggest Leveraged Funds "  → A=Ticker  B=Name  C=AUM  F:L=Price+Returns
        "Biggest Leveraged Funds(Com.)" → A=Ticker  B=Name  C=AUM  G:M=Price+Returns
        "ETFs US"                   → B=Ticker  C=Name  D=AUM  F:L=Price+Returns
        "Commodity ETFs"            → B=Ticker  C=Name  D=AUM  F:L=Price+Returns

    ETFs India and Crypto are kept on the legacy YahooDataEngine (ETFdb
    does not cover Indian exchange-listed ETFs or spot crypto tickers).
    """

    SCREENER_URL = "https://etfdb.com/api/screener/"

    # Minimal headers — matching what the etfdb-api JS library sends.
    # Extra browser-simulation headers (Sec-Fetch-*, Origin etc.) actually
    # trigger ETFdb's bot detection. Keep it simple.
    HEADERS = {
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }

    # Per-sheet configuration
    # Keys: sheet_name → (etfdb_type, n, start_row, ticker_col, name_col, aum_col, returns_col)
    SHEET_CONFIGS = {
        "Biggest Leveraged Funds ": {
            "etfdb_type":  "Leveraged ETFs",
            "n":           116,
            "start_row":   7,
            "ticker_col":  "A",
            "name_col":    "B",
            "aum_col":     "C",
            "returns_col": "F",   # unchanged from existing layout
        },
        "Biggest Leveraged Funds(Com.)": {
            # Commodity-focused leveraged ETFs — use Leveraged ETFs type;
            # adjust etfdb_type to "Commodity ETFs" if you want non-leveraged.
            "etfdb_type":  "Leveraged ETFs",
            "n":           7,
            "start_row":   5,
            "ticker_col":  "A",
            "name_col":    "B",
            "aum_col":     "C",
            "returns_col": "G",   # unchanged from existing layout
        },
        "ETFs US": {
            "etfdb_type":  None,   # no type filter = all ETFs sorted by AUM
            "n":           200,
            "start_row":   3,
            "ticker_col":  "B",
            "name_col":    "C",
            "aum_col":     "D",
            "returns_col": "F",   # unchanged from existing layout
        },
        "Commodity ETFs": {
            "etfdb_type":  "Commodity ETFs",
            "n":           10,
            "start_row":   4,
            "ticker_col":  "B",
            "name_col":    "C",
            "aum_col":     "D",
            "returns_col": "F",   # unchanged from existing layout
        },
    }

    def __init__(self, sheet_client: GoogleSheetClient):
        self.sheet_client = sheet_client

    # ── ETFdb scrape ──────────────────────────────────────────

    def _scrape(self, etf_type, n) -> list:
        """
        POST to ETFdb screener API and return a list of dicts:
            [{"ticker": "TQQQ", "name": "ProShares UltraPro QQQ", "aum": "$21,300M"}, ...]
        Sorted by AUM descending, limited to n results.

        Payload matches the etfdb-api JS library exactly:
          - only: ["meta", "data"] is required
          - Minimal headers only — extra browser headers trigger bot detection
        """
        payload = {
            "page":           1,
            "per_page":       n,
            "sort_by":        "assets",
            "sort_direction": "desc",
            "only":           ["meta", "data"],
        }
        if etf_type:
            payload["type"] = etf_type

        resp = requests.post(
            self.SCREENER_URL,
            json    = payload,
            headers = self.HEADERS,
            timeout = 30,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = []
        for item in data.get("data", []):
            sym    = item.get("symbol", "")
            ticker = sym.get("text", "") if isinstance(sym, dict) else str(sym)

            nm   = item.get("name", "")
            name = nm.get("text", "") if isinstance(nm, dict) else str(nm)

            aum = item.get("assets", "")

            if ticker:
                rows.append({"ticker": ticker.strip(), "name": name.strip(), "aum": aum})

        return rows

    # ── Sheet update ──────────────────────────────────────────

    def _update_sheet(self, sheet_name: str, cfg: dict):
        print(f"Updating {sheet_name} (ETFdb)...")

        ws        = self.sheet_client.get_worksheet(sheet_name)
        start_row = cfg["start_row"]
        tc, nc, ac, rc = cfg["ticker_col"], cfg["name_col"], cfg["aum_col"], cfg["returns_col"]

        # 1. Scrape ETFdb for ticker/name/AUM ─────────────────
        # If ETFdb blocks (403) fall back to tickers already in the sheet
        # so yfinance price/return updates still run normally.
        etfs         = []
        etfdb_ok     = False
        scrape_error = None
        try:
            etfs     = self._scrape(cfg["etfdb_type"], cfg["n"])
            etfdb_ok = bool(etfs)
        except Exception as e:
            scrape_error = e

        if not etfdb_ok:
            # Fall back: read existing tickers + names from sheet
            print(f"  [WARN] ETFdb unavailable ({scrape_error or 'no results'}) "
                  f"— using existing tickers from sheet for price update.")
            end_row    = start_row + cfg["n"] - 1
            sheet_rows = ws.get(f"{tc}{start_row}:{nc}{end_row}")
            for r in sheet_rows:
                ticker = r[0].strip() if len(r) > 0 else ""
                name   = r[1].strip() if len(r) > 1 else ""
                if ticker:
                    etfs.append({"ticker": ticker, "name": name, "aum": ""})
            if not etfs:
                print(f"  [SKIP] No tickers in sheet either — skipping {sheet_name}.")
                return
        else:
            # ETFdb worked — write fresh ticker/name/AUM to sheet
            meta_values  = [[e["ticker"], e["name"], e["aum"]] for e in etfs]
            blank        = ["", "", ""]
            meta_values += [blank] * max(0, cfg["n"] + 10 - len(meta_values))
            end_row      = start_row + len(meta_values) - 1
            self.sheet_client.batch_update(ws, [{
                "range":  f"{tc}{start_row}:{ac}{end_row}",
                "values": meta_values,
            }])

        # 3. Fetch price + returns from yfinance ──────────────
        symbols    = [e["ticker"] for e in etfs]
        end_date   = datetime.now()
        start_date = end_date - relativedelta(years=4)

        data = yf.download(
            symbols,
            start       = start_date,
            auto_adjust = True,
            repair      = True,
            progress    = False,
        )

        start_col_idx  = ord(rc) - ord("A")
        end_col_letter = chr(start_col_idx + 7 + ord("A"))

        # Normalise DataFrame to {ticker: close_series} so the loop is uniform.
        price_updates = []
        for i, etf in enumerate(etfs):
            row = start_row + i
            try:
                close_series = _extract_close(data, etf["ticker"], symbols)
                if close_series is None or close_series.empty:
                    raise ValueError(f"no data for {etf['ticker']}")
                current_price = ReturnCalculator.last_confirmed_close(close_series)
                returns       = ReturnCalculator.calculate(close_series, current_price)
            except Exception as e:
                print(f"  [WARN] {etf['ticker']} price fetch failed: {e}")
                returns = ["NA"] * 8
            price_updates.append({
                "range":  f"{rc}{row}:{end_col_letter}{row}",
                "values": [ReturnCalculator.clean(returns)],
            })

        self.sheet_client.batch_update(ws, price_updates)

        price_as_of, updated_at = _make_metadata("US")
        self.sheet_client.batch_update(ws, [
            {"range": "A1", "values": [[price_as_of]]},
            {"range": "A2", "values": [[updated_at]]},
        ])

        print(f"{sheet_name} (ETFdb) updated OK\n")

    # ── Public entry point ────────────────────────────────────

    def update_all(self):
        """Update all ETFdb-backed sheets sequentially."""
        for sheet_name, cfg in self.SHEET_CONFIGS.items():
            try:
                self._update_sheet(sheet_name, cfg)
            except Exception as e:
                print(f"  [ERROR] {sheet_name} ETFdb update failed: {e}")


# ======================================================
# MUTUAL FUNDS INDIA ENGINE
# ======================================================

class MutualFundsEngine:

    SCHEME_CACHE_FILE = "mf_schemes.pkl"
    NAV_CACHE_DIR     = "nav_cache"
    NAV_URL           = "https://api.mfapi.in/mf/{}"
    NAV_WORKERS       = 15   # mfapi.in handles ~15 concurrent connections well
    NAV_RETRIES       = 2    # retry once on timeout before giving up

    def __init__(self, sheet_client: GoogleSheetClient):
        self.sheet_client = sheet_client
        self.schemes      = self._load_schemes()
        self._session     = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=self.NAV_WORKERS, pool_maxsize=self.NAV_WORKERS)
        self._session.mount("https://", adapter)
        os.makedirs(self.NAV_CACHE_DIR, exist_ok=True)

    def _load_schemes(self):
        """Download full scheme list from mfapi.in and cache it locally."""
        if os.path.exists(self.SCHEME_CACHE_FILE):
            with open(self.SCHEME_CACHE_FILE, "rb") as f:
                return pickle.load(f)

        print("  Downloading MF scheme list (one-time)...")
        resp = requests.get("https://api.mfapi.in/mf", timeout=30)
        resp.raise_for_status()
        schemes = resp.json()

        with open(self.SCHEME_CACHE_FILE, "wb") as f:
            pickle.dump(schemes, f)

        return schemes

    def _find_scheme_code(self, fund_name):
        """Match fund name to an AMFI scheme code using local word matching."""
        search = fund_name
        for suffix in [" Direct \u2013 Growth", " Direct - Growth",
                       " \u2013 Growth",         " - Growth"]:
            if search.endswith(suffix):
                search = search[:-len(suffix)].strip()
                break
        search = search.replace("\u2013", "-").lower()

        # Only consider Direct + Growth schemes
        candidates = [
            s for s in self.schemes
            if "direct" in s["schemeName"].lower()
            and "growth" in s["schemeName"].lower()
        ]

        # 1. Word match: every word in search must appear in scheme name
        words = search.split()
        for s in candidates:
            name = s["schemeName"].lower()
            if all(w in name for w in words):
                return s["schemeCode"]

        # 2. Fuzzy fallback
        names   = [s["schemeName"].lower() for s in candidates]
        matches = difflib.get_close_matches(search, names, n=1, cutoff=0.5)
        if matches:
            return candidates[names.index(matches[0])]["schemeCode"]

        return None

    def _nav_cache_path(self, scheme_code):
        return os.path.join(self.NAV_CACHE_DIR, f"{scheme_code}.pkl")

    def _load_nav_cache(self, scheme_code):
        path = self._nav_cache_path(scheme_code)
        if os.path.exists(path):
            with open(path, "rb") as f:
                return pickle.load(f)
        return None

    def _save_nav_cache(self, scheme_code, series):
        with open(self._nav_cache_path(scheme_code), "wb") as f:
            pickle.dump(series, f)

    def _fetch_nav_series(self, scheme_code):
        for attempt in range(self.NAV_RETRIES):
            try:
                resp = self._session.get(self.NAV_URL.format(scheme_code), timeout=20)
                resp.raise_for_status()
                data = resp.json().get("data", [])
                if not data:
                    return None
                dates  = pd.to_datetime([d["date"] for d in data], format="%d-%m-%Y")
                navs   = pd.to_numeric([d["nav"]  for d in data], errors="coerce")
                series = pd.Series(navs, index=dates).sort_index()
                self._save_nav_cache(scheme_code, series)
                return series
            except (requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
                is_server_error = isinstance(e, requests.exceptions.HTTPError) and e.response is not None and e.response.status_code >= 500
                if (isinstance(e, requests.exceptions.Timeout) or is_server_error) and attempt < self.NAV_RETRIES - 1:
                    time.sleep(2)
                else:
                    cached = self._load_nav_cache(scheme_code)
                    if cached is not None:
                        print(f"  [CACHE]     using cached NAV for {scheme_code}")
                        return cached
                    raise

    def _fetch_fund_data(self, fund_name):
        try:
            scheme_code = self._find_scheme_code(fund_name)
            if not scheme_code:
                print(f"  [NO MATCH]  {fund_name}")
                return fund_name, None
            series = self._fetch_nav_series(scheme_code)
            if series is None or series.empty:
                print(f"  [EMPTY NAV] {fund_name} (code={scheme_code})")
                return fund_name, None
            return fund_name, series
        except Exception as e:
            print(f"  [ERROR]     {fund_name}: {type(e).__name__}: {e}")
            return fund_name, None

    def update_mutual_funds(self):
        print("Updating Mutual Funds India...")

        worksheet = self.sheet_client.get_worksheet("Mutual Funds India")

        rows        = worksheet.get("B3:B67")
        fund_rows   = []
        row_pointer = 3

        for r in rows:
            name = r[0].strip() if r else ""
            if name:
                fund_rows.append((name, row_pointer))
            row_pointer += 1

        if not fund_rows:
            return

        nav_map = {}
        with ThreadPoolExecutor(max_workers=self.NAV_WORKERS) as executor:
            for name, series in executor.map(self._fetch_fund_data, [f[0] for f in fund_rows]):
                nav_map[name] = series

        updates = []

        for fund_name, sheet_row in fund_rows:
            nav_series = nav_map.get(fund_name)

            if nav_series is None or nav_series.empty:
                returns = ["NA"] * 5
            else:
                current_nav = float(nav_series.iloc[-1])
                all_returns = ReturnCalculator.calculate(nav_series, current_nav)
                # all_returns = [price, 1D, 1W, 1M, 3M, 6M, 1Y, 3Y] — drop price, 1D, 1W
                returns = all_returns[3:]

            returns = ReturnCalculator.clean(returns)

            updates.append({
                "range":  f"C{sheet_row}:G{sheet_row}",
                "values": [returns],
            })

        self.sheet_client.batch_update(worksheet, updates)

        price_as_of, updated_at = _make_metadata("NAV")
        self.sheet_client.batch_update(worksheet, [
            {"range": "A1", "values": [[price_as_of]]},
            {"range": "A2", "values": [[updated_at]]},
        ])

        print("Mutual Funds India updated OK\n")


# ======================================================
# MAIN ORCHESTRATOR
# ======================================================

class MarketUpdater:

    # Zerodha ticker → yfinance symbol for indices that appear in both
    # Global Indices (yfinance) and NIFTY tabs (Zerodha).
    # Zerodha is the authoritative source for these — its returns are injected
    # into Global Indices so both tabs always show identical numbers.
    _ZERODHA_GLOBAL_OVERRIDES = {
        "NIFTY 50": "^NSEI",
    }

    def __init__(self):
        self.config        = Config()
        self.sheet_client  = GoogleSheetClient(self.config)
        self.yahoo         = YahooDataEngine(self.sheet_client)
        self.etfdb         = ETFdbEngine(self.sheet_client)
        self.zerodha       = ZerodhaDataEngine(self.config, self.sheet_client)
        self.global_indices = GlobalIndicesEngine(self.sheet_client)
        self.mutual_funds   = MutualFundsEngine(self.sheet_client)

    def _global_indices_overrides(self) -> dict:
        """Fetch returns from Zerodha for indices shared with NIFTY tabs."""
        overrides = {}
        for zerodha_ticker, yf_symbol in self._ZERODHA_GLOBAL_OVERRIDES.items():
            try:
                returns = self.zerodha.get_returns(zerodha_ticker)
                if returns and returns != ["NA"] * 8:
                    overrides[yf_symbol] = returns
                    print(f"  Global Indices override: {zerodha_ticker} → {yf_symbol} (Zerodha)")
            except Exception as e:
                print(f"  [WARN] Zerodha override failed for {zerodha_ticker}: {e}")
        return overrides

    @property
    def _sheet_map(self):
        return {
            "ETFs India":                   lambda: self.yahoo.update_sheet("ETFs India", "C7:C100", 7, "E", market="IN"),
            "Crypto":                        lambda: self.yahoo.update_sheet("Crypto", "B104:B118", 104, "D", market="CRYPTO"),
            "Global Indices":               lambda: self.global_indices.update_global_indices(self._global_indices_overrides()),
            "Mutual Funds":                 self.mutual_funds.update_mutual_funds,
            "NIFTY Sectors":                self.zerodha.update_nifty_sectors,
            "NIFTY Indices":                self.zerodha.update_nifty_indices,
            **{
                name: (lambda n, c: lambda: self.etfdb._update_sheet(n, c))(name, cfg)
                for name, cfg in ETFdbEngine.SHEET_CONFIGS.items()
            },
        }

    def run_single(self, sheet_name: str):
        sheet_map = self._sheet_map
        if sheet_name not in sheet_map:
            print(f"Unknown sheet '{sheet_name}'. Available sheets:")
            for name in sheet_map:
                print(f"  - {name}")
            return
        print(f"\n===== UPDATING: {sheet_name} =====\n")
        sheet_map[sheet_name]()
        print(f"===== {sheet_name} COMPLETE OK\n")

    def run(self):

        print("\n===== RUNNING MARKET DATABASE UPDATE =====\n")

        # yfinance is not thread-safe when multiple yf.download() calls run
        # concurrently — it uses shared internal state that causes
        # "dictionary changed size during iteration" errors.
        # Fix: run all Yahoo sheets sequentially in one worker thread while
        # the other independent engines (MF, global indices, Zerodha) run
        # in parallel alongside them.

        def run_yfinance_sequential():
            """
            All yfinance calls run in a single sequential block.
            yfinance uses shared internal state that causes
            'dictionary changed size during iteration' errors when
            multiple yf.download() calls run concurrently.
            """
            for name, fn in [
                ("ETFs India",     lambda: self.yahoo.update_sheet("ETFs India", "C7:C100", 7, "E")),
                ("Crypto",         lambda: self.yahoo.update_sheet("Crypto", "B104:B118", 104, "D")),
                ("Global Indices", self.global_indices.update_global_indices),
                ("ETFdb Sheets",   self.etfdb.update_all),
            ]:
                try:
                    fn()
                except Exception as e:
                    print(f"  [ERROR] {name} failed: {e}")

        # Zerodha (Kite API) and Mutual Funds (mfapi.in) are fully independent
        # of yfinance and can run concurrently alongside the yfinance block.
        tasks = [
            ("yfinance + ETFdb", run_yfinance_sequential),
            ("Mutual Funds",     self.mutual_funds.update_mutual_funds),
            ("NIFTY Sectors",    self.zerodha.update_nifty_sectors),
            ("NIFTY Indices",    self.zerodha.update_nifty_indices),
        ]

        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            future_to_name = {pool.submit(fn): name for name, fn in tasks}
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"  [ERROR] {name} failed: {e}")

        print("===== ALL UPDATES COMPLETE OK\n")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    import sys
    updater = MarketUpdater()
    if len(sys.argv) > 1:
        updater.run_single(" ".join(sys.argv[1:]))
    else:
        updater.run()