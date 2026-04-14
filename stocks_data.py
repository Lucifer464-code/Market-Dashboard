"""
stocks_data.py
==============
Standalone script that updates four Google Sheets:

  1. "Top G&L US"    — Top 15 gainers + losers (US, $2Bn+ market cap)
  2. "Top G&L India" — Top 15 gainers + losers (India, Rs1000Cr+ market cap)
  3. "ATH US"        — Stocks within 1% of all-time high (US, $2Bn+)
  4. "ATH India"     — Stocks within 1% of all-time high (India, Rs1000Cr+)

Universes:
  US    : iShares Russell 3000 ETF holdings CSV (~3000 tickers)
  India : NSE NIFTY Total Market CSV (~1800+ tickers)

Name cache:
  One-time fetch of shortName per ticker via yfinance, saved to
  ticker_names.csv. Only new tickers not in the file are ever fetched.

Usage:
  python stocks_data.py                # full run (G&L + ATH)
  python stocks_data.py --gl-only      # gainers/losers only
  python stocks_data.py --ath-only     # ATH only
  python stocks_data.py --names-only   # rebuild name cache, no sheet updates

G&L sheet layout ("Top G&L US" / "Top G&L India"):
    Row 3      : Gainers label (Col A-D) | Losers label (Col F-I)
    Row 4      : Gainers headers         | Losers headers
    Rows 5-19  : Top 15 gainers          | Top 15 losers (same rows, Col F-I)

ATH sheet layout ("ATH US" / "ATH India"):
    Row 3      : Section label
    Row 4      : Column headers
    Rows 5+    : All qualifying stocks (sorted by 1W% desc)
    Cols A-L   : Ticker | Name | Market Cap | ATH | ATH% |
                 Price | 1W% | 1M% | 3M% | 6M% | 1Y% | 3Y%
"""

import logging
import yfinance as yf
import gspread
import pandas as pd
import numpy as np
import os
import pickle
import requests
import time
import io
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

# Suppress noisy yfinance warnings (delisted tickers, no data found, etc.)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials


# ======================================================
# CONFIGURATION
# ======================================================

class Config:
    SHEET_ID     = "1uJoD2JRvzRpn2KHJa80aZADQ2DfRwm2qbZKMuv0PKBM"
    SERVICE_FILE = "service_account.json"


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
        self.client   = gspread.authorize(creds)
        self.sheet_id = config.SHEET_ID

    def get_worksheet(self, name):
        return self.client.open_by_key(self.sheet_id).worksheet(name)

    def batch_update(self, worksheet, updates):
        if updates:
            worksheet.batch_update(updates)

    def apply_formats(self, worksheet, requests):
        """Send raw Sheets API batchUpdate requests for cell formatting."""
        if requests:
            worksheet.spreadsheet.batch_update({"requests": requests})


# ======================================================
# STOCKS DATA ENGINE
# ======================================================

class StocksDataEngine:
    """
    Fetches price history for the full Russell 3000 + NIFTY Total Market universe,
    then derives:
      - Top 15 gainers / losers by 1W return (market cap filtered)
      - Stocks within 1% of their all-time high (market cap filtered)

    All data written to Google Sheets.
    """

    TOP_N         = 15
    ATH_THRESHOLD = 0.05    # within 5% of all-time high

    # ── Data sources ──────────────────────────────────────────
    RUSSELL_3000_URL = (
        "https://www.ishares.com/us/products/239714/"
        "ishares-russell-3000-etf/1467271812596.ajax"
        "?fileType=csv&fileName=IWV_holdings&dataType=fund"
    )
    NIFTY_TOTAL_MARKET_URL = (
        "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"
    )

    # ── Market cap floors ─────────────────────────────────────
    US_MCAP_FLOOR = 2_000_000_000    # $2 Bn in USD
    IN_MCAP_FLOOR = 5_000_000_000    # Rs500 Cr in INR

    # ── Universe cache (pickle, 24h TTL) ──────────────────────
    US_CACHE_FILE   = "russell3000_tickers.pkl"
    IN_CACHE_FILE   = "nifty_total_market_tickers.pkl"
    CACHE_TTL_HOURS = 24

    # ── Name cache (CSV, permanent) ───────────────────────────
    NAME_CACHE_FILE = "ticker_names.csv"

    # ── Batch settings ────────────────────────────────────────
    PRICE_BATCH_SIZE = 500   # larger batches = fewer round trips
    MCAP_WORKERS     = 12    # parallel threads for market cap prefetch (too high triggers 401)

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }

    def __init__(self, sheet_client: GoogleSheetClient):
        self.sheet_client = sheet_client

    # ── Universe cache (pickle) ───────────────────────────────

    def _load_pkl_cache(self, path):
        if not os.path.exists(path):
            return None
        if (time.time() - os.path.getmtime(path)) / 3600 > self.CACHE_TTL_HOURS:
            return None
        with open(path, "rb") as f:
            return pickle.load(f)

    def _save_pkl_cache(self, path, data):
        with open(path, "wb") as f:
            pickle.dump(data, f)

    # ── Name cache (CSV) ──────────────────────────────────────

    def _load_name_cache(self) -> dict:
        if not os.path.exists(self.NAME_CACHE_FILE):
            return {}
        try:
            df = pd.read_csv(self.NAME_CACHE_FILE, dtype=str)
            df.columns = df.columns.str.strip()
            df["Ticker"] = df["Ticker"].str.strip()
            df["Name"]   = df["Name"].str.strip()
            # Drop bad entries where Name == Ticker — these are unfilled
            # placeholders from a previous run before name seeding worked.
            # They will be re-seeded with real names on this run.
            df = df[df["Name"] != df["Ticker"]]
            # Deduplicate — keep last entry per ticker (most recent wins)
            df = df.drop_duplicates(subset=["Ticker"], keep="last")
            return dict(zip(df["Ticker"], df["Name"]))
        except Exception as e:
            print(f"  [WARN] Could not load name cache: {e}")
            return {}

    def _save_name_cache(self, name_map: dict):
        df = pd.DataFrame(sorted(name_map.items()), columns=["Ticker", "Name"])
        df.to_csv(self.NAME_CACHE_FILE, index=False)

    # ── Universe: Russell 3000 ────────────────────────────────

    def _fetch_russell3000(self) -> tuple:
        """
        Parse iShares Russell 3000 CSV.
        Returns (tickers, name_map) where names come directly from the
        CSV 'Name' column — no yfinance lookup needed for US stocks.
        Cache stores (tickers, name_map) tuple.
        """
        cached = self._load_pkl_cache(self.US_CACHE_FILE)
        if cached:
            # Handle old cache format (list only) gracefully
            if isinstance(cached, tuple):
                tickers, name_map = cached
                # Normalize to title case in case cache was built before this fix
                name_map = {k: v.title() for k, v in name_map.items()}
            else:
                tickers  = cached
                name_map = {}
            print(f"  Russell 3000: using cache ({len(tickers)} tickers)")
            return tickers, name_map

        print("  Downloading Russell 3000 from iShares...")
        try:
            r = requests.get(self.RUSSELL_3000_URL, headers=self.HEADERS, timeout=30)
            r.raise_for_status()

            lines      = r.text.splitlines()
            header_idx = next(
                (i for i, line in enumerate(lines) if line.startswith("Ticker,")), None
            )
            if header_idx is None:
                print("  [ERROR] Header row not found in iShares CSV.")
                return [], {}

            df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
            df.columns = df.columns.str.strip()
            df = df[df["Asset Class"].str.strip().str.lower() == "equity"]

            # Clean tickers
            df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
            df = df[
                df["Ticker"].apply(
                    lambda t: bool(t and t != "-" and t.isalpha() and len(t) <= 5)
                )
            ]

            # Build name map from CSV Name column directly
            name_map = dict(zip(
                df["Ticker"],
                df["Name"].astype(str).str.strip().str.title()
            ))

            tickers = df["Ticker"].tolist()

            print(f"  Russell 3000: {len(tickers)} tickers, {len(name_map)} names loaded from CSV")
            self._save_pkl_cache(self.US_CACHE_FILE, (tickers, name_map))
            return tickers, name_map

        except Exception as e:
            print(f"  [ERROR] Russell 3000 fetch failed: {e}")
            return [], {}

    # ── Universe: NIFTY 500 ───────────────────────────────────

    def _fetch_nifty_total_market(self) -> tuple:
        """
        Parse NSE NIFTY Total Market CSV (~1800+ stocks).
        Returns (tickers, name_map) where:
            tickers  : list of yfinance symbols e.g. ["RELIANCE.NS", ...]
            name_map : dict of display ticker -> company name
                       e.g. {"RELIANCE": "Reliance Industries Ltd."}
        Company names come directly from the CSV "Company Name" column —
        no yfinance lookup needed for Indian stocks.

        FIX (ATH India tickers shown instead of names):
        Old pickle cache may be in list-only format with no name_map.
        We now force-invalidate any cache that doesn't contain a non-empty
        name_map so names are always freshly seeded from the CSV.
        """
        cached = self._load_pkl_cache(self.IN_CACHE_FILE)
        if cached:
            if isinstance(cached, tuple):
                tickers, name_map = cached
                # Invalidate cache if name_map is empty — forces re-fetch
                if name_map:
                    print(f"  NIFTY Total Market: using cache ({len(tickers)} tickers)")
                    return tickers, name_map
                else:
                    print("  NIFTY Total Market: cache has no names — re-fetching from NSE...")
            else:
                print("  NIFTY Total Market: old cache format — re-fetching from NSE...")

        print("  Downloading NIFTY Total Market from NSE...")
        try:
            headers = {**self.HEADERS, "Referer": "https://www.nseindia.com/"}
            r = requests.get(self.NIFTY_TOTAL_MARKET_URL, headers=headers, timeout=20)
            r.raise_for_status()

            df = pd.read_csv(io.StringIO(r.text))
            df.columns = df.columns.str.strip()

            if "Series" in df.columns:
                df = df[df["Series"].str.strip() == "EQ"]

            df["Symbol"] = (
                df["Symbol"].dropna()
                .astype(str).str.strip().str.upper()
            )

            # Build name map from CSV directly — confirmed column: "Company Name"
            name_map = dict(zip(
                df["Symbol"],
                df["Company Name"].astype(str).str.strip()
            ))

            tickers = (df["Symbol"] + ".NS").tolist()

            print(f"  NIFTY Total Market: {len(tickers)} tickers, {len(name_map)} names loaded from CSV")
            self._save_pkl_cache(self.IN_CACHE_FILE, (tickers, name_map))
            return tickers, name_map

        except Exception as e:
            print(f"  [ERROR] NIFTY Total Market fetch failed: {e}")
            return [], {}

    # ── Market cap prefetch ───────────────────────────────────

    def _fetch_market_caps(self, tickers: list) -> tuple:
        """
        Fetch market_cap and last_price for all tickers in parallel using fast_info.
        Both values come from the same single fast_info call per ticker, so there is
        no extra cost vs. fetching market cap alone.

        Returns (mcap_map, live_prices):
            mcap_map    : {ticker: market_cap_float} — only tickers with valid caps
            live_prices : {ticker: last_price_float} — used instead of intraday download
        """
        print(f"  Fetching market caps for {len(tickers)} tickers (parallel)...")

        def _get(symbol):
            for attempt in range(3):
                try:
                    fi         = yf.Ticker(symbol).fast_info
                    mcap       = fi.market_cap
                    last_price = getattr(fi, "last_price", None)
                    return (
                        symbol,
                        float(mcap)       if mcap       else None,
                        float(last_price) if last_price else None,
                    )
                except Exception:
                    if attempt < 2:
                        time.sleep(1 + attempt)
            return symbol, None, None

        mcap_map    = {}
        live_prices = {}
        with ThreadPoolExecutor(max_workers=self.MCAP_WORKERS) as pool:
            for symbol, mcap, last_price in pool.map(_get, tickers):
                if mcap:
                    mcap_map[symbol] = mcap
                if last_price:
                    live_prices[symbol] = last_price

        print(f"  Market caps: {len(mcap_map)}/{len(tickers)} tickers resolved")
        return mcap_map, live_prices

    # ── Market hours check ────────────────────────────────────

    @staticmethod
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

    # ── Price history fetch ───────────────────────────────────

    def _fetch_price_history_ath(self, tickers: list, mcap_map: dict, live_prices: dict, market: str = "US") -> tuple:
        """
        Download 4Y of daily history for the ATH pipeline.

        live_prices is the dict returned by _fetch_market_caps — used as the
        current price when the market is open, replacing the old intraday
        yf.download() call that doubled the number of network requests.

        Returns (df, price_as_of) where df has columns:
            Ticker | MarketCap | Price | ATH | PctFromATH |
            Change1D | Change1W | Change1M | Change3M | Change6M | Change1Y | Change3Y
        """
        all_rows    = []
        last_date   = None
        market_open = self._is_market_open(market)
        batches     = [
            tickers[i:i + self.PRICE_BATCH_SIZE]
            for i in range(0, len(tickers), self.PRICE_BATCH_SIZE)
        ]
        total_b = len(batches)
        print(f"  Fetching price history — {len(tickers)} tickers, {total_b} batches...")

        # Build the "price as of" label for the sheet header
        if market_open:
            _tz_name  = "America/New_York" if market == "US" else "Asia/Kolkata"
            _tz_label = "ET" if market == "US" else "IST"
            _tz       = ZoneInfo(_tz_name)
            _now      = datetime.now(_tz)
            price_as_of = (
                f"Price as on {_now.strftime('%b')} {_now.day}, {_now.year}"
                f"  ·  {int(_now.strftime('%I'))}:{_now.strftime('%M %p')} {_tz_label}"
                f"  (Live)"
            )
            print(f"  Market is OPEN — live prices will be used where available")
        else:
            price_as_of = None   # filled from last_date after batch loop

        today = pd.Timestamp.now().normalize()

        # Pre-compute return offset targets once (not per-stock)
        off_1m = pd.DateOffset(months=1)
        off_3m = pd.DateOffset(months=3)
        off_6m = pd.DateOffset(months=6)
        off_1y = pd.DateOffset(years=1)
        off_3y = pd.DateOffset(years=3)

        for batch_idx, batch in enumerate(batches, 1):
            print(f"  Batch {batch_idx}/{total_b}...", end=" ", flush=True)
            try:
                data = yf.download(
                    batch,
                    period      = "4y",     # FIX: was "3y" — must exceed 3Y lookback
                    auto_adjust = False,
                    group_by    = "ticker",
                    threads     = True,
                    progress    = False,
                )

                for symbol in batch:
                    try:
                        close = (
                            data["Close"] if len(batch) == 1
                            else data[symbol]["Close"]
                        )
                        close = close.dropna().sort_index()

                        if len(close) < 6:
                            continue

                        # Normalise timezone
                        raw_idx = close.index
                        if hasattr(raw_idx, "tz") and raw_idx.tz is not None:
                            idx_naive = raw_idx.tz_localize(None)
                        else:
                            idx_naive = raw_idx
                        s = pd.Series(close.values, index=idx_naive)

                        # s_confirmed = all closes before today (only trading days
                        # — yfinance never returns weekends/holidays).
                        # iloc[-1] = last trading day, iloc[-2] = the one before.
                        s_confirmed = s[idx_naive.normalize() < today]
                        if len(s_confirmed) < 2:
                            continue
                        last_td_close  = float(s_confirmed.iloc[-1])   # e.g. Friday
                        prev_td_close  = float(s_confirmed.iloc[-2])   # e.g. Thursday
                        confirmed_date = s_confirmed.index[-1]
                        if last_date is None or confirmed_date > last_date:
                            last_date = confirmed_date

                        # Display price: live when market is open, else last trading day
                        if market_open and symbol in live_prices:
                            price     = live_prices[symbol]
                            change_1d = (price / last_td_close - 1) * 100 if last_td_close else np.nan
                        else:
                            price     = last_td_close
                            change_1d = (last_td_close / prev_td_close - 1) * 100 if prev_td_close else np.nan

                        # ATH: extend to live price in case of intraday new high
                        ath          = max(float(s.max()), price)
                        pct_from_ath = (price / ath - 1) * 100

                        # Market cap from pre-fetched map
                        mcap = mcap_map.get(symbol)
                        if not mcap:
                            continue

                        # Return helper — step back to nearest prior close
                        def ret(offset):
                            target   = today - offset
                            eligible = s[s.index <= target]
                            if eligible.empty:
                                return np.nan
                            past = float(eligible.iloc[-1])
                            return (price / past - 1) * 100 if past != 0 else np.nan

                        change_1w = (
                            (price / float(s_confirmed.iloc[-6]) - 1) * 100
                            if len(s_confirmed) >= 6 else np.nan
                        )

                        all_rows.append({
                            "Ticker":     symbol.replace(".NS", ""),
                            "MarketCap":  float(mcap),
                            "Price":      price,
                            "ATH":        ath,
                            "PctFromATH": pct_from_ath,
                            "Change1D":   change_1d,
                            "Change1W":   change_1w,
                            "Change1M":   ret(off_1m),
                            "Change3M":   ret(off_3m),
                            "Change6M":   ret(off_6m),
                            "Change1Y":   ret(off_1y),
                            "Change3Y":   ret(off_3y),
                        })

                    except Exception:
                        continue

                print(f"{len(all_rows)} stocks collected")

            except Exception as e:
                print(f"\n  [WARN] Batch {batch_idx} error: {e}")

        # Finalise EOD label using the last confirmed date across all stocks
        if price_as_of is None:
            if last_date is not None:
                ld = pd.Timestamp(last_date)
                price_as_of = f"Price as on {ld.strftime('%b')} {ld.day}, {ld.year}  (Close)"
            else:
                price_as_of = "Price as on —  (Close)"

        return pd.DataFrame(all_rows), price_as_of

    # ── G&L price history (short pipeline) ───────────────────

    def _fetch_price_history_gl(self, tickers: list, mcap_map: dict, live_prices: dict, market: str = "US") -> tuple:
        """
        Download 1 month of daily data — enough for the 1W return needed by G&L.
        Much faster than the 4Y ATH download: smaller payload per batch,
        and no ATH calculation.

        Returns (df, price_as_of) where df has columns:
            Ticker | MarketCap | Price | Change1W
        """
        all_rows    = []
        market_open = self._is_market_open(market)
        batches     = [
            tickers[i:i + self.PRICE_BATCH_SIZE]
            for i in range(0, len(tickers), self.PRICE_BATCH_SIZE)
        ]
        total_b = len(batches)
        print(f"  Fetching G&L price history — {len(tickers)} tickers, {total_b} batches (1mo)...")

        today = pd.Timestamp.now().normalize()

        for batch_idx, batch in enumerate(batches, 1):
            print(f"  Batch {batch_idx}/{total_b}...", end=" ", flush=True)
            try:
                data = yf.download(
                    batch,
                    period      = "1mo",
                    auto_adjust = False,
                    group_by    = "ticker",
                    threads     = True,
                    progress    = False,
                )

                for symbol in batch:
                    try:
                        close = (
                            data["Close"] if len(batch) == 1
                            else data[symbol]["Close"]
                        )
                        close = close.dropna().sort_index()

                        if len(close) < 6:
                            continue

                        raw_idx = close.index
                        if hasattr(raw_idx, "tz") and raw_idx.tz is not None:
                            idx_naive = raw_idx.tz_localize(None)
                        else:
                            idx_naive = raw_idx
                        s = pd.Series(close.values, index=idx_naive)

                        s_confirmed = s[idx_naive.normalize() < today]
                        if s_confirmed.empty:
                            continue
                        prev_close = float(s_confirmed.iloc[-1])

                        # Use fast_info live price when market is open
                        price = live_prices.get(symbol, prev_close) if market_open else prev_close

                        change_1w = (
                            (price / float(s_confirmed.iloc[-6]) - 1) * 100
                            if len(s_confirmed) >= 6 else np.nan
                        )

                        mcap = mcap_map.get(symbol)
                        if not mcap:
                            continue

                        all_rows.append({
                            "Ticker":    symbol.replace(".NS", ""),
                            "MarketCap": float(mcap),
                            "Price":     price,
                            "Change1W":  change_1w,
                        })

                    except Exception:
                        continue

                print(f"{len(all_rows)} stocks collected")

            except Exception as e:
                print(f"\n  [WARN] Batch {batch_idx} error: {e}")

        # Build price label
        if market_open:
            _tz_name  = "America/New_York" if market == "US" else "Asia/Kolkata"
            _tz_label = "ET" if market == "US" else "IST"
            _now      = datetime.now(ZoneInfo(_tz_name))
            price_as_of = (
                f"Price as on {_now.strftime('%b')} {_now.day}, {_now.year}"
                f"  ·  {int(_now.strftime('%I'))}:{_now.strftime('%M %p')} {_tz_label}"
                f"  (Live)"
            )
        else:
            _tz_name = "America/New_York" if market == "US" else "Asia/Kolkata"
            _now     = datetime.now(ZoneInfo(_tz_name))
            price_as_of = f"Price as on {_now.strftime('%b')} {_now.day}, {_now.year}  (Close)"

        return pd.DataFrame(all_rows), price_as_of

    # ── Sheets API format request builder ────────────────────

    @staticmethod
    def _cell_fmt(sheet_id, r0, r1, c0, c1, center=False, bold=False):
        """
        Build a repeatCell format request.
        All indices are 0-based; endRow/endCol are exclusive.
        """
        fmt    = {}
        fields = []
        if center:
            fmt["horizontalAlignment"] = "CENTER"
            fields.append("userEnteredFormat.horizontalAlignment")
        if bold:
            fmt.setdefault("textFormat", {})["bold"] = True
            fields.append("userEnteredFormat.textFormat.bold")
        return {
            "repeatCell": {
                "range": {
                    "sheetId":          sheet_id,
                    "startRowIndex":    r0,
                    "endRowIndex":      r1,
                    "startColumnIndex": c0,
                    "endColumnIndex":   c1,
                },
                "cell":   {"userEnteredFormat": fmt},
                "fields": ",".join(fields),
            }
        }

    @staticmethod
    def _pct_color_fmt(sheet_id, row, col, is_positive: bool):
        """
        Build an updateCells request that sets text colour for a single cell.
        Row and col are 0-based.
        """
        color = (
            {"red": 15/255, "green": 157/255, "blue": 88/255}   # #0F9D58 Google Sheets green
            if is_positive else
            {"red": 197/255, "green": 57/255, "blue": 41/255}   # #C53929 red
        )
        return {
            "updateCells": {
                "range": {
                    "sheetId":          sheet_id,
                    "startRowIndex":    row,
                    "endRowIndex":      row + 1,
                    "startColumnIndex": col,
                    "endColumnIndex":   col + 1,
                },
                "rows": [{
                    "values": [{
                        "userEnteredFormat": {
                            "textFormat": {
                                "foregroundColor": color,
                                "foregroundColorStyle": {"rgbColor": color},
                            }
                        }
                    }]
                }],
                "fields": "userEnteredFormat.textFormat.foregroundColor,userEnteredFormat.textFormat.foregroundColorStyle",
            }
        }

    def _color_pct_cells(self, ws, sid, rows_data: list, row_offset: int, pct_cols: list):
        """
        Emit per-cell text colour requests for all percentage columns.

        rows_data  : list of row lists (already written to sheet)
        row_offset : 0-based sheet row index of the first data row
        pct_cols   : list of 0-based column indices that contain pct strings
        """
        reqs = []
        for r_idx, row in enumerate(rows_data):
            for c_idx in pct_cols:
                if c_idx >= len(row):
                    continue
                val = str(row[c_idx]).strip()
                if val in ("", "NA"):
                    continue
                try:
                    num = float(val.replace("%", ""))
                    reqs.append(
                        self._pct_color_fmt(sid, row_offset + r_idx, c_idx, num >= 0)
                    )
                except ValueError:
                    continue
        if reqs:
            self.sheet_client.apply_formats(ws, reqs)

    # ── Formatting helpers ────────────────────────────────────

    @staticmethod
    def _fmt_mcap(val, market: str) -> str:
        try:
            v = float(val)
            if market == "US":
                if v >= 1e12: return f"${v/1e12:.2f}T"
                if v >= 1e9:  return f"${v/1e9:.2f}B"
                return f"${v/1e6:.0f}M"
            else:
                cr = v / 1e7
                return f"{cr:,.0f}"
        except Exception:
            return str(val)

    @staticmethod
    def _fmt_pct(val) -> str:
        try:
            return f"{float(val):.2f}%" if not np.isnan(float(val)) else "NA"
        except Exception:
            return "NA"

    # ── Derive G&L ────────────────────────────────────────────

    def _derive_gl(
        self,
        df:         pd.DataFrame,
        name_cache: dict,
        market:     str,
    ):
        """Filter by market cap, sort by 1W (raw floats), return (gainers_df, losers_df)."""
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        floor = self.US_MCAP_FLOOR if market == "US" else self.IN_MCAP_FLOOR
        df    = df[df["MarketCap"] >= floor].dropna(subset=["Change1W"]).copy()

        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Sort on raw floats BEFORE formatting to avoid string-parse roundtrip
        df_sorted = df.sort_values("Change1W", ascending=False).reset_index(drop=True)

        # FIX (ATH India names): look up both bare ticker and ticker+".NS"
        # in case the name_cache was populated with ".NS" suffixed keys
        def _lookup_name(t, market):
            name = name_cache.get(t)
            if name:
                return name
            if market == "IN":
                name = name_cache.get(t + ".NS")
                if name:
                    return name
            return t  # fallback to ticker if still not found

        df_sorted["Name"]      = df_sorted.apply(
            lambda r: _lookup_name(r["Ticker"], market), axis=1
        )
        df_sorted["MarketCap"] = df_sorted["MarketCap"].apply(lambda v: self._fmt_mcap(v, market))
        df_sorted["Change1W"]  = df_sorted["Change1W"].apply(self._fmt_pct)

        gainers = df_sorted.head(self.TOP_N)[["Ticker", "Name", "MarketCap", "Change1W"]]
        losers  = df_sorted.tail(self.TOP_N)[["Ticker", "Name", "MarketCap", "Change1W"]].iloc[::-1].reset_index(drop=True)

        return gainers, losers

    # ── Derive ATH ────────────────────────────────────────────

    def _derive_ath(
        self,
        df:         pd.DataFrame,
        name_cache: dict,
        market:     str,
    ) -> pd.DataFrame:
        """
        Filter to stocks within ATH_THRESHOLD of their all-time high,
        apply market cap floor, sort by 1W return.
        """
        if df.empty:
            return pd.DataFrame()

        floor = self.US_MCAP_FLOOR if market == "US" else self.IN_MCAP_FLOOR
        df    = df[df["MarketCap"] >= floor].copy()

        # Within 1% of ATH means PctFromATH >= -1.0
        df = df[df["PctFromATH"] >= -self.ATH_THRESHOLD * 100].copy()

        if df.empty:
            return pd.DataFrame()

        # FIX (ATH India names): same dual-lookup as in _derive_gl
        def _lookup_name(t):
            name = name_cache.get(t)
            if name:
                return name
            if market == "IN":
                name = name_cache.get(t + ".NS")
                if name:
                    return name
            return t

        df["Name"] = df["Ticker"].map(_lookup_name)

        # Format display columns
        df["MarketCap"]  = df["MarketCap"].apply(lambda v: self._fmt_mcap(v, market))
        df["ATH"]        = df["ATH"].apply(lambda v: round(float(v), 2))
        df["Price"]      = df["Price"].apply(lambda v: round(float(v), 2))
        df["PctFromATH"] = df["PctFromATH"].apply(self._fmt_pct)
        for col in ["Change1D", "Change1W", "Change1M", "Change3M", "Change6M", "Change1Y", "Change3Y"]:
            df[col] = df[col].apply(self._fmt_pct)

        df = df.sort_values(
            "Change1W",
            key       = lambda s: pd.to_numeric(s.str.replace("%", ""), errors="coerce"),
            ascending = False,
        ).reset_index(drop=True)

        cols = [
            "Ticker", "Name", "MarketCap", "ATH", "PctFromATH",
            "Price", "Change1D", "Change1W", "Change1M", "Change3M", "Change6M", "Change1Y", "Change3Y",
        ]
        return df[cols]

    # ── Sheet writers ─────────────────────────────────────────

    def _write_gl_sheet(
        self,
        sheet_name:  str,
        gainers:     pd.DataFrame,
        losers:      pd.DataFrame,
        label:       str,
        price_as_of: str = "",
        updated_at:  str = "",
    ):
        """
        FIX (Losers layout): Losers now sit in Col F-I alongside gainers
        in Col A-D, both starting at row 2. No more vertical stacking.

        Layout:
            Row 2     : Gainers label (A2:D2) | Losers label (F2:I2)
            Row 3     : Gainers headers        | Losers headers
            Rows 4-18 : Gainers data           | Losers data
        """
        ws      = self.sheet_client.get_worksheet(sheet_name)
        updates = []
        col_hdr = [["Ticker", "Name", "Market Cap", "Change 1W"]]
        empty   = ["", "", "", ""]

        # ── Metadata (rows 1-2, read by dashboard) ────────────
        updates.append({"range": "A1", "values": [[price_as_of]]})
        updates.append({"range": "A2", "values": [[updated_at]]})

        # ── Gainers block (Col A-D) ────────────────────────────
        updates.append({"range": "A3:D3", "values": [[f"Top {self.TOP_N} Gainers - 1 Week", "", "", ""]]})
        updates.append({"range": "A4:D4", "values": col_hdr})
        gainers_vals = gainers.reset_index(drop=True).values.tolist()
        gainers_vals += [empty] * (self.TOP_N - len(gainers_vals))
        updates.append({"range": f"A5:D{4 + self.TOP_N}", "values": gainers_vals})

        # ── Losers block (Col F-I, same rows as gainers) ───────
        updates.append({"range": "F3:I3", "values": [[f"Top {self.TOP_N} Losers - 1 Week", "", "", ""]]})
        updates.append({"range": "F4:I4", "values": col_hdr})
        losers_vals = losers.reset_index(drop=True).values.tolist()
        losers_vals += [empty] * (self.TOP_N - len(losers_vals))
        updates.append({"range": f"F5:I{4 + self.TOP_N}", "values": losers_vals})

        self.sheet_client.batch_update(ws, updates)

        # ── Formatting ────────────────────────────────────────
        sid = ws.id
        f   = self._cell_fmt
        fmt_reqs = [
            # Gainers header row 4 (0-based index 3, cols A-D): center + bold
            f(sid, 3, 4, 0, 4, center=True, bold=True),
            # Losers header row 4 (0-based index 3, cols F-I): center + bold
            f(sid, 3, 4, 5, 9, center=True, bold=True),
            # Gainers data rows 5-19 (0-based 4 onwards, cols A-D): col A center+bold, cols C-D center
            f(sid, 4, 4 + self.TOP_N, 0, 1, center=True, bold=True),
            f(sid, 4, 4 + self.TOP_N, 2, 4, center=True),
            # Losers data rows 5-19 (0-based 4 onwards, cols F-I): col F center+bold, cols H-I center
            f(sid, 4, 4 + self.TOP_N, 5, 6, center=True, bold=True),
            f(sid, 4, 4 + self.TOP_N, 7, 9, center=True),
        ]
        self.sheet_client.apply_formats(ws, fmt_reqs)

        # ── Per-cell text colour for % columns ────────────────
        # Change 1W is col D (index 3) for gainers, col I (index 8) for losers
        # row_offset = 4 (0-based) = sheet row 5
        self._color_pct_cells(ws, sid, gainers_vals, row_offset=4, pct_cols=[3])
        self._color_pct_cells(ws, sid, losers_vals,  row_offset=4, pct_cols=[8])
        print(f"  {label} G&L -> '{sheet_name}' done")

    def _write_ath_sheet(
        self,
        sheet_name:  str,
        df:          pd.DataFrame,
        label:       str,
        price_as_of: str = "",
        updated_at:  str = "",
    ):
        """
        FIX (data starts from row 5):
            Row 1  : price_as_of metadata (read by dashboard)
            Row 2  : updated_at metadata  (read by dashboard)
            Row 3  : Section label
            Row 4  : Column headers
            Row 5+ : Data rows
        """
        ws      = self.sheet_client.get_worksheet(sheet_name)
        updates = []
        ncols   = 13   # A–M
        empty   = [""] * ncols

        col_hdr = [[
            "Ticker", "Name", "Market Cap", "ATH", "ATH %",
            "Price", "1D%", "1W%", "1M%", "3M%", "6M%", "1Y%", "3Y%",
        ]]

        # ── Metadata (rows 1-2, read by dashboard) ────────────
        updates.append({"range": "A1", "values": [[price_as_of]]})
        updates.append({"range": "A2", "values": [[updated_at]]})

        if df.empty:
            updates.append({"range": "A3:M3", "values": [["No stocks currently at all-time high"] + [""] * (ncols - 1)]})
            updates.append({"range": "A4:M4", "values": col_hdr})
            updates.append({"range": f"A5:M{4 + 50}", "values": [empty] * 50})
        else:
            updates.append({"range": "A3:M3", "values": [[f"Stocks within 5% of All-Time High — sorted by 1W%"] + [""] * (ncols - 1)]})
            updates.append({"range": "A4:M4", "values": col_hdr})
            # Data rows start at row 5 + 50 trailing clear rows
            all_rows = df.reset_index(drop=True).values.tolist() + [empty] * 50
            updates.append({"range": f"A5:M{4 + len(all_rows)}", "values": all_rows})

        self.sheet_client.batch_update(ws, updates)

        # ── Formatting ────────────────────────────────────────
        sid      = ws.id
        f        = self._cell_fmt
        data_end = 200   # generous upper bound for ATH rows
        fmt_reqs = [
            # Column header row 4 (0-based index 3): all cols center + bold
            f(sid, 3, 4, 0, 13, center=True, bold=True),
            # Data rows 5-200 (0-based 4 onwards): col A (ticker) center + bold
            f(sid, 4, data_end, 0, 1, center=True, bold=True),
            # Data rows 5-200: cols C-M (all numeric cols) center
            f(sid, 4, data_end, 2, 13, center=True),
        ]
        self.sheet_client.apply_formats(ws, fmt_reqs)

        # ── Re-pin banding at row 4 (headers) ─────────────────
        # Google Sheets Tables auto-shift their range when data is written.
        # Tables own their banded ranges so we can't update them independently.
        # Fix: delete any Table (frees the banding), delete orphan banded ranges,
        # then re-create a plain banded range pinned at row 4.
        data_rows = len(df) if not df.empty else 0
        end_row_idx = 4 + max(data_rows, 1)   # 0-based exclusive
        correct_range = {
            "sheetId":          sid,
            "startRowIndex":    3,           # 0-based row 4 (headers)
            "endRowIndex":      end_row_idx,
            "startColumnIndex": 0,
            "endColumnIndex":   ncols,
        }
        try:
            meta = ws.spreadsheet.fetch_sheet_metadata()
            for sheet in meta["sheets"]:
                if sheet["properties"]["sheetId"] != sid:
                    continue
                # Step 1: delete Tables (they own the banded ranges)
                del_reqs = []
                for tbl in sheet.get("tables", []):
                    del_reqs.append({"deleteTable": {"tableId": tbl["tableId"]}})
                for br in sheet.get("bandedRanges", []):
                    del_reqs.append({"deleteBandedRange": {"bandedRangeId": br["bandedRangeId"]}})
                if del_reqs:
                    self.sheet_client.apply_formats(ws, del_reqs)
                # Step 2: re-create plain banded range at correct position
                self.sheet_client.apply_formats(ws, [{"addBandedRange": {"bandedRange": {
                    "range": correct_range,
                    "rowProperties": {
                        "headerColorStyle":    {"rgbColor": {"red": 0.259, "green": 0.522, "blue": 0.957}},
                        "firstBandColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}},
                        "secondBandColorStyle": {"rgbColor": {"red": 0.867, "green": 0.867, "blue": 0.867}},
                    },
                }}}])
                break
        except Exception as e:
            print(f"  [WARN] Could not re-pin banding on '{sheet_name}': {e}")

        # ── Per-cell text colour for % columns ────────────────
        # ATH%=col4, 1D%=col6, 1W%=col7, 1M%=col8, 3M%=col9, 6M%=col10, 1Y%=col11, 3Y%=col12
        # row_offset = 4 (0-based) = sheet row 5
        if not df.empty:
            ath_rows = df.reset_index(drop=True).values.tolist()
            self._color_pct_cells(ws, sid, ath_rows, row_offset=4,
                                  pct_cols=[4, 6, 7, 8, 9, 10, 11, 12])
        n = len(df) if not df.empty else 0
        print(f"  {label} ATH -> '{sheet_name}' ({n} stocks) done")

    # ── Public entry point ────────────────────────────────────

    def run(self, run_gl: bool = True, run_ath: bool = True):
        print("\n===== STOCKS DATA UPDATE =====\n")

        # Compute "updated at" timestamp once for this run (IST)
        _ist = ZoneInfo("Asia/Kolkata")
        _now = datetime.now(_ist)
        updated_at = (
            f"Updated {_now.strftime('%b')} {_now.day}, {_now.year}"
            f"  ·  {int(_now.strftime('%I'))}:{_now.strftime('%M %p')} IST"
        )

        # Load name cache once — shared across all markets
        name_cache = self._load_name_cache()
        print(f"Name cache: {len(name_cache)} tickers already known\n")

        # ── US ────────────────────────────────────────────────
        print("--- US (Russell 3000) ---")
        us_tickers, us_name_map = self._fetch_russell3000()

        if us_tickers:
            # Always overwrite from iShares CSV — authoritative source,
            # ensures any bad ticker=ticker entries get corrected immediately
            updated = sum(1 for t, n in us_name_map.items() if name_cache.get(t) != n)
            name_cache.update(us_name_map)
            if updated:
                self._save_name_cache(name_cache)
                print(f"  Seeded/updated {updated} US names from iShares CSV")
            else:
                print(f"  US names: all {len(us_tickers)} already up-to-date")

            # Fetch market caps + live prices in one pass, then filter universe
            us_mcaps, us_live = self._fetch_market_caps(us_tickers)
            us_filtered = [t for t in us_tickers if us_mcaps.get(t, 0) >= self.US_MCAP_FLOOR]
            print(f"  Market cap filter: {len(us_filtered)}/{len(us_tickers)} tickers pass ${self.US_MCAP_FLOOR/1e9:.0f}B floor")

            if run_gl:
                us_gl_df, us_gl_label = self._fetch_price_history_gl(us_filtered, us_mcaps, us_live, market="US")
                us_gainers, us_losers = self._derive_gl(us_gl_df, name_cache, "US")
                if not us_gainers.empty:
                    self._write_gl_sheet("Top G&L US", us_gainers, us_losers, "US",
                                         price_as_of=us_gl_label, updated_at=updated_at)
                else:
                    print("  [WARN] US G&L: no stocks passed market cap filter.")

            if run_ath:
                us_ath_df, us_ath_label = self._fetch_price_history_ath(us_filtered, us_mcaps, us_live, market="US")
                us_ath = self._derive_ath(us_ath_df, name_cache, "US")
                self._write_ath_sheet("ATH US", us_ath, "US",
                                      price_as_of=us_ath_label, updated_at=updated_at)
        else:
            print("  [SKIP] US universe unavailable.")

        print()

        # ── India ─────────────────────────────────────────────
        print("--- India (NIFTY Total Market) ---")
        in_tickers, in_name_map = self._fetch_nifty_total_market()

        if in_tickers:
            # Always overwrite from NSE CSV — authoritative source,
            # ensures any bad ticker=ticker entries get corrected immediately
            updated = sum(1 for t, n in in_name_map.items() if name_cache.get(t) != n)
            name_cache.update(in_name_map)
            if updated:
                self._save_name_cache(name_cache)
                print(f"  Seeded/updated {updated} India names from NSE CSV")
            else:
                print(f"  India names: all {len(in_tickers)} already up-to-date")

            # Fetch market caps + live prices in one pass, then filter universe
            in_mcaps, in_live = self._fetch_market_caps(in_tickers)
            in_filtered = [t for t in in_tickers if in_mcaps.get(t, 0) >= self.IN_MCAP_FLOOR]
            print(f"  Market cap filter: {len(in_filtered)}/{len(in_tickers)} tickers pass Rs{self.IN_MCAP_FLOOR/1e7:.0f}Cr floor")

            if run_gl:
                in_gl_df, in_gl_label = self._fetch_price_history_gl(in_filtered, in_mcaps, in_live, market="IN")
                in_gainers, in_losers = self._derive_gl(in_gl_df, name_cache, "IN")
                if not in_gainers.empty:
                    self._write_gl_sheet("Top G&L India", in_gainers, in_losers, "India",
                                         price_as_of=in_gl_label, updated_at=updated_at)
                else:
                    print("  [WARN] India G&L: no stocks passed market cap filter.")

            if run_ath:
                in_ath_df, in_ath_label = self._fetch_price_history_ath(in_filtered, in_mcaps, in_live, market="IN")
                in_ath = self._derive_ath(in_ath_df, name_cache, "IN")
                self._write_ath_sheet("ATH India", in_ath, "India",
                                      price_as_of=in_ath_label, updated_at=updated_at)
        else:
            print("  [SKIP] India universe unavailable.")

        print("\n===== STOCKS DATA UPDATE COMPLETE =====")

# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":

    # python stocks_data.py                # full run (G&L + ATH)
    # python stocks_data.py --gl-only      # gainers/losers only
    # python stocks_data.py --ath-only     # ATH only

    gl_only  = "--gl-only"  in sys.argv
    ath_only = "--ath-only" in sys.argv

    run_gl  = not ath_only
    run_ath = not gl_only

    config       = Config()
    sheet_client = GoogleSheetClient(config)
    engine       = StocksDataEngine(sheet_client)
    engine.run(run_gl=run_gl, run_ath=run_ath)