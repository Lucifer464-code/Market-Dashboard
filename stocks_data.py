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
    # Vanguard VTI (Total Stock Market ETF) — ~3,400 holdings, covers Russell 3000
    # universe. Switched from iShares IWV in 2026-05 after iShares put the .ajax
    # CSV endpoint behind a JS/bot wall that returns HTML regardless of headers.
    US_HOLDINGS_URL = (
        "https://investor.vanguard.com/investment-products/etfs/profile/"
        "api/vti/portfolio-holding/stock"
    )
    US_HOLDINGS_PAGE_SIZE = 500
    NIFTY_TOTAL_MARKET_URL = (
        "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"
    )

    # ── Manual additions ──────────────────────────────────────
    # Recently-listed stocks not yet in NSE's index constituent CSV
    # (NSE only updates it on periodic reconstitution). Merged into the
    # universe so they appear on the dashboard. {SYMBOL: Company Name}.
    IN_MANUAL_ADDITIONS = {
        "SEDEMAC": "SEDEMAC Mechatronics Ltd.",
    }

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
    # 500-ticker batches reliably trip yfinance's rate limiter: one such
    # request came back with 499 of 500 symbols throttled. yf.download does not
    # raise in that case — it returns all-NaN columns, which the per-symbol
    # loop skips silently — so the run simply lost those stocks and reported
    # success. Smaller batches stay under the limit; the extra round trips are
    # far cheaper than silently dropping half the universe.
    PRICE_BATCH_SIZE = 100
    MCAP_WORKERS     = 8     # parallel threads for market cap prefetch (too high triggers 401/429)

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

    def _load_pkl_cache(self, path, *, allow_stale: bool = False):
        if not os.path.exists(path):
            return None
        if not allow_stale and (time.time() - os.path.getmtime(path)) / 3600 > self.CACHE_TTL_HOURS:
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
        Fetch US universe from Vanguard VTI portfolio-holdings JSON API.
        Returns (tickers, name_map) — same contract as before; the method name
        is kept for backward compatibility with the cache file and call site.
        On fetch failure, falls back to the pickle cache even if stale.
        """
        cached = self._load_pkl_cache(self.US_CACHE_FILE)
        if cached:
            if isinstance(cached, tuple):
                tickers, name_map = cached
                name_map = {k: v.title() for k, v in name_map.items()}
            else:
                tickers, name_map = cached, {}
            print(f"  US universe: using cache ({len(tickers)} tickers)")
            return tickers, name_map

        print("  Downloading US universe (Vanguard VTI)...")
        try:
            holdings = []
            start = 1
            while True:
                url = (
                    f"{self.US_HOLDINGS_URL}"
                    f"?start={start}&count={self.US_HOLDINGS_PAGE_SIZE}"
                )
                r = requests.get(
                    url,
                    headers={**self.HEADERS, "Accept": "application/json"},
                    timeout=30,
                )
                r.raise_for_status()
                page = r.json().get("fund", {}).get("entity", []) or []
                if not page:
                    break
                holdings.extend(page)
                if len(page) < self.US_HOLDINGS_PAGE_SIZE:
                    break
                start += self.US_HOLDINGS_PAGE_SIZE

            df = pd.DataFrame(holdings)
            if df.empty or "ticker" not in df.columns:
                raise RuntimeError("Vanguard VTI response missing ticker column")

            df["Ticker"] = df["ticker"].astype(str).str.strip().str.upper()
            # Prefer longName ("NVIDIA Corp.") over shortName ("NVIDIA CORP")
            name_col = "longName" if "longName" in df.columns else "shortName"
            df["Name"] = df[name_col].astype(str).str.strip().str.title()

            # Filter to clean equity tickers (alpha, <=5 chars, drop placeholders)
            df = df[
                df["Ticker"].apply(
                    lambda t: bool(t and t != "-" and t.isalpha() and len(t) <= 5)
                )
            ]
            df = df.drop_duplicates(subset=["Ticker"], keep="first")

            name_map = dict(zip(df["Ticker"], df["Name"]))
            tickers = df["Ticker"].tolist()

            print(f"  US universe: {len(tickers)} tickers, {len(name_map)} names loaded from VTI")
            self._save_pkl_cache(self.US_CACHE_FILE, (tickers, name_map))
            return tickers, name_map

        except Exception as e:
            print(f"  [ERROR] US universe fetch failed: {e}")
            stale = self._load_pkl_cache(self.US_CACHE_FILE, allow_stale=True)
            if stale:
                if isinstance(stale, tuple):
                    tickers, name_map = stale
                    name_map = {k: v.title() for k, v in name_map.items()}
                else:
                    tickers, name_map = stale, {}
                age_h = (time.time() - os.path.getmtime(self.US_CACHE_FILE)) / 3600
                print(f"  [FALLBACK] Using stale cache ({len(tickers)} tickers, {age_h:.0f}h old)")
                return tickers, name_map
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

            # Merge manual additions for stocks NSE hasn't added to the index yet
            for sym, name in self.IN_MANUAL_ADDITIONS.items():
                if sym not in name_map:
                    name_map[sym] = name
                    tickers.append(f"{sym}.NS")

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

        Returns (mcap_map, live_prices, prev_closes):
            mcap_map    : {ticker: market_cap_float} — only tickers with valid caps
            live_prices : {ticker: last_price_float} — used instead of intraday download
            prev_closes : {ticker: previous_close_float} — the most recent SETTLED
                          close. yfinance's daily history can lag a full session
                          (the latest row arrives with Close=NaN), but fast_info
                          carries that close already, so this is the only reliable
                          way to get last-close figures on the day they settle.
        """
        print(f"  Fetching market caps for {len(tickers)} tickers (parallel)...")

        def _get(symbol):
            for attempt in range(3):
                try:
                    fi         = yf.Ticker(symbol).fast_info
                    mcap       = fi.market_cap
                    last_price = getattr(fi, "last_price", None)
                    prev_close = getattr(fi, "previous_close", None)
                    return (
                        symbol,
                        float(mcap)       if mcap       else None,
                        float(last_price) if last_price else None,
                        float(prev_close) if prev_close else None,
                    )
                except Exception:
                    if attempt < 2:
                        time.sleep(1 + attempt)
            return symbol, None, None, None

        mcap_map    = {}
        live_prices = {}
        prev_closes = {}
        failed      = []
        with ThreadPoolExecutor(max_workers=self.MCAP_WORKERS) as pool:
            for symbol, mcap, last_price, prev_close in pool.map(_get, tickers):
                if mcap:
                    mcap_map[symbol] = mcap
                else:
                    failed.append(symbol)
                if last_price:
                    live_prices[symbol] = last_price
                if prev_close:
                    prev_closes[symbol] = prev_close

        # A ticker whose fast_info never resolved used to just vanish: it fell
        # out of mcap_map, so the market-cap filter dropped it and it never
        # reached the price stage at all. Nothing distinguished "this stock is
        # too small" from "yfinance rate-limited us", which is how ATH India
        # silently lost whole stretches of the alphabet — 134 of 749 tickers on
        # one run, clustered wherever the throttling happened to land.
        #
        # Retry the stragglers serially. fast_info is one network call per
        # ticker and the failures are load-induced, so a slower second pass
        # recovers most of them.
        if failed:
            print(f"  [RETRY] {len(failed)} ticker(s) unresolved — retrying serially...")
            recovered = 0
            for symbol in failed:
                try:
                    fi   = yf.Ticker(symbol).fast_info
                    mcap = fi.market_cap
                    if mcap:
                        mcap_map[symbol] = float(mcap)
                        recovered += 1
                        lp = getattr(fi, "last_price", None)
                        pc = getattr(fi, "previous_close", None)
                        if lp:
                            live_prices[symbol] = float(lp)
                        if pc:
                            prev_closes[symbol] = float(pc)
                except Exception:
                    pass
                time.sleep(0.15)
            print(f"  [RETRY] recovered {recovered}/{len(failed)}")

        still_missing = len(tickers) - len(mcap_map)
        print(f"  Market caps: {len(mcap_map)}/{len(tickers)} tickers resolved")
        if still_missing:
            # Loud, because these are excluded from every downstream sheet.
            print(f"  [WARN] {still_missing} ticker(s) have no market cap and "
                  f"will be missing from ATH/G&L output.")
        return mcap_map, live_prices, prev_closes

    # Liquid, always-traded reference used to establish which session the
    # sheet should be priced from. These names trade every session, so their
    # newest bar is the market's newest bar.
    _SESSION_REFERENCE = {
        "US": ["SPY", "AAPL", "MSFT"],
        "IN": ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"],
    }

    def _latest_settled_session(self, market: str = "US"):
        """The most recent SETTLED session for a market, as a normalised date.

        Individual tickers cannot be trusted to answer this: a thinly traded
        name may simply have no bar for a session (AZAD had Aug 14, Aug 17 and
        Aug 19 but no Aug 18), so letting each row use its own newest bar mixes
        sessions within one table. Resolve it once from liquid references and
        hold every row to it.

        A session counts as settled only once its close exists. Mid-session the
        provider already carries today's bar with a live, still-moving close,
        so today is excluded unless the market has closed.
        """
        refs = self._SESSION_REFERENCE.get(market, self._SESSION_REFERENCE["US"])
        today = pd.Timestamp.now().normalize()
        market_open = self._is_market_open(market)

        best = None
        for sym in refs:
            try:
                hist = yf.download(sym, period="1mo", auto_adjust=False,
                                   progress=False)
                col = hist["Close"]
                if hasattr(col, "columns"):
                    col = col.iloc[:, 0]
                col = col.dropna()
                if col.empty:
                    continue
                idx = col.index
                idx = idx.tz_localize(None) if getattr(idx, "tz", None) is not None else idx
                dates = [d.normalize() for d in idx]
                # Drop today while the session is still running — its close has
                # not settled yet.
                if market_open:
                    dates = [d for d in dates if d < today]
                if not dates:
                    continue
                cand = max(dates)
                if best is None or cand > best:
                    best = cand
            except Exception:
                continue

        if best is not None:
            print(f"  Pricing session: {best.date()} "
                  f"({'market open, today excluded' if market_open else 'market closed'})")
        else:
            print("  [WARN] could not resolve a pricing session from reference "
                  "tickers; falling back to each ticker's newest settled bar.")
        return best

    def _download_batch_with_retry(self, batch: list, period: str = "max"):
        """yf.download for a batch, retrying and then splitting on throttle.

        yfinance signals rate limiting by returning all-NaN columns rather
        than raising, so "did this work?" has to be answered by inspecting the
        data. Coverage is measured as the share of symbols with a usable close
        series; anything well below full is treated as throttled.
        """
        def _coverage(df, syms):
            if df is None or df.empty:
                return 0.0
            ok = 0
            for s in syms:
                try:
                    col = df[s]["Close"] if len(syms) > 1 else df["Close"]
                    if col.dropna().shape[0] >= 6:
                        ok += 1
                except Exception:
                    pass
            return ok / max(len(syms), 1)

        def _fetch(syms):
            return yf.download(syms, period=period, auto_adjust=False,
                               group_by="ticker", threads=True, progress=False)

        data = None
        for attempt in range(3):
            try:
                data = _fetch(batch)
            except Exception as e:
                print(f"\n  [WARN] batch download error: {type(e).__name__}: {e}")
                data = None
            cov = _coverage(data, batch)
            if cov >= 0.9:
                return data
            if attempt < 2:
                wait = 5 * (attempt + 1)
                print(f"\n  [RETRY] batch coverage {cov:.0%} — likely rate "
                      f"limited; waiting {wait}s (attempt {attempt + 1}/3)")
                time.sleep(wait)

        # Still short: split into smaller chunks, which the limiter tolerates
        # far better than one large request.
        if len(batch) > 50:
            print(f"\n  [SPLIT] retrying {len(batch)} tickers in chunks of 50")
            merged = {}
            for i in range(0, len(batch), 50):
                chunk = batch[i:i + 50]
                for chunk_attempt in range(2):
                    try:
                        sub = _fetch(chunk)
                    except Exception:
                        sub = None
                    if _coverage(sub, chunk) >= 0.5:
                        break
                    time.sleep(3)
                if sub is not None and not sub.empty:
                    for s in chunk:
                        try:
                            merged[s] = sub[s] if len(chunk) > 1 else sub
                        except Exception:
                            pass
                time.sleep(1)
            if merged:
                return pd.concat(merged, axis=1)

        return data

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

    def _fetch_price_history_ath(self, tickers: list, mcap_map: dict, live_prices: dict,
                                 prev_closes: dict | None = None, market: str = "US") -> tuple:
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
        # Newest SETTLED close date seen across the universe. Drives the sheet
        # label, so it must track the close actually used per stock — not the
        # newest bar yfinance happens to carry.
        last_date   = None
        # Session every row must be priced from. Without this each ticker fell
        # back to its OWN newest bar, so one table mixed sessions: a run on
        # Aug 19 produced 41 rows priced Aug 17, 49 priced Aug 18 and 1 priced
        # Aug 19, all under a single "Aug 18" header. Rows priced on different
        # days cannot be compared with each other, which is the whole point of
        # the sheet. Resolved once from a liquid reference below.
        target_session = self._latest_settled_session(market)
        stale_skipped  = []   # (symbol, its newest session) for reporting
        market_open = self._is_market_open(market)
        batches     = [
            tickers[i:i + self.PRICE_BATCH_SIZE]
            for i in range(0, len(tickers), self.PRICE_BATCH_SIZE)
        ]
        total_b = len(batches)
        print(f"  Fetching price history — {len(tickers)} tickers, {total_b} batches...")

        # The "price as of" label is always resolved from the close date
        # actually used, after the batch loop — never from the clock.
        #
        # Both markets are settled-close only. Intraday prices make 1D% drift
        # through the session and never tie out against a source quoting
        # closes. The bug this replaces stamped "(Live)" from _is_market_open
        # while the rows held day-old closes: a run shortly after the open
        # finds no fresh candle, silently falls back to the last close, and the
        # clock-based label then misdated the whole sheet by a session.
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
            rows_before = len(all_rows)
            try:
                # yf.download does NOT raise when it is rate-limited — it
                # returns a frame full of NaNs. The per-symbol loop below then
                # sees len(close) < 6 and quietly skips, so a throttled batch
                # drops every one of its tickers while still printing
                # "N stocks collected". That is how ATH India lost whole
                # stretches of the alphabet: one 500-ticker batch came back
                # 499/500 rate-limited and nothing said so.
                #
                # Retry the batch until enough symbols carry real data, then
                # fall back to smaller sub-batches, which are far less likely
                # to trip the limiter.
                data = self._download_batch_with_retry(batch)

                for symbol in batch:
                    try:
                        sym_df = data if len(batch) == 1 else data[symbol]
                        close = sym_df["Close"]

                        # yfinance sometimes returns the most recent trading
                        # day with Close=NaN while High/Low/Open are already
                        # populated (the close has not settled in the feed).
                        # Only "Adj Close" is an acceptable stand-in — Open or
                        # High would invent a close that never happened.
                        if close.isna().any() and "Adj Close" in sym_df.columns:
                            close = close.fillna(sym_df["Adj Close"])

                        # Every date the provider returned a BAR for, including
                        # one whose Close has not settled yet. Needed to tell a
                        # not-yet-settled session apart from one that never
                        # traded, and to date the settled close correctly.
                        _all_idx  = sym_df.index
                        raw_dates = list(
                            _all_idx.tz_localize(None)
                            if getattr(_all_idx, "tz", None) is not None
                            else _all_idx
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

                        # Closes up to and including the target session, so
                        # every row in the sheet is priced from the SAME day.
                        #
                        # This used to be `< today`, which had two failure
                        # modes. It discarded today's close even after the
                        # market had shut, leaving the sheet a session behind;
                        # and because each ticker then fell back to its own
                        # newest bar, a name missing that session (AZAD had no
                        # Aug 18 bar) silently dropped another day further
                        # back, mixing sessions inside one table.
                        cutoff = target_session if target_session is not None else (
                            today - pd.Timedelta(days=1)
                        )
                        s_confirmed = s[idx_naive.normalize() <= cutoff]
                        if len(s_confirmed) < 2:
                            continue
                        last_td_close  = float(s_confirmed.iloc[-1])   # e.g. Friday
                        prev_td_close  = float(s_confirmed.iloc[-2])   # e.g. Thursday
                        confirmed_date = s_confirmed.index[-1]
                        has_today_bar  = bool((idx_naive.normalize() == today).any())

                        # Drop rows that cannot reach the target session. A
                        # ticker with no bar for it would otherwise be shown
                        # with an older close beside up-to-date rows, and
                        # nothing in the sheet would reveal the difference.
                        if (target_session is not None
                                and confirmed_date.normalize() < target_session):
                            stale_skipped.append(
                                (symbol, str(confirmed_date.date()))
                            )
                            continue

                        # Settled closes only — no intraday prices.
                        #
                        # yfinance's daily history can lag a full session: the
                        # most recent trading day arrives with Close=NaN, so
                        # s_confirmed ends a day early and every return silently
                        # describes the session BEFORE the one it claims (AMZN
                        # read +15.32%, its Jul 31 earnings gap, on Aug 4).
                        #
                        # fast_info.previous_close carries that settled close
                        # before it reaches the history, so prefer it when it is
                        # genuinely newer than the newest bar we have. Its date
                        # is the next trading day after last_td_close, which is
                        # what the sheet must report.
                        settled = (prev_closes or {}).get(symbol)

                        # Use the newest SETTLED close.
                        #
                        # s_confirmed excludes today, so mid-session it already
                        # ends on the correct day. The gap case is different:
                        # after a session closes, the provider may carry a bar
                        # for it with Close=NaN (Open/High/Low only) while
                        # fast_info.previous_close already holds that settled
                        # close. Without this, the sheet reports the session
                        # before the last one — the original AMZN +15.32% bug.
                        #
                        # Only trust `settled` when a bar exists past
                        # confirmed_date AND that bar has no close of its own;
                        # mid-session `previous_close` is just last_td_close, so
                        # taking it then would change nothing but risks pairing
                        # a stale reference with a fresh price.
                        gap_bar_date = None
                        if not has_today_bar and settled:
                            gap_bar_date = next(
                                (d for d in raw_dates
                                 if d.normalize() > confirmed_date.normalize()),
                                None,
                            )

                        if gap_bar_date is not None:
                            price        = float(settled)
                            change_1d    = (price / last_td_close - 1) * 100 if last_td_close else np.nan
                            settled_date = gap_bar_date
                        else:
                            price        = last_td_close
                            change_1d    = (last_td_close / prev_td_close - 1) * 100 if prev_td_close else np.nan
                            settled_date = confirmed_date

                        if last_date is None or settled_date > last_date:
                            last_date = settled_date

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

                # Report per-batch yield: a batch that returns far fewer
                # rows than tickers means the download was throttled, and
                # that must be visible rather than inferred later from a
                # short sheet.
                got = len(all_rows) - rows_before
                flag = "" if got >= len(batch) * 0.8 else "  [LOW YIELD]"
                print(f"{got}/{len(batch)} stocks collected{flag}")

            except Exception as e:
                print(f"\n  [WARN] Batch {batch_idx} error: {e}")

        if stale_skipped:
            print(f"  [STALE] {len(stale_skipped)} ticker(s) had no bar for "
                  f"{target_session.date()} and were excluded rather than shown "
                  f"with an older price:")
            for sym, dt in stale_skipped[:10]:
                print(f"      {sym} (newest {dt})")
            if len(stale_skipped) > 10:
                print(f"      ... and {len(stale_skipped) - 10} more")

        # Finalise EOD label using the last confirmed date across all stocks.
        # If live prices were used before today's candle existed, the figures
        # are intraday — dating them to the last confirmed close would repeat
        # the exact mistake this pipeline is meant to avoid.
        if price_as_of is None:
            if last_date is not None:
                ld = pd.Timestamp(last_date)
                price_as_of = f"Price as on {ld.strftime('%b')} {ld.day}, {ld.year}  (Close)"
            else:
                price_as_of = "Price as on —  (Close)"

        return pd.DataFrame(all_rows), price_as_of

    # ── G&L price history (short pipeline) ───────────────────

    def _fetch_price_history_gl(self, tickers: list, mcap_map: dict, live_prices: dict,
                                prev_closes: dict | None = None, market: str = "US") -> tuple:
        """
        Download 1 month of daily data — enough for the 1W return needed by G&L.
        Much faster than the 4Y ATH download: smaller payload per batch,
        and no ATH calculation.

        Returns (df, price_as_of) where df has columns:
            Ticker | MarketCap | Price | Change1W
        """
        all_rows    = []
        # Newest settled close date seen across the universe — drives the
        # sheet label, so it must track the close actually used per stock.
        last_close_date = None
        # One pricing session for every row (see _latest_settled_session).
        target_session  = self._latest_settled_session(market)
        batches     = [
            tickers[i:i + self.PRICE_BATCH_SIZE]
            for i in range(0, len(tickers), self.PRICE_BATCH_SIZE)
        ]
        total_b = len(batches)
        print(f"  Fetching G&L price history — {len(tickers)} tickers, {total_b} batches (1mo)...")

        today = pd.Timestamp.now().normalize()

        for batch_idx, batch in enumerate(batches, 1):
            print(f"  Batch {batch_idx}/{total_b}...", end=" ", flush=True)
            rows_before = len(all_rows)
            try:
                # Same throttle handling as the ATH pipeline: a rate-limited
                # yf.download returns all-NaN rather than raising, which would
                # silently drop the batch's tickers from the G&L universe.
                data = self._download_batch_with_retry(batch, period="1mo")

                for symbol in batch:
                    try:
                        sym_df = data if len(batch) == 1 else data[symbol]
                        close = sym_df["Close"]

                        # yfinance sometimes returns the most recent trading
                        # day with Close=NaN while High/Low/Open are already
                        # populated (the close has not settled in the feed).
                        # Only "Adj Close" is an acceptable stand-in — Open or
                        # High would invent a close that never happened.
                        if close.isna().any() and "Adj Close" in sym_df.columns:
                            close = close.fillna(sym_df["Adj Close"])

                        # Dates the provider returned a BAR for, including one
                        # whose Close has not settled — used to date the settled
                        # close correctly (see the US branch below).
                        _all_idx  = sym_df.index
                        raw_dates = list(
                            _all_idx.tz_localize(None)
                            if getattr(_all_idx, "tz", None) is not None
                            else _all_idx
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

                        # Pin to one session for the whole sheet — see the
                        # ATH pipeline: per-ticker fallback mixed sessions
                        # inside a single table.
                        cutoff = target_session if target_session is not None else (
                            today - pd.Timedelta(days=1)
                        )
                        s_confirmed = s[idx_naive.normalize() <= cutoff]
                        if s_confirmed.empty:
                            continue
                        prev_close = float(s_confirmed.iloc[-1])
                        _cd = s_confirmed.index[-1]

                        # US: settled closes only. yfinance's daily history can
                        # lag a session (latest row has Close=NaN), so take the
                        # settled close from fast_info when it is newer than the
                        # newest bar — same correction as the ATH pipeline, and
                        # keep _cd pointing at the day actually used.
                        _settled = (prev_closes or {}).get(symbol)
                        _has_today_bar = bool((idx_naive.normalize() == today).any())
                        _gap_date = None
                        if not _has_today_bar and _settled:
                            _gap_date = next(
                                (d for d in raw_dates
                                 if d.normalize() > _cd.normalize()), None)

                        # Both markets: settled closes only, same rule as ATH.
                        if _gap_date is not None:
                            price = float(_settled)
                            _cd   = _gap_date
                        else:
                            price = prev_close

                        if last_close_date is None or _cd > last_close_date:
                            last_close_date = _cd

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

                # Report per-batch yield: a batch that returns far fewer
                # rows than tickers means the download was throttled, and
                # that must be visible rather than inferred later from a
                # short sheet.
                got = len(all_rows) - rows_before
                flag = "" if got >= len(batch) * 0.8 else "  [LOW YIELD]"
                print(f"{got}/{len(batch)} stocks collected{flag}")

            except Exception as e:
                print(f"\n  [WARN] Batch {batch_idx} error: {e}")

        # Build the price label from the close date actually used, never from
        # the clock. The old branch stamped TODAY's date with "(Close)" even
        # when the figures came from an earlier session — Top G&L US read
        # "Price as on Aug 4 (Close)" while ATH US, on the same data, said
        # Jul 31 — and a clock-based "(Live)" label misdated ATH India by a
        # full session when a run just after the open found no fresh prices.
        if last_close_date is not None:
            ld = pd.Timestamp(last_close_date)
            price_as_of = f"Price as on {ld.strftime('%b')} {ld.day}, {ld.year}  (Close)"
        else:
            price_as_of = "Price as on —  (Close)"

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
        # See _write_ath_sheet: A1 is what tells the reader which trading day
        # these figures belong to, so a malformed label must not reach it.
        if not price_as_of or not str(price_as_of).lower().startswith("price as on"):
            print(f"  [WARN] {sheet_name}: refusing to write malformed "
                  f"price_as_of {price_as_of!r}; using a fallback label.")
            price_as_of = "Price as on — (date unavailable)"

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

        col_hdr = [[
            "Ticker", "Name", "Market Cap", "ATH", "ATH %",
            "Price", "1D%", "1W%", "1M%", "3M%", "6M%", "1Y%", "3Y%",
        ]]

        # ── Metadata (rows 1-2, read by dashboard) ────────────
        # A1 is the only place the sheet discloses WHICH trading day the
        # prices and returns belong to. If it is empty or truncated the page
        # shows figures with no date to contradict them — a stale 1D% then
        # reads as current. Refuse to write a label that is obviously not one.
        if not price_as_of or not str(price_as_of).lower().startswith("price as on"):
            print(f"  [WARN] {sheet_name}: refusing to write malformed "
                  f"price_as_of {price_as_of!r}; using a fallback label.")
            price_as_of = "Price as on — (date unavailable)"

        updates.append({"range": "A1", "values": [[price_as_of]]})
        updates.append({"range": "A2", "values": [[updated_at]]})

        # Wipe any stale rows from previous runs before writing
        ws.batch_clear(["A5:M200"])

        if df.empty:
            updates.append({"range": "A3:M3", "values": [["No stocks currently at all-time high"] + [""] * (ncols - 1)]})
            updates.append({"range": "A4:M4", "values": col_hdr})
        else:
            updates.append({"range": "A3:M3", "values": [[f"Stocks within 5% of All-Time High — sorted by 1W%"] + [""] * (ncols - 1)]})
            updates.append({"range": "A4:M4", "values": col_hdr})
            data_rows = df.reset_index(drop=True).values.tolist()
            updates.append({"range": f"A5:M{4 + len(data_rows)}", "values": data_rows})

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
                    del_reqs.append({"deleteBanding": {"bandedRangeId": br["bandedRangeId"]}})
                if del_reqs:
                    self.sheet_client.apply_formats(ws, del_reqs)
                # Step 2: re-create plain banded range at correct position
                self.sheet_client.apply_formats(ws, [{"addBanding": {"bandedRange": {
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

    def run(self, run_gl: bool = True, run_ath: bool = True,
             run_us: bool = True, run_in: bool = True):
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
        if not run_us:
            print("--- US: skipped ---\n")
        else:
            print("--- US (Russell 3000) ---")
            us_tickers, us_name_map = self._fetch_russell3000()

            if us_tickers:
                # Always overwrite from Vanguard VTI — authoritative source,
                # ensures any bad ticker=ticker entries get corrected immediately
                updated = sum(1 for t, n in us_name_map.items() if name_cache.get(t) != n)
                name_cache.update(us_name_map)
                if updated:
                    self._save_name_cache(name_cache)
                    print(f"  Seeded/updated {updated} US names from iShares CSV")
                else:
                    print(f"  US names: all {len(us_tickers)} already up-to-date")

                # Fetch market caps + live prices in one pass, then filter universe
                us_mcaps, us_live, us_prev = self._fetch_market_caps(us_tickers)
                us_filtered = [t for t in us_tickers if us_mcaps.get(t, 0) >= self.US_MCAP_FLOOR]
                print(f"  Market cap filter: {len(us_filtered)}/{len(us_tickers)} tickers pass ${self.US_MCAP_FLOOR/1e9:.0f}B floor")

                if run_gl:
                    us_gl_df, us_gl_label = self._fetch_price_history_gl(us_filtered, us_mcaps, us_live, us_prev, market="US")
                    us_gainers, us_losers = self._derive_gl(us_gl_df, name_cache, "US")
                    if not us_gainers.empty:
                        self._write_gl_sheet("Top G&L US", us_gainers, us_losers, "US",
                                             price_as_of=us_gl_label, updated_at=updated_at)
                    else:
                        print("  [WARN] US G&L: no stocks passed market cap filter.")

                if run_ath:
                    us_ath_df, us_ath_label = self._fetch_price_history_ath(us_filtered, us_mcaps, us_live, us_prev, market="US")
                    us_ath = self._derive_ath(us_ath_df, name_cache, "US")
                    self._write_ath_sheet("ATH US", us_ath, "US",
                                          price_as_of=us_ath_label, updated_at=updated_at)
            else:
                print("  [SKIP] US universe unavailable.")

            print()

        # ── India ─────────────────────────────────────────────
        if not run_in:
            print("--- India: skipped ---\n")
        else:
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
                in_mcaps, in_live, in_prev = self._fetch_market_caps(in_tickers)
                in_filtered = [t for t in in_tickers if in_mcaps.get(t, 0) >= self.IN_MCAP_FLOOR]
                print(f"  Market cap filter: {len(in_filtered)}/{len(in_tickers)} tickers pass Rs{self.IN_MCAP_FLOOR/1e7:.0f}Cr floor")

                if run_gl:
                    in_gl_df, in_gl_label = self._fetch_price_history_gl(in_filtered, in_mcaps, in_live, in_prev, market="IN")
                    in_gainers, in_losers = self._derive_gl(in_gl_df, name_cache, "IN")
                    if not in_gainers.empty:
                        self._write_gl_sheet("Top G&L India", in_gainers, in_losers, "India",
                                             price_as_of=in_gl_label, updated_at=updated_at)
                    else:
                        print("  [WARN] India G&L: no stocks passed market cap filter.")

                if run_ath:
                    in_ath_df, in_ath_label = self._fetch_price_history_ath(in_filtered, in_mcaps, in_live, in_prev, market="IN")
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

    # python stocks_data.py                    # full run (G&L + ATH, US + India)
    # python stocks_data.py --gl-only          # gainers/losers only
    # python stocks_data.py --ath-only         # ATH only
    # python stocks_data.py --india-only       # India only (G&L + ATH)
    # python stocks_data.py --us-only          # US only (G&L + ATH)
    # Combine freely: --india-only --ath-only  # India ATH only

    gl_only    = "--gl-only"    in sys.argv
    ath_only   = "--ath-only"   in sys.argv
    india_only = "--india-only" in sys.argv
    us_only    = "--us-only"    in sys.argv

    run_gl  = not ath_only
    run_ath = not gl_only
    run_us  = not india_only
    run_in  = not us_only

    config       = Config()
    sheet_client = GoogleSheetClient(config)
    engine       = StocksDataEngine(sheet_client)
    engine.run(run_gl=run_gl, run_ath=run_ath, run_us=run_us, run_in=run_in)