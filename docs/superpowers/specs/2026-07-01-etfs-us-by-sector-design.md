# ETFs US by Sector — Design

Date: 2026-07-01
Status: Approved (design), pending spec review

## Goal

Add a new **"ETFs US by Sector"** dashboard page with a **sector dropdown**
(the 11 GICS sectors + "Other"). It is driven by a new Google Sheet tab of
the same name, **auto-generated** by the ETF data script from the tickers
already maintained in the existing **ETFs US** manual list — each ETF
classified by its yfinance `category`, normalized to a GICS sector.

Only the plain **ETFs US** list is classified. Leveraged and Commodity ETF
lists are untouched (yfinance categorizes leveraged ETFs by structure, not
sector, so a sector split there is not meaningful).

## Data flow

```
ManualETFEngine (existing ETF update run)
  1. _update_sheet("ETFs US", ...)  — refreshes ETFs US prices/returns (existing)
  2. _build_sector_tab()            — NEW:
       read the just-updated ETFs US rows (Ticker/Name/AUM/Price/1D..3Y)
       classify each ticker: yfinance category → GICS sector (cached CSV)
       write "ETFs US by Sector" tab: [Sector, Ticker, Name, AUM, Price, 1D..3Y]
       sorted by Sector, then 5D desc

Dashboard "ETFs US by Sector" page
  load tab → sector dropdown filter (All + present sectors) → render_table
```

## Components

### 1. Sector classification (`database.py`)
- `CATEGORY_TO_GICS`: dict mapping yfinance `category` strings to one of the
  11 GICS sectors. Names match the S&P 500 Sectors page:
  Technology, Healthcare, Financials, Energy, Industrials, Materials,
  Consumer Discretionary, Consumer Staples, Utilities, Real Estate,
  Communication Services. Anything not a clean sector → **"Other"**.
  (yfinance uses e.g. "Technology", "Health", "Equity Energy", "Financial",
  "Industrials", "Natural Resources", "Consumer Cyclical",
  "Consumer Defensive", "Utilities", "Real Estate", "Communications";
  the map handles these plus falls through to "Other".)
- `_classify_etf_sector(ticker) -> str`: `yf.Ticker(t).info.get("category")`
  → normalize via `CATEGORY_TO_GICS` (case-insensitive, substring-tolerant)
  → sector or "Other". Any exception → "Other".
- **Cache:** `etf_sectors.csv` (ticker,sector). On each run, only tickers not
  already in the cache are looked up (parallel thread pool); the cache is then
  updated. Keeps re-runs cheap and resilient to Yahoo hiccups.

### 2. New sheet writer (extend `ManualETFEngine`)
- `SECTOR_TAB = "ETFs US by Sector"` — a new worksheet the user creates once
  (empty); the script populates it.
- `_build_sector_tab()`:
  - Read the ETFs US rows that were just refreshed. ETFs US layout:
    Ticker=B, Name=C, AUM=D (numeric) / E (formatted), Price=F, returns G:M.
    Read `B{start}:M{end}` to get Ticker + Name + AUM(formatted, col E) +
    Price + the 7 returns per row.
  - For each ticker, get its sector from the classifier/cache.
  - Build rows: `[Sector, Ticker, Name, AUM, Price, 1D, 5D, 1M, 3M, 6M, 1Y, 3Y]`.
  - Sort by (Sector asc, 5D desc).
  - Clear the tab's data area, write header row + rows, and the A1/A2 metadata
    (reuse `_make_metadata("US")`).
- Called at the end of `update_all()` (after ETFs US is refreshed) and wired so
  it runs in the normal cycle. Also add a `_sheet_map` entry so it can be run
  standalone via `run_single`.

### 3. Dashboard page
- `dashboard/data.py`: `load_etfs_us_by_sector()` reads the new tab
  (header row 3-style, generous row bound), cached 8h, via `_range_to_df`.
- `dashboard.py`: new page **"ETFs US by Sector"** in the **FUNDS** nav group
  (single-source NAV → mobile nav derives automatically). Render branch:
  - `safe_load(data.load_etfs_us_by_sector)`,
  - build a `st.selectbox` of `["All sectors"] + sorted(unique Sector)`,
  - filter the DataFrame to the chosen sector (drop the Sector column from the
    displayed table when a single sector is selected, keep it for "All"),
  - `render_table` (searchable, like the other large ETF tables).

## Error handling
- Classification failures → "Other"; never crash the run.
- `safe_load` + `load_error` on the page; empty tab → the existing empty state.
- If the "ETFs US by Sector" worksheet doesn't exist yet, `_build_sector_tab`
  catches the error and logs a clear "create the tab first" message rather than
  crashing the whole ETF update.

## Testing / verification
- `_classify_etf_sector`: IGV/CIBR/XLK → Technology, IBB → Healthcare,
  SPY → Other, an energy ETF → Energy.
- `CATEGORY_TO_GICS` covers the yfinance category strings actually seen in the
  ETFs US list (spot-check against the live list).
- `_build_sector_tab` produces rows with a Sector column, sorted correctly
  (verify against a small sample without writing, or a dry-run print).
- Dashboard: page appears in nav; selectbox filters; `render_table` renders;
  files parse.

## Files touched
- `database.py` — `CATEGORY_TO_GICS`, `_classify_etf_sector`, sector CSV cache,
  `ManualETFEngine._build_sector_tab`, wire into `update_all` + `_sheet_map`.
- `dashboard/data.py` — `load_etfs_us_by_sector()`.
- `dashboard.py` — NAV entry + render branch.
- (User action) create an empty "ETFs US by Sector" worksheet once.

## Out of scope / notes
- Leveraged / Commodity ETF sector splits (not meaningful via yfinance).
- Live price on this page — it inherits the sheet's normal refresh cadence.
- The "Other" bucket is expected (broad-market/bond/odd ETFs); not a bug.
