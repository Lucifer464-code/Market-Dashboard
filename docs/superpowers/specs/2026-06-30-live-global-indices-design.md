# Live Global Indices — Design

Date: 2026-06-30
Status: Approved (design), pending spec review

## Goal

Make the **Global Indices** and **Additional Global Indices** dashboard pages
auto-refresh their **Price** and **1D%** columns every 30 seconds by fetching
yfinance directly from Streamlit. The slower-moving return columns
(5D/1M/3M/6M/1Y/3Y) keep coming from the Google Sheet as today. No Firebase,
no ticker, no data-script run required for the live part.

## Honest constraints (baked into the UI)

- **Delayed ~15 min:** free world-index data (yfinance/Yahoo) is delayed; the
  page is labeled accordingly. Not true real-time.
- **Timezones:** world markets aren't all open at once, so only the currently-
  open markets visibly change between refreshes.
- **Live = Price + 1D% only.** Other returns refresh on the sheet's normal cadence.

## Architecture

Same "dashboard polls a cloud API directly" model as planned for the US page —
crucially, **no middle layer** (no Firestore/PC ticker) is needed because
yfinance is a cloud API the Streamlit server can call itself.

```
Global Indices page (re-runs every 30s via auto-refresh)
  ├─ load_global_indices()  → sheet rows: names + 5D..3Y returns   [existing, cache 8h]
  └─ fetch_global_live(syms) → yfinance batch: price + 1D% per index [new, cache 30s]
        merge live Price + 1D onto the sheet DataFrame (match by index name)
        → render_table
```

## Components

### 1. Name→ticker map (shared)
`database.py` already has `GlobalIndicesEngine.TABLE1_TICKER_MAP` and
`TABLE2_TICKER_OVERRIDES`. To avoid importing the heavy `database.py` (pulls
gspread/kite/etc.) into the dashboard, **duplicate a slim map** in
`dashboard/data.py`: `GLOBAL_INDEX_TICKERS = {index_name: yf_symbol}` covering
the names that appear in the two sheet tables. (Names are matched **stripped**
of surrounding whitespace — e.g. the sheet has "KOSPI " with a trailing space.)

### 2. `fetch_global_live(symbols)` in `dashboard/data.py`
- One **batched** `yf.download(symbols, period="5d", interval="1d", progress=False,
  group_by="ticker")` for all unique symbols across both tables.
- For each symbol: `price` = last available close; `prev` = the close before it;
  `change_1d = (price/prev - 1) * 100`. (During a market's session yfinance's last
  row is the live partial candle, so this gives "vs previous close" — same
  convention used elsewhere in the codebase.)
- Returns `{index_name: {"price": float, "chg1d": float}}` keyed by the sheet
  index name (reverse of the ticker map).
- `@st.cache_data(ttl=30)` — concurrent viewers share one fetch; bounds Yahoo load.
- On any exception: return `{}` (caller then leaves sheet values untouched).

### 3. Auto-refresh + merge in `dashboard.py`
- **Auto-refresh without a new dependency:** `streamlit-autorefresh` is NOT
  installed and we won't add it. Use a small `components.html` snippet that calls
  `setTimeout(() => window.parent.location.reload(), 30000)` (a self-reloading
  iframe), rendered only on these two pages. (Decision: this keeps zero new Python
  deps; the reload re-runs the Streamlit script, which re-reads the 30s-cached
  live fetch.)
- In each branch (Global Indices, Additional Global Indices), after loading the
  sheet DataFrame:
  - call `fetch_global_live(...)`,
  - for each row, if the (stripped) index name has a live entry, **overwrite the
    `Price` and `1D` cells** with the live values; otherwise leave the sheet
    values.
  - Then the existing sort / `render_table` runs as today.
- Add a caption under the header: "Live price & 1D% · delayed ~15 min · updates
  every 30s".

## Error handling
- `fetch_global_live` returns `{}` on failure → the page renders the sheet
  values unchanged (never blank). Wrapped consistently with the existing
  `safe_load` philosophy.
- Rows whose name doesn't resolve to a ticker simply keep their sheet Price/1D.

## Files touched
- `dashboard/data.py` — add `GLOBAL_INDEX_TICKERS` map + `fetch_global_live()`.
- `dashboard.py` — auto-refresh snippet + live merge on the two Global Indices
  branches + caption.

## Testing / verification
- `fetch_global_live(["^GSPC","^FTSE"])` returns a dict with `price`/`chg1d`
  floats for resolvable symbols; returns `{}` on a forced exception.
- Offline-stub render: confirm the merged DataFrame shows live Price/1D for
  matched names and sheet values for unmatched ones.
- Confirm the auto-refresh snippet is emitted only on the two pages and the
  component reloads the parent.
- All files parse.

## Out of scope / notes
- Not changing the data-script (`database.py`) Global Indices writer; the sheet
  still updates on its normal cadence for the longer returns.
- Not adding real-time (paid) world data — explicitly delayed.
- yfinance from Streamlit Cloud shared IPs can occasionally rate-limit; the 30s
  cache + graceful fallback mitigate this. If it proves flaky, the documented
  next step is the hybrid (sheet fallback already is the fallback) or a longer
  interval.
