# Highs & Lows Dashboard — Design

Date: 2026-10-22
Status: draft for review

## Why

The existing IFPL Market Dashboard covers many markets and asset classes; its
ATH US / ATH India sections are one corner of it. The user wants a separate,
focused app for stock highs and lows, which adds two views the current one
lacks (stocks near their 52-week high, and stocks that crashed and have
stopped falling), lets the user narrow either market to its main index, and
has a fresh look rather than the existing dashboard's style.

## What it is

A second Streamlit app ("Highs & Lows") with its own link, reading from the
**same Google Sheet** as the current dashboard, from **new tabs** written by a
new data job. The existing dashboard, its sheets and `stocks_data.py` keep
working exactly as they do today.

### Three views

Each view has a **US / India** toggle and an **index dropdown**:
US: *S&P 500* or *All*; India: *NIFTY 500* or *All*.

| View | Who qualifies |
|---|---|
| **PTH** (near 52-week high) | Price within 5% of its 52-week high. |
| **ATH** (near all-time high) | Price within 5% of its all-time high. Same rule as today's ATH sheets (`ATH_THRESHOLD = 0.05`, full price history). |
| **Fallen from PTH** | Price is **down 80–90% from its 52-week high** (price / 52w high between 0.10 and 0.20), **and** it has **not made a new 52-week low in the past 2 weeks** (10 trading sessions): the 52-week low was set more than 10 sessions ago. |

The user said "down 80–90%" and confirmed that reading over "trading at
80–90% of the high". They were shown that this leaves few stocks (on Oct 19:
5 US at $1B+, 2 India, before the 2-week rule) and chose to keep it.

### Universe and size floors

- **US:** all US-listed common stock **including ADRs** (user's choice), with
  market cap **≥ $1B** for this app. That's lower than the existing $2B, at the
  user's request. Source: yfinance equity screener (see Data below).
- **India:** NIFTY Total Market universe, as in `stocks_data.py`, with the
  same **≥ ₹500 Cr** floor (`IN_MCAP_FLOOR`).
- Excluded: warrants, preferreds, units, and other non-common lines
  (the screener's single-letter share-class rule from `stocks_data.py`:
  `BRK-B` stays in, `JPM-PC` and `DJTWW` stay out).

### Index membership

- **S&P 500:** current constituents list (builder's choice of source; the
  Wikipedia "List of S&P 500 companies" table is the usual one). Normalise
  `BRK.B` → `BRK-B` to match yfinance.
- **NIFTY 500:** NSE's `ind_nifty500list.csv` from `nsearchives.nseindia.com`,
  the same host the job already uses for NIFTY Total Market.
- Each row gets an `in_index` flag; the dropdown filters on it. If an index
  list can't be fetched, fall back to the last cached copy. If there's no
  cache, the dropdown shows only "All" with a short note, rather than an
  empty table.

## Look and behaviour

The user picked a blend of two mockups ("E" fintech app + "G" bento). The
mockups are in `.superpowers/brainstorm/1359-1792652255/content/`
(`blend-eg.html` and `row-expand.html`) and are the reference.

- **Light only. No dark mode** (the user rejected it).
- White and very light grey (#f7f8fa) surfaces, **green accent (#00a86b)**,
  red for falls (#e5484d), Inter/system sans, rounded cards (~14px radius).
- **Header:** app name, plus a "Close · <date>" pill showing the pricing
  session the data is from.
- **Controls row:** a US / India segmented toggle, and the index dropdown on
  the right.
- **Three count tiles** (PTH · ATH · Fallen) that **are the tabs**. Each shows
  how many stocks qualify under the current market and index selection; the
  selected one is filled green, and clicking another switches view.
- **List of stocks**, one row each: ticker (bold), company name and market cap
  underneath, a small **trend line** (last ~1 month of closes for PTH and ATH,
  ~3 months for Fallen), the view's key number, and price with 1W change.
  - PTH / ATH key number: **% from the high** (52-week or all-time).
  - Fallen columns: **% down from the 52-week high**, **% up from the 52-week
    low**, and "low N days ago" in the subtitle.
- **Click a row to expand it in place** (approved via the live mockup):
  1. a returns strip: **1D, 1W, 1M, 3M, 6M, 1Y, 3Y**, coloured up/down;
  2. a **52-week range bar** with a dot at today's price, labelled with the
     low and high;
  3. facts: all-time high, market cap, index membership.
  **Recent listings:** stocks with less than a year of price history are
  included in every view like any other stock (user's choice). Wherever the
  trend line or the range bar is shown, they get a small-font note,
  "Listed <date>", so it's clear their "52-week" high and low only go back
  to the listing. Listing date = first bar in the downloaded history. Store
  it in a `ListedDate` column (blank when there's ≥ 1 year of history).
  Expanding must not trigger a Streamlit rerun (use HTML `<details>` or an
  equivalent client-side approach). Several rows may be open at once.
- Must work on phone width (the existing app is used as a PWA on mobile).

## Data

### New job: `highs_data.py`

A separate, self-contained batch script. **`stocks_data.py` must not be
edited for this work** (user's requirement). `highs_data.py` may *import*
`StocksDataEngine` helpers (session resolution, the batched download with
throttle retry, name cache, India universe fetch) as long as that needs no
change to `stocks_data.py`; where a helper doesn't fit as-is, copy and adapt
it inside `highs_data.py`. Add it to `run_all.py`, but run it **after** `stocks_data.py`
finishes, not alongside it. `run_all.py` starts batch jobs in parallel today,
and two full-universe `period="max"` downloads at once would double the
yfinance load and invite the throttling that dropped stocks on Oct 19. The
whole run takes longer; coverage matters more. *(My call.)*

**US universe and market caps come from the screener in a single pass.** The
yfinance screener returns `marketCap`, `regularMarketPrice` and
`regularMarketPreviousClose` per stock. Use those instead of the per-ticker
`fast_info` sweep. On Oct 19 that sweep was rate-limited (1,646 of 2,206
tickers failed), silently cutting the US sheets to a quarter of their stocks,
and it took about 25 minutes. India keeps its existing market-cap path, since
the screener's India coverage hasn't been checked against the NSE list.

**Price history:** one `period="max"` download per batch (as the existing ATH
pipeline does) gives everything: the all-time high, 52-week high/low and the
low's date, returns, and the trend-line points. Price every row from one
settled session, using the same rule as `stocks_data.py`
(`_latest_settled_session`), and drop rows that can't reach it.

### New sheet tabs

One tab per market, holding **every stock that qualifies for at least one
view**, with flag columns so the app filters and counts without recomputing:

`H&L US`, `H&L India`

| Column | Notes |
|---|---|
| Ticker, Name, MarketCap | display ticker (no `.NS`) |
| Price | settled close of the pricing session |
| High52, Low52, Low52Date, ATH | |
| FromHigh52 %, FromATH %, OffLow52 % | |
| R1D, R1W, R1M, R3M, R6M, R1Y, R3Y | `NA` where the history is too short, as today |
| InIndex | TRUE/FALSE (S&P 500 or NIFTY 500) |
| IsPTH, IsATH, IsFallen | TRUE/FALSE |
| Spark | comma-separated recent closes for the trend line |
| ListedDate | first trading date, only when history is under 1 year |

Rows 1–2 hold the metadata, as on the existing sheets: A1 "Price as on …
(Close)", A2 "Updated …". The header goes on row 3, data from row 4.

### Sort order (builder's call, adjustable)

PTH: closest to the high first. ATH: by 1W %, as the existing sheets do.
Fallen: by % up from the low, highest first.

## Constraints

- Same Google Sheet and service account as today. Same password login
  (`dashboard/auth.py`, `st.secrets["PASSWORD"]`). Reuse it, don't fork it.
- Deployed by the user as a second Streamlit Cloud app from the same repo.
  The entry file must be distinct from `dashboard.py`.
- Must not change what the existing dashboard or its sheets show.
- yfinance throttles: keep batch sizes and retry behaviour no more aggressive
  than `stocks_data.py`.

## Goals / non-goals / anti-goals

**Goals:** the three views for both markets, the index filter, the approved
look, and click-to-expand returns.

**Non-goals:** dark mode, live intraday prices, editing or watchlists, alerts,
and any change to the existing dashboard.

**Anti-goals** (a failure even if it works):
- **Silently missing stocks.** If coverage drops (e.g. market caps or prices
  failing), the job must say so loudly in its log, and the app should show a
  small warning when a market's row count is far below normal. This is the
  failure that happened on Oct 19.
- **Mislabelled dates:** the "Close · date" must be the session the prices
  actually come from.
- **Junk in Fallen.** An 80–90% "fall" that is really a split, demerger or bad
  price print (VEDL showed up this way) makes the tab misleading. Screen these
  out (see Left to the builder).

## Decided by the user

- Separate app, same Google Sheet, new tabs.
- Three views: PTH (within 5% of 52w high), ATH (as today), Fallen
  (down 80–90% from 52w high, no new 52w low in 2 weeks).
- US/India in every view, with S&P 500 / NIFTY 500 / All dropdowns.
- US floor $1B for this app; ADRs included.
- Light, fresh look: blend of mockups E + G; no dark mode.
- Full returns shown by clicking a stock (expand in place).
- Separate data script; no changes to `stocks_data.py`.
- Recent listings stay in every view, marked with a small "Listed <date>".

## My calls (flagged for review)

- One sheet tab per market with flag columns, rather than six tabs.
- New script `highs_data.py` rather than a mode of `stocks_data.py`.
- India floor stays ₹500 Cr (only the US floor change was requested).
- Sort orders above.
- Trend-line windows: 1 month for PTH/ATH, 3 months for Fallen.
- S&P 500 list from Wikipedia.
- `highs_data.py` runs after `stocks_data.py` in `run_all.py`, not in parallel.

## Left to the builder

- **Corporate-action screen for Fallen:** e.g. skip a stock whose drop is
  mostly one single-session gap over 50%, or use split-adjusted closes and
  cross-check against yfinance's split/dividend actions. Pick something
  simple, and log what it excluded.
- File and package layout for the new app (e.g. `highs_app.py` plus a
  `highs/` package mirroring `dashboard/`), caching TTLs, and how the
  rendering is split between Streamlit and custom HTML.
- How "row count far below normal" is judged (e.g. compared with the
  previous run's count stored in the sheet).
