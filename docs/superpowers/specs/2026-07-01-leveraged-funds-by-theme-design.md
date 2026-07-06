# Leveraged Funds by Theme — Design

Date: 2026-07-01
Status: Approved (design), pending spec review

## Goal

Add a **"Leveraged Funds by Theme"** dashboard page with **two combining
dropdowns** — **Leverage** (2x / 3x / …) and **Theme** (Single Stock / Index /
Sector-Thematic / Commodity / Factor-Dividend / Other). Driven by a new
script-generated Google Sheet tab, mirroring the ETFs US by Sector build.

Why theme, not sector: yfinance classifies 105/115 leveraged funds as
"Trading--Leveraged Equity" (structure, not sector), so a GICS-sector page
would be all "Other". The fund **name** is the only viable signal, so Theme is
parsed from the name via keyword rules. Leverage comes straight from the
sheet's existing Leverage column (100% accurate).

## Data flow

```
ManualETFEngine (existing run)
  1. _update_sheet("Biggest Leveraged Funds ", ...)   — existing price refresh
  2. build_leveraged_theme_tab()                       — NEW:
       read the just-refreshed Leveraged rows
         (Ticker=A, Name=B, Leverage=C, AUM formatted=E, Price=F, returns G:M)
       classify Theme from the Name via keyword rules
       write "Leveraged Funds by Theme":
         [Theme, Leverage, Ticker, Name, AUM, Price, 1D, 5D, 1M, 3M, 6M, 1Y, 3Y]
         sorted by Theme, then 5D desc

Dashboard "Leveraged Funds by Theme" page
  load tab → Leverage dropdown AND Theme dropdown (combine, AND) → render_table
```

## Components

### 1. Name→Theme classifier (`database.py`)
`_classify_leveraged_theme(name) -> str`, ordered keyword rules (order matters
— checked top to bottom, first match wins):

1. **Commodity** — gold, miner, oil, silver, gas, mlp, platinum, uranium,
   commodit
2. **Factor/Dividend** — factor, dividend, volatility, quality, momentum,
   high dividend  (before Index, so "MSCI Quality Factor" reads as Factor)
3. **Sector/Thematic** — financ, bank, biotech, semiconductor, pharma, medical,
   aerospace, defense/defence, consumer, transportation, travel, robotic,
   artificial intelligence, fang, cloud, internet, cyber, auto, bdc,
   real estate, utilit, energy, health, industr, communication, retail,
   homebuild, innovation, mstr, coin
4. **Index** — s&p, nasdaq, qqq, dow, russell, ftse, msci, eafe, csi, stoxx,
   nifty, emerging, small cap/smallcap, mid cap/midcap, large cap,
   total market
5. **Single Stock** — otherwise, if the name contains a Long/Bull/Ultra token
   (the leverage-on-a-single-name pattern) → Single Stock
6. **Other** — fallback (mostly "Accelerated Plus" / "Buffer" defined-outcome
   funds, which genuinely don't fit — acceptable)

Verified against the live 115-name list: distribution ≈ Single Stock 32,
Sector/Thematic 36, Index 25, Factor 9, Commodity 6, Other ~7. Pure string
logic — no yfinance/network calls, no cache needed.

### 2. New sheet writer (`ManualETFEngine.build_leveraged_theme_tab`)
- `LEVERAGED_THEME_TAB = "Leveraged Funds by Theme"`.
- Read "Biggest Leveraged Funds " rows `A7:M122` (Ticker=A, Name=B,
  Leverage=C, AUM formatted=E, Price=F, returns G..M).
- Build `[Theme, Leverage, Ticker, Name, AUM, Price, 1D..3Y]`; sort by
  (Theme asc, 5D desc).
- Clear the tab's data area, write header (row 3) + data (row 4+) + A1/A2
  metadata (`_make_metadata("US")`).
- Skip gracefully (log, don't crash) if the tab is missing.
- Called at the end of `update_all()` (after the leveraged refresh), next to
  `build_sector_tab()`. Add a `_sheet_map` entry for standalone runs.

### 3. Dashboard page
- `dashboard/data.py`: `load_leveraged_by_theme()` reads the new tab
  (header row 3, `A3:M400`), cached 8h.
- `dashboard.py`: new page **"Leveraged Funds by Theme"** in the **FUNDS**
  nav group (after Leveraged Funds). Render branch:
  - `safe_load(...)`,
  - **Leverage** selectbox = `["All leverage"] + sorted(unique Leverage)`,
  - **Theme** selectbox = `["All themes"] + sorted(unique Theme)`,
  - filter by both (AND); a count caption; `render_table(searchable=True)`.
  - Keep Theme/Leverage columns visible (they're useful context); no column
    dropping needed since both are meaningful.

## Error handling
- Classifier never raises (pure string ops; empty/None name → "Other").
- `safe_load` + `load_error`; empty tab → empty state.
- Missing worksheet → build step logs "create the tab first" and skips.

## Testing / verification
- `_classify_leveraged_theme`: TSLL→Single Stock, SPXL/TQQQ/UDOW→Index,
  FAS/LABU/BNKU→Sector/Thematic, NUGT/GDXU→Commodity, QULL/IWFL→Factor/Dividend.
- Full-list distribution has few "Other" (~7, the defined-outcome funds).
- Tab builds + sorts; page's two filters combine correctly; files parse.

## Files touched
- `database.py` — `_classify_leveraged_theme`, `build_leveraged_theme_tab`,
  wire into `update_all` + `_sheet_map`.
- `dashboard/data.py` — `load_leveraged_by_theme()`.
- `dashboard.py` — NAV entry + render branch.
- (Setup) create an empty "Leveraged Funds by Theme" worksheet once.

## Out of scope / notes
- Name-parsing is imperfect by nature; tuned against the real list but some
  odd names will land in "Other" or a wrong bucket. Leverage filter is exact.
- No live price; inherits the sheet's normal refresh cadence.
- Commodity-leveraged tab ("Biggest Leveraged Funds(Com.)") not included.
