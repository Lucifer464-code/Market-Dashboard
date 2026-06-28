# Live Market Page — Design

Date: 2026-06-28
Status: Approved (design), pending spec review

## Goal

Add a **"Live Market"** page to the existing Streamlit dashboard that shows
live NSE prices while the market is open: headline indices, the 14 NIFTY
sectoral indices, and the broad NIFTY indices. Live updates come from the
StockPulse Firestore project (`stockpulse-e3c5f`), which is fed by
StockPulse's `ticker.py` (Zerodha WebSocket) running on the user's PC.

## Key architectural decision

The live page is a **client-side Firestore widget embedded in Streamlit**,
not a server-side read. The browser that loads the Streamlit dashboard
connects directly to Firestore via the Firebase web SDK and `onSnapshot`
(push updates) — exactly as StockPulse's `firebase.js` does today. Streamlit
only hosts the HTML via `components.html`.

Consequences:
- No Firebase **Python** SDK added to the dashboard.
- No service-account secret on Streamlit Cloud. Only the Firebase **web
  config** is used (client-safe by design; Firestore rules are read-only:
  `allow read: if true; allow write: if false`).
- The 8h Google-Sheets cache is irrelevant to this page — it's a separate
  data path.
- True real-time updates (push), matching StockPulse, despite Streamlit
  having no native realtime.

```
ticker.py (user PC, Zerodha WS) ──writes──► Firestore (stockpulse-e3c5f)
                                                  │ onSnapshot (push)
Streamlit Cloud ──serves HTML──► user browser ───┘──► live tiles
```

## Scope

1. New read-only "Live Market" Streamlit page (one `components.html` block).
2. `ticker.py`: add the broad NIFTY indices to a new `broad_indices`
   Firestore collection (sectors + headline indices already covered).
3. `seed.js`: seed `broad_indices` so the page has values before first live
   run.

Out of scope: changing existing dashboard pages; Firebase Python SDK in the
dashboard; writing live data into Google Sheets; a stock watchlist on this
page; automated Zerodha login.

## Component 1 — "Live Market" Streamlit page

- Registered in `dashboard.py` `NAV` under the **MARKETS** group
  (label "Live Market", key "Live Market"); mobile nav derives from NAV
  automatically (single-source nav already in place).
- A new render branch builds one `components.html(...)` block containing:
  - Firebase web SDK (CDN, compat build, same versions StockPulse uses:
    `firebasejs/10.8.0` app-compat + firestore-compat).
  - The `firebaseConfig` for project `stockpulse-e3c5f`.
  - `onSnapshot` listeners adapted from `StockPulse/firebase.js`.
  - Render + formatting logic adapted from `StockPulse/app.js`
    (`formatPrice`, `formatChangePct`, sector bar width/color), restyled to
    match this dashboard (dark `#0f172a` headers, GREEN `#34a853` /
    RED `#ea4335`).
- Three live sections, top to bottom:
  1. **Headline KPI tiles**: NIFTY 50, SENSEX, BANK NIFTY, VIX
     (`indices` collection, docs `nifty50`/`sensex`/`banknifty`/`vix`).
  2. **NIFTY Sectoral Indices** (14): `sectors` collection.
  3. **Broad NIFTY Indices**: new `broad_indices` collection.
- **Market-status badge**: reuse StockPulse's IST clock + `NSE_HOLIDAYS`
  rule in JS. Open → "NSE Open" green pulsing; Closed → "NSE Closed" grey
  steady + "Showing closing prices · as of 15:30 IST". Honors
  `prefers-reduced-motion`.
- Component height generous enough for all three sections; `scrolling`
  handled inside the component.

### Firebase web config location
Client-safe, but to avoid hard-coding in source, read from
`st.secrets["FIREBASE_WEB_CONFIG"]` (a dict) with a fallback to the known
`stockpulse-e3c5f` values so it works before secrets are set. Final choice
(secrets vs. constant) is the user's; default to secrets-with-fallback.

## Component 2 — ticker.py broad indices

Add the broad NIFTY indices, resolved by NSE tradingsymbol at startup the
same way sectors already are (`resolve_sector_instruments` pattern →
generalize to a shared resolver, or add a parallel `resolve_broad_indices`).

Broad set (from the Data Bot NIFTY Indices sheet C21:C28, excluding
NIFTY 50 which is already a headline KPI):

| doc id | NSE tradingsymbol | display name |
|---|---|---|
| nifty-100 | NIFTY 100 | NIFTY 100 |
| nifty-500 | NIFTY 500 | NIFTY 500 |
| nifty-midcap-100 | NIFTY MIDCAP 100 | NIFTY Midcap 100 |
| nifty-smlcap-100 | NIFTY SMLCAP 100 | NIFTY Smallcap 100 |
| nifty-microcap-250 | NIFTY MICROCAP250 | NIFTY Microcap 250 |
| nifty-total-mkt | NIFTY TOTAL MKT | NIFTY Total Market |
| nifty-500-mom-50 | NIFTY500MOMENTM50 | NIFTY500 Momentum 50 |

(Exact tradingsymbols verified against the live sheet; tokens resolved via
`kite.instruments("NSE")` at startup — unresolved symbols are skipped with a
warning, never crash the feed, matching the existing sector behavior.)

Firestore `broad_indices/{id}` doc shape (mirrors sectors):
```
{ name: string, value: number, change_pct: number, sort_order: number }
```
The flush loop writes these in the same batch, gated by `is_market_open`
(frozen close off-hours), identical to indices/sectors.

## Component 3 — seed.js

Add a `broad_indices` seed block (the 7 docs above with sensible mock
values + sort_order) so the page is populated before the first live run,
consistent with how sectors/indices are seeded.

## Error handling

Reuse StockPulse's patterns inside the component:
- `onSnapshot` `onError`: log to console, leave last-rendered values in
  place (no blank UI).
- If a collection is empty/unreachable: that section shows a small
  "Live feed unavailable" note; other sections still render.
- If the ticker isn't running: Firestore holds the last writes (the close),
  so the page shows frozen values + the "Closed / feed idle" badge — never
  blank.

## Testing / verification

- Dashboard side: confirm the render branch produces a `components.html`
  block containing the firebaseConfig, the three section containers, and
  `onSnapshot` calls for `indices`, `sectors`, `broad_indices`; confirm the
  page is reachable via NAV and mobile nav derives it.
- ticker.py: dry-run broad-index symbol resolution against
  `kite.instruments("NSE")` to confirm all 7 resolve to tokens; confirm the
  flush builds `broad_indices` updates only when market open.
- Manual: with ticker running during market hours, the tiles tick; with it
  stopped, values freeze and the badge reads Closed.

## Files touched

- `dashboard.py` — NAV entry + "Live Market" render branch.
- `dashboard/ui.py` — optional: a `render_live_market(config)` helper that
  returns/embeds the component HTML (keeps dashboard.py thin).
- `random/scripts/ticker.py` — broad-index resolution + flush.
- `random/scripts/seed.js` — seed `broad_indices`.
- `.streamlit/secrets.toml` (or Streamlit Cloud secrets) — optional
  `FIREBASE_WEB_CONFIG`.

## Risks / open points

- **Depends on ticker.py being up** during market hours (user's PC). Off-hours
  or if down, the page shows frozen/last values — acceptable and explicit.
- **Cross-project coupling**: the dashboard now depends on the StockPulse
  Firestore project. The `random/` folder is a separate git repo nested in
  Data Bot; ticker/seed edits are committed there, dashboard edits here.
- **Firebase web config** is committed/used client-side. Safe by design, but
  noted explicitly.
- This is a v1 to view and iterate on; layout/coverage refinements expected
  after the user sees it live.
