# Mobile Responsiveness Design

**Date:** 2026-03-27
**Scope:** Make the IFPL Market Dashboard usable on mobile screens (≤768px)

---

## 1. Navigation — Horizontal Scrollable Tab Bar

### Problem
The sidebar takes up the full left side of the screen on mobile, leaving no room for content.

### Solution
- Hide the Streamlit sidebar on mobile via CSS (`@media (max-width: 768px)`), using `transform: translateX(-100%)` so the buttons remain in the DOM and are still clickable via JavaScript.
- Inject a sticky horizontal tab bar at the top of the main content area using `st.markdown` HTML, visible only on mobile (`display:none` on desktop, `display:flex` on mobile).
- The tab bar lists all section names in a horizontally scrollable row.
- Tapping a tab triggers `window.parent.document.querySelector(...)` to find and click the matching sidebar button, which fires the existing Streamlit `st.session_state` + `st.rerun()` flow. No new routing logic is introduced.
- The active tab is highlighted by matching `st.session_state.section` to the tab label at render time.

### Sections in tab bar (same as sidebar NAV dict)
MARKETS: Global Indices, NIFTY Sectors, NIFTY Indices
ASSETS: ETFs US, Leveraged Funds, ETFs India, Mutual Funds India, Crypto
STOCKS: Gainers & Losers US, Gainers & Losers India, ATH US, ATH India

---

## 2. Stat Cards — 2×2 Grid on Mobile

### Problem
`render_stat_cards` uses `st.columns(4)` which renders 4 cards in a single row. On a narrow screen these become too small to read.

### Solution
- Replace the `st.columns()` approach with a single `st.markdown` HTML block that wraps all cards in a CSS Grid container.
- Desktop (>768px): `grid-template-columns: repeat(4, 1fr)`
- Mobile (≤768px): `grid-template-columns: repeat(2, 1fr)` via `@media` query.
- The `_stat_card_html` function is unchanged — only the wrapper changes.
- The 2+2 split variant (gainers/losers pages) uses the same grid but with different border colours for the two groups, preserved as-is.

---

## 3. Tables — Proper Horizontal Scroll on Mobile

### Problem
Tables already have `overflow:auto` on the scroll wrapper, but the `components.html` iframe doesn't constrain to viewport width on mobile, causing layout overflow.

### Solution
- Add `width:100%; max-width:100%;` to the `components.html` call style (via a wrapping `<div>` in the injected HTML).
- Ensure the `.scroll-wrap` div has `max-width:100vw; box-sizing:border-box`.
- Remove any fixed minimum widths that prevent the table from fitting inside the iframe on narrow viewports.
- `white-space:nowrap` on cells is kept — horizontal scrolling within the table is the intended behaviour.

---

## Files Changed

| File | Change |
|------|--------|
| `dashboard.py` | Add mobile CSS (sidebar hide, tab bar show/hide), inject sticky tab bar HTML above main content |
| `dashboard/ui.py` | Replace `st.columns()` in `render_stat_cards` with CSS Grid HTML block; add `max-width` fix to table scroll wrapper |

---

## Out of Scope
- No changes to data loading, authentication, or update scripts.
- No server-side screen-size detection — all responsiveness is CSS/JS only.
- Desktop layout is unchanged.
