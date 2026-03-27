# Mobile Responsiveness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the IFPL Market Dashboard usable on mobile screens (≤768px) with a dropdown overlay nav, 2×2 stat cards, and horizontally scrollable tables.

**Architecture:** All changes are CSS/JS only — no new Python routing logic. On mobile, the Streamlit sidebar is pushed off-screen (but kept in the DOM so JS can still click its buttons). A dropdown nav bar injected via `st.markdown` handles mobile navigation by programmatically clicking hidden sidebar buttons. Stat cards are converted from `st.columns()` to a CSS Grid with a media query. Tables already scroll horizontally; a `max-width` fix ensures they fit mobile viewports.

**Tech Stack:** Python, Streamlit, HTML/CSS/JS (injected via `st.markdown` and `components.html`)

---

## File Map

| File | Change |
|------|--------|
| `dashboard.py` | Add mobile CSS rules to existing `<style>` block; call `ui.mobile_nav()` once before main content |
| `dashboard/ui.py` | Add `mobile_nav()` function; update `render_stat_cards()` to use CSS Grid; add `max-width` fix to table scroll wrapper; add class to `section_header` div for mobile hiding |

---

## Task 1: Mobile CSS — push sidebar off-screen on mobile

**Files:**
- Modify: `dashboard.py` (inside the existing `st.markdown` CSS block)

- [ ] **Step 1: Add mobile CSS rules to the existing style block in `dashboard.py`**

Find the closing `</style>` tag in the existing `st.markdown` CSS block and insert these rules before it:

```css
/* ── Mobile: push sidebar off-screen, keep buttons clickable ── */
@media (max-width: 768px) {
    [data-testid="stSidebar"] {
        position: fixed !important;
        left: -9999px !important;
        width: 1px !important;
        overflow: hidden !important;
    }
    /* Remove the left gap left by the hidden sidebar */
    [data-testid="stAppViewContainer"] > div:first-child {
        margin-left: 0 !important;
    }
    section[data-testid="stMain"] > div:first-child {
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }
    /* Hide section header on mobile (mobile nav bar replaces it) */
    .ifpl-section-header {
        display: none !important;
    }
}
```

- [ ] **Step 2: Verify visually**

Open the dashboard on a mobile-sized browser window (or DevTools device emulation at 375px wide). The sidebar should be gone and the main content should fill the screen. The section content should still render correctly.

- [ ] **Step 3: Commit**

```bash
git add dashboard.py
git commit -m "Add mobile CSS to hide sidebar and fix layout on small screens"
```

---

## Task 2: Mobile dropdown nav component

**Files:**
- Modify: `dashboard/ui.py` (add `mobile_nav()` function)
- Modify: `dashboard.py` (call `ui.mobile_nav()` before main content; add class to `section_header`)

- [ ] **Step 1: Add `_NAV_GROUPS` constant and `mobile_nav()` to `dashboard/ui.py`**

Add this near the top of `ui.py`, after the existing constants:

```python
_NAV_GROUPS = {
    "MARKETS": ["Global Indices", "NIFTY Sectors", "NIFTY Indices"],
    "ASSETS":  ["ETFs US", "Leveraged Funds", "ETFs India", "Mutual Funds India", "Crypto"],
    "STOCKS":  ["Gainers & Losers US", "Gainers & Losers India", "ATH US", "ATH India"],
}
```

Then add this function at the bottom of `ui.py`:

```python
def mobile_nav(current_section: str):
    """Floating dropdown nav bar — visible only on mobile (≤768px)."""
    items_html = ""
    for group, sections in _NAV_GROUPS.items():
        items_html += f'<div class="mn-group">{group}</div>'
        for sec in sections:
            active_cls = "mn-active" if sec == current_section else ""
            items_html += (
                f'<div class="mn-item {active_cls}" '
                f'data-section="{sec}" onclick="mnSelectThis(this)">'
                f'{sec}</div>'
            )

    st.markdown(f"""
<style>
.mn-wrap {{ display:none; }}
@media (max-width:768px) {{
  .mn-wrap {{ display:block; position:relative; z-index:100; margin-bottom:12px; }}
}}
.mn-bar {{
  background:white; border:1px solid #e2e8f0; border-radius:10px;
  padding:12px 16px; display:flex; align-items:center;
  justify-content:space-between; cursor:pointer; user-select:none;
}}
.mn-title {{ font-size:16px; font-weight:700; color:#0f172a; }}
.mn-chevron {{ font-size:12px; color:#64748b; display:inline-block;
               transition:transform 0.2s; margin-left:6px; }}
.mn-chevron.mn-open {{ transform:rotate(180deg); }}
.mn-overlay {{
  display:none; position:fixed; top:0; left:0; right:0; bottom:0;
  background:rgba(15,23,42,0.3); z-index:99;
}}
.mn-overlay.mn-open {{ display:block; }}
.mn-dropdown {{
  display:none; position:absolute; left:0; right:0; top:calc(100% + 4px);
  background:white; border:1px solid #e2e8f0; border-radius:10px;
  box-shadow:0 8px 24px rgba(0,0,0,0.12); z-index:100;
  max-height:70vh; overflow-y:auto;
}}
.mn-dropdown.mn-open {{ display:block; }}
.mn-group {{
  font-size:9px; font-weight:700; color:#475569;
  letter-spacing:1.5px; text-transform:uppercase; padding:10px 16px 4px;
}}
.mn-item {{
  padding:9px 16px; font-size:13px; color:#334155;
  cursor:pointer; border-left:3px solid transparent;
}}
.mn-item.mn-active {{
  background:#f0f9ff; color:#0ea5e9;
  border-left-color:#0ea5e9; font-weight:600;
}}
.mn-item:hover {{ background:#f8fafc; }}
</style>
<div class="mn-wrap">
  <div class="mn-overlay" id="mnOverlay" onclick="mnClose()"></div>
  <div class="mn-bar" onclick="mnToggle()">
    <span class="mn-title">{current_section}</span>
    <span class="mn-chevron" id="mnChevron">▾</span>
  </div>
  <div class="mn-dropdown" id="mnDropdown">
    {items_html}
  </div>
</div>
<script>
function mnToggle() {{
  document.getElementById('mnDropdown').classList.toggle('mn-open');
  document.getElementById('mnChevron').classList.toggle('mn-open');
  document.getElementById('mnOverlay').classList.toggle('mn-open');
}}
function mnClose() {{
  document.getElementById('mnDropdown').classList.remove('mn-open');
  document.getElementById('mnChevron').classList.remove('mn-open');
  document.getElementById('mnOverlay').classList.remove('mn-open');
}}
function mnSelectThis(el) {{
  mnClose();
  var section = el.getAttribute('data-section');
  var btns = document.querySelectorAll('[data-testid="stSidebar"] button');
  for (var i = 0; i < btns.length; i++) {{
    if (btns[i].innerText.trim() === section) {{
      btns[i].click();
      return;
    }}
  }}
}}
</script>
""", unsafe_allow_html=True)
```

- [ ] **Step 2: Add class `ifpl-section-header` to the section header div in `ui.py`**

In the `section_header()` function, change:

```python
        <div style="background:white;border-bottom:1px solid #e2e8f0;padding:14px 0;
                    display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
```

to:

```python
        <div class="ifpl-section-header" style="background:white;border-bottom:1px solid #e2e8f0;padding:14px 0;
                    display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
```

- [ ] **Step 3: Call `ui.mobile_nav()` in `dashboard.py` before main content**

In `dashboard.py`, add this line immediately before the `section = st.session_state.section` line:

```python
# ── Mobile nav ──────────────────────────────────────────────
ui.mobile_nav(st.session_state.section)

# ── Main content ────────────────────────────────────────────
section = st.session_state.section
```

- [ ] **Step 4: Verify visually on mobile**

In DevTools at 375px width:
- The dropdown bar should appear at the top of the page showing the current section name
- Tapping it should open a floating dropdown over the content (not pushing it down)
- Tapping a section name should navigate to that section
- Tapping outside should close the dropdown
- On desktop (>768px), the mobile nav bar should be invisible

- [ ] **Step 5: Commit**

```bash
git add dashboard/ui.py dashboard.py
git commit -m "Add mobile dropdown nav bar with floating overlay"
```

---

## Task 3: Stat cards — 2×2 grid on mobile

**Files:**
- Modify: `dashboard/ui.py` — `render_stat_cards()`

- [ ] **Step 1: Replace `st.columns()` with CSS Grid in `render_stat_cards()`**

Replace the entire `render_stat_cards` function body (the rendering section at the bottom, after `_get_rows`) with:

```python
    if secondary_df is not None and not secondary_df.empty:
        primary_rows   = _get_rows(df, 2)
        secondary_rows = _get_rows(secondary_df, 2)
        cards_html = (
            "".join(_stat_card_html(l, v, c, "#dcfce7") for l, v, c in primary_rows)
            + "".join(_stat_card_html(l, v, c, "#fef2f2") for l, v, c in secondary_rows)
        )
    else:
        rows = _get_rows(df, 4)
        cards_html = "".join(_stat_card_html(l, v, c) for l, v, c in rows)

    st.markdown(f"""
<style>
.ifpl-stat-grid {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
  margin-bottom: 16px;
}}
@media (max-width: 768px) {{
  .ifpl-stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
}}
</style>
<div class="ifpl-stat-grid">{cards_html}</div>
""", unsafe_allow_html=True)
```

- [ ] **Step 2: Verify on desktop and mobile**

- Desktop (>768px): 4 cards in a single row — same as before
- Mobile (≤768px): 4 cards in a 2×2 grid

- [ ] **Step 3: Commit**

```bash
git add dashboard/ui.py
git commit -m "Convert stat cards to CSS Grid for 2x2 layout on mobile"
```

---

## Task 4: Table scroll — fix mobile viewport overflow

**Files:**
- Modify: `dashboard/ui.py` — `render_table()`

- [ ] **Step 1: Add `max-width` and `box-sizing` to the scroll wrapper CSS in `render_table()`**

In the `html` f-string inside `render_table()`, change the `.scroll-wrap` CSS rule from:

```css
  .scroll-wrap {{ overflow:auto; max-height:{frame_height}px;
                  border:1px solid #e2e8f0; border-radius:8px; }}
```

to:

```css
  .scroll-wrap {{ overflow:auto; max-height:{frame_height}px;
                  max-width:100%; box-sizing:border-box;
                  border:1px solid #e2e8f0; border-radius:8px; }}
```

- [ ] **Step 2: Verify on mobile**

On a 375px wide screen, tables should be contained within the viewport and scroll horizontally to reveal all columns. No horizontal page overflow.

- [ ] **Step 3: Commit and push**

```bash
git add dashboard/ui.py
git commit -m "Fix table horizontal scroll overflow on mobile viewports"
git push
```
