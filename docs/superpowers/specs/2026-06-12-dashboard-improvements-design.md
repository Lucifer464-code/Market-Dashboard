# Dashboard Improvements — Design

Date: 2026-06-12
Status: Approved

## Context

The IFPL Market Dashboard is a Streamlit app (`dashboard.py` + `dashboard/`
package) that reads ~20 sections from a Google Sheet and renders them as
sortable HTML tables. This spec covers four targeted, independent
improvements identified during a codebase review. No behavioural change to
the data pipeline (`database.py`, `stocks_data.py`) — dashboard layer only.

## Scope

Four changes, all in the dashboard layer:

1. Resilient sheet loads (retry-once, then inline notice)
2. Delete dead `load_last_updated()`
3. Single-source navigation (eliminate `NAV` / `_NAV_GROUPS` drift)
4. In-browser search filter on large tables

Explicitly **out of scope**: CSV export (not wanted), per-section stale
badges, render-branch consolidation into a registry (kept as explicit
`elif`s), any change to data fetching/writing.

## 1. Resilient sheet loads

**Problem:** Every render branch calls `data.load_*()` with no error
handling. If one sheet is mid-update, renamed, or the Sheets API hiccups,
the whole page throws a Streamlit exception.

**Design:** A `safe_load(fn, *args)` helper in `dashboard.py` that:
- calls `fn(*args)`, retrying **once** on any exception;
- on final failure returns a sentinel `None`;
- leaves the existing `load_*()` functions in `data.py` untouched.

Render branches call `safe_load(data.load_x)` instead of `data.load_x()`.
When the result is `None`, the branch renders a distinct inline notice via
a new `ui.load_error()` helper:

> "Couldn't load this section. Try **Refresh Data** in the sidebar."

This is visually distinct from the existing `st.info("No data available.")`
empty-state in `render_table`, so a real outage is not mistaken for an
empty table.

For branches that unpack tuples (e.g. `load_global_indices` returns
`(t1, t2)`; `load_gl_us` returns `(gainers, losers)`), `safe_load` returns
`None` on failure and the branch guards with `if result is None: ...`
before unpacking.

**Why at the call boundary, not in each loader:** keeps the 20+ loaders as
single-purpose data functions; centralises retry/notice policy in one place.

## 2. Delete `load_last_updated()`

`dashboard/data.py::load_last_updated()` is never called and returns
`datetime.now()` at cache-population time (view time), which is misleading.
Remove the function. Per-section "Price as on" headers (read from sheet
A1) already convey freshness.

## 3. Single-source navigation

**Problem:** `NAV` (dashboard.py, `group -> [(label, key)]`, drives sidebar
buttons and routing) and `_NAV_GROUPS` (ui.py, `group -> [label]`, drives
mobile dropdown) are maintained by hand and have drifted when sections were
added.

**Design:** `NAV` in `dashboard.py` is the single source of truth.
- `ui.mobile_nav()` currently reads its own module-level `_NAV_GROUPS`.
  Change it to accept the nav structure as a parameter.
- `dashboard.py` derives the mobile structure from `NAV`
  (`{group: [label for label, key in items]}`) and passes it to
  `ui.mobile_nav(current_label, nav_groups)`.
- Remove `_NAV_GROUPS` from `ui.py`.

Note: mobile nav matches the **label** (button text), and `mnSelect` clicks
the sidebar button by matching `innerText` to the label — so the derived
structure must use labels, not keys. Routing in `dashboard.py` keys off
`st.session_state.section` (the `key`), unchanged.

Render branches stay as explicit `elif section == ...` blocks.

## 4. In-browser search filter on large tables

**Problem:** Large tables (up to ~200 rows) have no filter. Only the Indian
Investors page has any filtering.

**Design:** `ui.render_table()` gains an optional `searchable: bool = False`
parameter. When `True`, the HTML component includes a search `<input>` above
the table that filters rows **client-side via JS**:
- case-insensitive, matches if any cell's text contains the query;
- instant (no Streamlit rerun);
- self-contained in the component's `<script>` (no Python state).

Applied (`searchable=True`) to the large tables:
- ATH US, ATH India
- ETFs US, ETFs India
- Mutual Funds India
- Leveraged Funds

Small tables (sectors, indices, G&L, crypto, hedge funds) keep
`searchable=False` (default) — unchanged.

No CSV export.

**Interaction with existing sort:** the JS sort operates on `tbody` rows;
the filter toggles row `display`. Sorting re-orders all rows (including
hidden ones); the filter re-applies on input. They must not conflict — the
filter sets `style.display`, sort only reorders, so re-running either
preserves the other's effect. The filter re-reads current rows on each
keystroke.

## Files touched

- `dashboard/data.py` — delete `load_last_updated()`.
- `dashboard/ui.py` — `mobile_nav()` takes nav param; remove `_NAV_GROUPS`;
  `render_table()` gains `searchable`; add `load_error()` helper.
- `dashboard.py` — add `safe_load()`; wrap loads + error notices; derive
  mobile nav from `NAV`; pass `searchable=True` on the six large tables.

## Testing / verification

- `python -c "import ast; ast.parse(...)"` on all three files.
- Streamlit can't be headlessly asserted here easily; verify by:
  - confirming `NAV`-derived mobile structure equals the old `_NAV_GROUPS`
    contents (no nav regression);
  - simulating a loader exception to confirm `safe_load` returns `None` and
    the branch shows the notice;
  - rendering a `searchable=True` table and confirming the produced HTML
    contains the search input + filter script.

## Risks

- Mobile nav label/key mismatch: mitigated by deriving from labels and
  keeping `mnSelect` label-matching.
- JS filter vs. sort interaction: low risk; both manipulate the same
  `tbody` independently. Verify in the generated HTML.
