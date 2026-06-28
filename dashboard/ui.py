"""Shared UI components."""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

GREEN = "#34a853"
RED   = "#ea4335"


def _pct_style(val) -> str:
    """Return CSS color rule for a percentage cell value."""
    try:
        num = float(str(val).replace("%", "").replace("+", "").strip())
        if num > 0:
            return f"color: {GREEN}"
        if num < 0:
            return f"color: {RED}"
    except (ValueError, TypeError):
        pass
    return ""


_RETURN_KEYWORDS = {"change", "return", "chg", "pct", "gain", "perf", "1d", "2d", "3d", "4d", "5d", "1w", "1m", "3m", "6m", "ytd", "1y", "3y", "5y"}


def _pct_cols(df: pd.DataFrame) -> list[str]:
    """Return columns that represent price returns — % in name, % in values, or return-like keyword in name."""
    result = []
    for c in df.columns:
        col_lower = str(c).lower()
        if "%" in str(c):
            result.append(c)
        elif any(kw in col_lower for kw in _RETURN_KEYWORDS):
            result.append(c)
        elif df[c].astype(str).str.contains("%").any():
            result.append(c)
    return result


def _sign_color(val) -> str:
    """Return green or red hex based on sign of val."""
    try:
        num = float(str(val).replace("%", "").replace("+", "").strip())
        if num > 0:
            return GREEN
        if num < 0:
            return RED
    except (ValueError, TypeError):
        pass
    return "#64748b"


def _stat_card_html(label: str, value: str, change: str | None, border_color: str = "#e2e8f0") -> str:
    if change is not None:
        color = _sign_color(change)
        arrow = "▲" if color == GREEN else ("▼" if color == RED else "")
        change_part = f'<div style="font-size:11px;font-weight:600;color:{color};margin-top:3px">{arrow} {change}</div>'
    else:
        change_part = ""
    return (
        f'<div style="background:white;border:1px solid {border_color};border-radius:10px;padding:14px 16px;min-width:0">'
        f'<div style="font-size:10px;color:#94a3b8;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px">{label}</div>'
        f'<div style="font-size:20px;font-weight:700;color:#0f172a">{value}</div>'
        f'{change_part}'
        f'</div>'
    )


def render_stat_cards(df: pd.DataFrame, secondary_df: pd.DataFrame | None = None):
    """Render top-4 stat cards above a table. Optionally split 2+2 from two DataFrames."""
    if df.empty:
        return

    def _is_numeric(val) -> bool:
        try:
            float(str(val).replace("%", "").replace("+", "").replace(",", "").strip())
            return True
        except (ValueError, TypeError):
            return False

    def _fmt_card(val) -> str:
        s = str(val)
        try:
            f = float(s.replace("%", "").replace("+", "").replace(",", "").strip())
            return f"{f:.2f}"
        except (ValueError, TypeError):
            return s

    def _get_rows(src: pd.DataFrame, n: int) -> list[tuple]:
        # Find first two numeric columns (skip text columns like Country)
        numeric_cols = [c for c in src.columns[1:] if src[c].apply(_is_numeric).mean() > 0.5]
        val_col   = numeric_cols[0] if len(numeric_cols) > 0 else (src.columns[1] if len(src.columns) > 1 else None)
        chg_col   = numeric_cols[2] if len(numeric_cols) > 2 else (numeric_cols[1] if len(numeric_cols) > 1 else None)
        rows = []
        for _, row in src.head(n).iterrows():
            label  = str(row.iloc[0])
            value  = _fmt_card(row[val_col]) if val_col else ""
            change = _fmt_card(row[chg_col]) if chg_col else None
            rows.append((label, value, change))
        return rows

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
  .ifpl-stat-grid {{ grid-template-columns: repeat(2, 1fr); gap: 6px; }}
  .ifpl-stat-grid > div {{ padding: 10px 12px !important; }}
  .ifpl-stat-grid > div > div:first-child {{ font-size: 9px !important; margin-bottom: 4px !important; }}
  .ifpl-stat-grid > div > div:nth-child(2) {{ font-size: 16px !important; }}
  .ifpl-stat-grid > div > div:nth-child(3) {{ font-size: 10px !important; }}
}}
</style>
<div class="ifpl-stat-grid">{cards_html}</div>
""", unsafe_allow_html=True)


def render_table(df: pd.DataFrame, height: int | None = None, bold_first_col: bool = True,
                 fixed_height: bool = False, cell_color_map: dict | None = None,
                 searchable: bool = False):
    """Render a sortable HTML table with red centered headers.
    Columns named "__link__<header>" are treated as hyperlink sidecars for
    the column named <header>; they are not displayed, but their values
    wrap the corresponding cell content in an <a> tag.
    searchable=True adds an in-browser search box that filters rows
    client-side (case-insensitive, matches any cell; no Streamlit rerun).
    """
    if df.empty:
        st.info("No data available.")
        return

    # Map visible col → sidecar col for hyperlink lookup (__link__<header>)
    link_prefix = "__link__"
    link_for    = {c[len(link_prefix):]: c for c in df.columns if c.startswith(link_prefix)}
    visible_cols = [c for c in df.columns if not c.startswith(link_prefix)]

    pct_cols = set(_pct_cols(df[visible_cols]))

    def _is_numeric_col(col):
        def _ok(v):
            try:
                float(str(v).replace("%", "").replace("+", "").replace(",", "").strip())
                return True
            except (ValueError, TypeError):
                return False
        return df[col].apply(_ok).mean() > 0.5

    numeric_cols = {col for col in visible_cols if _is_numeric_col(col)}

    header_cells = "".join(
        f'<th onclick="sortTable({i})" data-col="{i}">'
        f'{col}<span class="sort-icon">⇅</span></th>'
        for i, col in enumerate(visible_cols)
    )

    def _fmt(val):
        """Format numeric values to exactly 2 decimal places; leave others unchanged."""
        try:
            s = str(val).replace("%", "").replace("+", "").replace(",", "").strip()
            if s in ("", "NA", "N/A", "-"):
                return val
            f = float(s)
            return f"{f:.2f}"
        except (ValueError, TypeError):
            return val

    color_map = cell_color_map or {}

    rows_html = ""
    for _, row in df.iterrows():
        cells = ""
        for j, col in enumerate(visible_cols):
            val = row[col]
            display = _fmt(val) if col in numeric_cols else val
            align = "center" if col in numeric_cols else "left"
            extra = _pct_style(val) if col in pct_cols else ""
            if col in color_map:
                mapped = color_map[col].get(str(val).strip())
                if mapped:
                    extra = f"color:{mapped};font-weight:600;"
            bold = "font-weight:700;" if (j == 0 and bold_first_col) else ""
            sidecar = link_for.get(col)
            link = row[sidecar] if sidecar is not None else None
            if link:
                display = (
                    f'<a href="{link}" target="_blank" rel="noopener noreferrer" '
                    f'style="color:inherit;text-decoration:underline;'
                    f'text-decoration-color:#94a3b8;text-underline-offset:2px">{display}</a>'
                )
            cells += f'<td style="text-align:{align};{bold}{extra}">{display}</td>'
        rows_html += f"<tr>{cells}</tr>"

    row_count = len(df)
    frame_height = height if height else min(42 + row_count * 37 + 20, 600)

    # Search box (client-side filter) sits above the scroll area; reserve
    # vertical space for it so the table itself isn't clipped.
    search_box_h = 44 if searchable else 0
    search_html = (
        '<input id="tsearch" type="text" placeholder="Search…" '
        'oninput="filterTable()" autocomplete="off" '
        'style="width:100%;box-sizing:border-box;padding:8px 12px;margin-bottom:8px;'
        'border:1px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none" />'
        if searchable else ""
    )

    html = f"""
<!DOCTYPE html><html><head><style>
  body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; font-size:13px; }}
  table {{ width:100%; border-collapse:collapse; }}
  thead th {{
    background:#0f172a; color:white; text-align:center;
    padding:9px 14px; font-weight:600; white-space:nowrap;
    border:1px solid #1e293b; cursor:pointer; user-select:none;
    position:sticky; top:0; z-index:1;
  }}
  thead th:hover {{ background:#1e293b; }}
  .sort-icon {{ margin-left:6px; opacity:0.8; font-size:11px; }}
  td {{ padding:8px 14px; border:1px solid #e2e8f0; white-space:nowrap; }}
  tbody tr:nth-child(odd) td {{ background:#f8fafc; }}
  tbody tr:nth-child(even) td {{ background:#ffffff; }}
  tr:hover td {{ background:#f1f5f9 !important; }}
  td:first-child {{ position:sticky; left:0; z-index:1; }}
  tbody tr:nth-child(odd) td:first-child {{ background:#f8fafc; }}
  tbody tr:nth-child(even) td:first-child {{ background:#ffffff; }}
  tr:hover td:first-child {{ background:#f1f5f9 !important; }}
  th:first-child {{ position:sticky; left:0; z-index:2; }}
  .scroll-wrap {{ overflow:auto; {"height" if fixed_height else "max-height"}:{frame_height}px;
                  max-width:100%; box-sizing:border-box;
                  border:1px solid #e2e8f0; border-radius:8px;
                  -webkit-overflow-scrolling:touch; }}
  @media (max-width: 768px) {{
    body {{ font-size:11px; }}
    thead th {{ padding:7px 8px; font-size:11px; }}
    td {{ padding:6px 8px; font-size:11px; }}
    .sort-icon {{ margin-left:3px; font-size:9px; }}
  }}
</style></head><body>
{search_html}
<div class="scroll-wrap">
  <table id="t">
    <thead><tr>{header_cells}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
<script>
  function filterTable() {{
    var q = (document.getElementById('tsearch').value || '').toLowerCase();
    var tb = document.getElementById('t').tBodies[0];
    Array.from(tb.rows).forEach(function(r) {{
      r.style.display = r.innerText.toLowerCase().indexOf(q) > -1 ? '' : 'none';
    }});
  }}
  var dir = {{}};
  function sortTable(col) {{
    var tb = document.getElementById('t').tBodies[0];
    var rows = Array.from(tb.rows);
    var asc = dir[col] = !dir[col];
    function parseVal(v) {{
      v = v.replace(/[%+,\\s$]/g, '');
      var m = v.match(/^(-?[\\d.]+)([KkMmBb]?)$/);
      if (!m) return NaN;
      var n = parseFloat(m[1]);
      var s = m[2].toUpperCase();
      if (s === 'K') n *= 1e3;
      else if (s === 'M') n *= 1e6;
      else if (s === 'B') n *= 1e9;
      return n;
    }}
    rows.sort(function(a, b) {{
      var av = a.cells[col].innerText;
      var bv = b.cells[col].innerText;
      var an = parseVal(av), bn = parseVal(bv);
      if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
      return asc ? av.localeCompare(bv) : bv.localeCompare(av);
    }});
    rows.forEach(function(r) {{ tb.appendChild(r); }});
    document.querySelectorAll('.sort-icon').forEach(function(s) {{ s.textContent = '⇅'; }});
    document.querySelector('th[data-col="'+col+'"] .sort-icon').textContent = asc ? '▲' : '▼';
  }}
</script>
</body></html>
"""
    components.html(html, height=frame_height + 10 + search_box_h, scrolling=False)


def _heat_color(val: float, lo: float, hi: float) -> tuple[str, str]:
    """Map a return value to (background, text) hex colors.
    Positive values scale 0..hi toward deep green; negative 0..lo toward
    deep red; near-zero stays pale. Intensity is relative to the lo/hi of
    the values currently in view. Returns (bg, fg)."""
    # Pale endpoints -> saturated endpoints
    GREEN_LIGHT, GREEN_DEEP = (0xE9, 0xF7, 0xEE), (0x18, 0x7A, 0x3B)
    RED_LIGHT,   RED_DEEP   = (0xFD, 0xEC, 0xEC), (0xB3, 0x26, 0x1A)
    NEUTRAL = (0xF1, 0xF5, 0xF9)

    def _lerp(a, b, t):
        return tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))

    try:
        v = float(val)
    except (ValueError, TypeError):
        r, g, b = NEUTRAL
        return f"#{r:02x}{g:02x}{b:02x}", "#0f172a"

    if v > 0 and hi > 0:
        t = min(v / hi, 1.0)
        bg = _lerp(GREEN_LIGHT, GREEN_DEEP, t)
    elif v < 0 and lo < 0:
        t = min(v / lo, 1.0)   # lo is negative, v is negative -> 0..1
        bg = _lerp(RED_LIGHT, RED_DEEP, t)
    else:
        bg = NEUTRAL
        t = 0.0

    # White text once the tile is dark enough for contrast
    fg = "#ffffff" if t > 0.55 else "#0f172a"
    r, g, b = bg
    return f"#{r:02x}{g:02x}{b:02x}", fg


def resolve_period_col(df: pd.DataFrame, period: str) -> str | None:
    """Find the df column matching a period label like '5D', tolerant of
    '%'/'↓' suffixes and whitespace used in the sheets. Returns the column
    name or None if no match."""
    target = period.strip().lower()
    for c in df.columns:
        norm = str(c).lower().replace("%", "").replace("↓", "").strip()
        if norm == target:
            return c
    return None


def render_heatmap(df: pd.DataFrame, name_col: str, period_col: str,
                   link_col: str | None = None):
    """Render sectors as a colored CSS-grid heatmap.
    name_col   : column holding the sector/tile label.
    period_col : column whose numeric value drives tile color + is shown.
    link_col   : optional "__link__<header>" sidecar; if present, tiles link.
    Color intensity is scaled relative to the min/max of period_col in view.
    """
    if df.empty:
        st.info("No data available.")
        return
    if not period_col or period_col not in df.columns:
        st.info("No data available for the selected period.")
        return

    def _num(v):
        try:
            return float(str(v).replace("%", "").replace("+", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    vals = [_num(v) for v in df[period_col]]
    nums = [v for v in vals if v is not None]
    if not nums:
        st.info("No data available.")
        return
    lo, hi = min(nums), max(nums)

    tiles = ""
    for (_, row), v in zip(df.iterrows(), vals):
        name = str(row[name_col])
        if v is None:
            bg, fg, label = "#f1f5f9", "#0f172a", "NA"
        else:
            bg, fg = _heat_color(v, lo, hi)
            label = f"{v:+.2f}%"
        link = row[link_col] if (link_col and link_col in df.columns) else None
        inner = (
            f'<div style="font-size:13px;font-weight:600;line-height:1.2;'
            f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{name}</div>'
            f'<div style="font-size:18px;font-weight:700;margin-top:6px">{label}</div>'
        )
        if link:
            inner = (f'<a href="{link}" target="_blank" rel="noopener noreferrer" '
                     f'style="color:inherit;text-decoration:none">{inner}</a>')
        tiles += (
            f'<div style="background:{bg};color:{fg};border-radius:10px;'
            f'padding:16px 14px;min-height:78px;display:flex;flex-direction:column;'
            f'justify-content:center">{inner}</div>'
        )

    rows = (len(df) + 3) // 4   # ~4 tiles per row on desktop
    frame_height = 40 + rows * 100

    html = f"""
<!DOCTYPE html><html><head><style>
  body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; }}
  .heat-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(170px,1fr)); gap:8px; }}
  @media (max-width:768px) {{
    .heat-grid {{ grid-template-columns:repeat(2,1fr); gap:6px; }}
  }}
</style></head><body>
<div class="heat-grid">{tiles}</div>
</body></html>
"""
    components.html(html, height=frame_height, scrolling=False)


def render_live_market(firebase_config: dict):
    """Embed a client-side Firestore live-market widget.

    The user's browser connects directly to the StockPulse Firestore project
    via the Firebase web SDK and onSnapshot (push updates). Streamlit only
    hosts the HTML — no Firebase Python SDK, no service-account secret.

    firebase_config: the Firebase WEB config dict (client-safe; Firestore
    rules are read-only). Renders three live sections: headline indices,
    NIFTY sectoral indices, and broad NIFTY indices.
    """
    import json as _json
    cfg = _json.dumps(firebase_config)

    html = f"""
<!DOCTYPE html><html><head><style>
  :root {{ --green:#34a853; --red:#ea4335; --ink:#0f172a; --muted:#94a3b8; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
          color:var(--ink); background:#f8fafc; }}
  .lm-bar {{ display:flex; align-items:center; justify-content:space-between;
             margin-bottom:14px; }}
  .lm-badge {{ display:inline-flex; align-items:center; gap:7px; font-size:12px;
               font-weight:600; padding:6px 12px; border-radius:999px;
               background:#dcfce7; color:#166534; }}
  .lm-badge .dot {{ width:8px; height:8px; border-radius:50%; background:#22c55e;
                    animation:pulse 1.6s infinite; }}
  .lm-badge.closed {{ background:#f1f5f9; color:#64748b; }}
  .lm-badge.closed .dot {{ background:#94a3b8; animation:none; }}
  @keyframes pulse {{ 0%{{opacity:1}} 50%{{opacity:.35}} 100%{{opacity:1}} }}
  @media (prefers-reduced-motion: reduce) {{ .lm-badge .dot {{ animation:none; }} }}
  .lm-clock {{ font-size:12px; color:var(--muted); font-variant-numeric:tabular-nums; }}
  .lm-note {{ font-size:11px; color:var(--muted); margin:-6px 0 14px; }}
  .lm-section-title {{ font-size:13px; font-weight:700; color:var(--ink);
                       margin:18px 0 8px; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:8px; }}
  .kpi {{ background:white; border:1px solid #e2e8f0; border-radius:10px;
          padding:14px 16px; }}
  .kpi-label {{ font-size:10px; color:var(--muted); font-weight:600;
                text-transform:uppercase; letter-spacing:.8px; }}
  .kpi-val {{ font-size:22px; font-weight:700; margin-top:6px;
              font-variant-numeric:tabular-nums; }}
  .kpi-chg {{ font-size:12px; font-weight:600; margin-top:3px; }}
  .kpi-pts {{ font-size:11px; color:var(--muted); margin-top:2px; }}
  .up {{ color:var(--green); }} .down {{ color:var(--red); }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(220px,1fr));
           gap:8px; }}
  .row {{ background:white; border:1px solid #e2e8f0; border-radius:10px;
          padding:12px 14px; display:flex; align-items:center; gap:10px; }}
  .row .nm {{ font-size:13px; font-weight:600; flex:1; overflow:hidden;
              text-overflow:ellipsis; white-space:nowrap; }}
  .row .vl {{ font-size:13px; font-variant-numeric:tabular-nums; color:#334155; }}
  .row .pc {{ font-size:13px; font-weight:700; min-width:64px; text-align:right;
              font-variant-numeric:tabular-nums; }}
  .empty {{ color:var(--muted); font-size:12px; padding:10px 0; }}
  @media (max-width:768px) {{
    .kpi-grid {{ grid-template-columns:repeat(2,1fr); }}
    .grid {{ grid-template-columns:1fr; }}
  }}
</style></head><body>

<div class="lm-bar">
  <div class="lm-badge" id="badge"><span class="dot"></span><span id="badge-tx">NSE Open</span></div>
  <div class="lm-clock" id="clock">--:--:-- IST</div>
</div>
<div class="lm-note" id="note" style="display:none">Showing closing prices · as of 15:30 IST</div>

<div class="lm-section-title">Headline Indices</div>
<div class="kpi-grid" id="kpis">
  <div class="kpi" data-id="nifty50"><div class="kpi-label">NIFTY 50</div><div class="kpi-val" data-val>—</div><div class="kpi-chg" data-chg>—</div><div class="kpi-pts" data-pts></div></div>
  <div class="kpi" data-id="sensex"><div class="kpi-label">SENSEX</div><div class="kpi-val" data-val>—</div><div class="kpi-chg" data-chg>—</div><div class="kpi-pts" data-pts></div></div>
  <div class="kpi" data-id="banknifty"><div class="kpi-label">BANK NIFTY</div><div class="kpi-val" data-val>—</div><div class="kpi-chg" data-chg>—</div><div class="kpi-pts" data-pts></div></div>
  <div class="kpi" data-id="vix"><div class="kpi-label">VIX</div><div class="kpi-val" data-val>—</div><div class="kpi-chg" data-chg>—</div><div class="kpi-pts" data-pts></div></div>
</div>

<div class="lm-section-title">NIFTY Sectoral Indices</div>
<div class="grid" id="sectors"><div class="empty">Connecting…</div></div>

<div class="lm-section-title">Broad NIFTY Indices</div>
<div class="grid" id="broad"><div class="empty">Connecting…</div></div>

<script src="https://www.gstatic.com/firebasejs/10.8.0/firebase-app-compat.js"></script>
<script src="https://www.gstatic.com/firebasejs/10.8.0/firebase-firestore-compat.js"></script>
<script>
const firebaseConfig = {cfg};

// ---- formatting ----
function fmtPrice(v) {{ return Number(v).toLocaleString('en-IN',{{minimumFractionDigits:2,maximumFractionDigits:2}}); }}
function fmtPct(p) {{ const s=p>=0?'▲ +':'▼ −'; return s+Math.abs(p).toFixed(2)+'%'; }}
function barW(p) {{ return Math.min(Math.abs(p)*33.33,100).toFixed(1)+'%'; }}

// ---- market status (mirrors StockPulse) ----
const NSE_HOLIDAYS=new Set(['2026-01-26','2026-02-16','2026-03-04','2026-03-21','2026-03-31','2026-04-01','2026-04-03','2026-04-14','2026-05-01','2026-05-27','2026-07-06','2026-08-15','2026-08-28','2026-10-02','2026-10-21','2026-11-09','2026-11-24','2026-12-25']);
function istKey(d){{return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');}}
function marketStatus(ist){{
  const day=ist.getDay(); if(day===0||day===6) return 'closed';
  if(NSE_HOLIDAYS.has(istKey(ist))) return 'closed';
  const m=ist.getHours()*60+ist.getMinutes(); const o=9*60+15, c=15*60+30;
  if(m<o||m>c) return 'closed'; if(m===c&&ist.getSeconds()>0) return 'closed';
  return 'open';
}}
function tick(){{
  const ist=new Date(new Date().toLocaleString('en-US',{{timeZone:'Asia/Kolkata'}}));
  document.getElementById('clock').textContent=
    String(ist.getHours()).padStart(2,'0')+':'+String(ist.getMinutes()).padStart(2,'0')+':'+String(ist.getSeconds()).padStart(2,'0')+' IST';
  const st=marketStatus(ist), closed=st==='closed';
  const b=document.getElementById('badge'); b.classList.toggle('closed',closed);
  document.getElementById('badge-tx').textContent=closed?'NSE Closed':'NSE Open';
  document.getElementById('note').style.display=closed?'block':'none';
}}
setInterval(tick,1000); tick();

// ---- render helpers ----
function setKpi(id,d){{
  const c=document.querySelector('.kpi[data-id="'+id+'"]'); if(!c) return;
  c.querySelector('[data-val]').textContent=fmtPrice(d.value);
  const ch=c.querySelector('[data-chg]'); ch.textContent=fmtPct(d.change_pct);
  ch.className='kpi-chg '+(d.change_pct>=0?'up':'down');
  const pts=c.querySelector('[data-pts]');
  if(!d.is_vix && d.change_pts!=null){{ const s=d.change_pts>=0?'+':'−'; pts.textContent=s+Math.abs(d.change_pts).toFixed(2)+' pts'; }}
}}
function renderRows(containerId,docs){{
  const el=document.getElementById(containerId);
  if(!docs.length){{ el.innerHTML='<div class="empty">No data available.</div>'; return; }}
  docs.sort((a,b)=>(a.sort_order||0)-(b.sort_order||0));
  el.innerHTML=docs.map(s=>{{
    const p=s.change_pct||0, cls=p>=0?'up':'down', sign=p>=0?'+':'−';
    const val=s.value!=null?fmtPrice(s.value):'—';
    return '<div class="row"><div class="nm">'+s.name+'</div><div class="vl">'+val+'</div>'+
           '<div class="pc '+cls+'">'+sign+Math.abs(p).toFixed(2)+'%</div></div>';
  }}).join('');
}}

// ---- firestore live ----
try {{
  firebase.initializeApp(firebaseConfig);
  const db=firebase.firestore();
  ['nifty50','sensex','banknifty','vix'].forEach(id=>{{
    db.collection('indices').doc(id).onSnapshot(
      doc=>{{ if(doc.exists) setKpi(id,doc.data()); }},
      err=>console.warn('indices/'+id,err));
  }});
  db.collection('sectors').onSnapshot(
    snap=>renderRows('sectors',snap.docs.map(d=>d.data())),
    err=>{{ console.warn('sectors',err); document.getElementById('sectors').innerHTML='<div class="empty">Live feed unavailable.</div>'; }});
  db.collection('broad_indices').onSnapshot(
    snap=>renderRows('broad',snap.docs.map(d=>d.data())),
    err=>{{ console.warn('broad_indices',err); document.getElementById('broad').innerHTML='<div class="empty">Live feed unavailable.</div>'; }});
}} catch(e) {{
  console.error('Firebase init failed',e);
  document.getElementById('sectors').innerHTML='<div class="empty">Live feed unavailable.</div>';
  document.getElementById('broad').innerHTML='<div class="empty">Live feed unavailable.</div>';
}}
</script>
</body></html>
"""
    components.html(html, height=720, scrolling=True)


def section_header(title: str, subtitle: str = "", price_as_of: str = "", updated_at: str = ""):
    if price_as_of:
        top_right = price_as_of
    else:
        yesterday  = datetime.now() - timedelta(days=1)
        top_right  = f"Price as on {yesterday.strftime('%b')} {yesterday.day}, {yesterday.year}"
    updated_html = (
        f"<div style='font-size:10px;color:#94a3b8;margin-top:2px;text-align:right'>Updated {updated_at}</div>"
        if updated_at else ""
    )
    sub_html = f"<div style='font-size:12px;color:#94a3b8;margin-top:2px'>{subtitle}</div>" if subtitle else ""
    st.markdown(
        f"""<style>
.ifpl-hdr {{ padding:14px 0;border-bottom:1px solid #e2e8f0;margin-bottom:16px;display:flex;justify-content:space-between;align-items:flex-start;gap:8px; }}
.ifpl-hdr-right {{ text-align:right;flex-shrink:0; }}
@media (max-width: 768px) {{
  .ifpl-hdr {{ flex-direction:column;gap:4px; }}
  .ifpl-hdr-right {{ text-align:left; }}
  .ifpl-hdr-title {{ font-size:16px !important; }}
  .ifpl-hdr-sub {{ font-size:11px !important; }}
  .ifpl-hdr-price {{ font-size:11px !important; }}
}}
</style>
<div class="ifpl-hdr">
  <div>
    <div class="ifpl-hdr-title" style="font-size:18px;font-weight:700;color:#0f172a">{title}</div>
    {f'<div class="ifpl-hdr-sub" style="font-size:12px;color:#94a3b8;margin-top:2px">{subtitle}</div>' if subtitle else ''}
  </div>
  <div class="ifpl-hdr-right">
    <span class="ifpl-hdr-price" style="font-size:12px;color:#64748b;font-weight:500">{top_right}</span>
    {updated_html}
  </div>
</div>""",
        unsafe_allow_html=True,
    )


def load_error():
    """Inline notice shown when a section's sheet load fails (after retry).
    Visually distinct from the 'No data available.' empty state so a real
    outage is not mistaken for an empty table."""
    st.markdown(
        "<div style='background:#fef2f2;border:1px solid #fecaca;border-radius:8px;"
        "padding:14px 16px;color:#b91c1c;font-size:13px'>"
        "Couldn't load this section. Try <b>Refresh Data</b> in the sidebar."
        "</div>",
        unsafe_allow_html=True,
    )


def sort_by_keyword(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    """Return df sorted descending by first column whose name contains keyword."""
    for col in df.columns:
        if keyword.lower() in col.lower():
            try:
                numeric = pd.to_numeric(
                    df[col].astype(str)
                        .str.replace("%", "", regex=False)
                        .str.replace("+", "", regex=False)
                        .str.strip(),
                    errors="coerce",
                )
                return df.iloc[numeric.sort_values(ascending=False, na_position="last").index].reset_index(drop=True)
            except Exception:
                pass
    return df


def secondary_label(text: str):
    st.markdown(
        f"<p style='font-size:13px;font-weight:600;color:#0f172a;margin:16px 0 8px'>{text}</p>",
        unsafe_allow_html=True,
    )


def mobile_nav(current_section: str, nav_groups: dict):
    """Trigger bar in iframe; dropdown injected into parent page DOM as
    position:fixed so it floats above all Streamlit content.

    nav_groups: {group_label: [section_label, ...]} — derived from the
    single-source NAV in dashboard.py so the mobile dropdown can't drift
    from the sidebar."""
    import json as _json
    nav_data = [{"group": g, "sections": s} for g, s in nav_groups.items()]

    components.html(f"""<!DOCTYPE html><html><head><style>
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:transparent; }}
.mn-bar {{
  background:white; border:1px solid #e2e8f0; border-radius:10px;
  padding:12px 16px; display:flex; align-items:center;
  justify-content:space-between; cursor:pointer; user-select:none;
}}
.mn-title {{ font-size:16px; font-weight:700; color:#0f172a; }}
.mn-chevron {{ font-size:12px; color:#64748b; margin-left:6px;
               display:inline-block; transition:transform 0.2s; }}
.mn-chevron.open {{ transform:rotate(180deg); }}
</style></head><body>
<div class="mn-bar" onclick="mnToggle()">
  <span class="mn-title">{current_section}</span>
  <span class="mn-chevron" id="ch">▾</span>
</div>
<script>
var NAV     = {_json.dumps(nav_data)};
var CURRENT = {_json.dumps(current_section)};
var isOpen  = false;

// Hide on desktop — display:none removes it from flex layout entirely (no gap)
(function() {{
  var w = window.parent ? window.parent.innerWidth : window.innerWidth;
  if (w > 768 && window.frameElement) {{
    var p = window.frameElement.parentElement;  // stCustomComponentV1
    if (p) p.style.display = 'none';
  }}
}})();

function mnClose() {{
  var pd = window.parent.document;
  var d  = pd.getElementById('mnDd'); if (d) d.remove();
  var o  = pd.getElementById('mnOv'); if (o) o.remove();
  var ch = document.getElementById('ch');
  if (ch) ch.classList.remove('open');
  isOpen = false;
}}

function mnSelect(section) {{
  mnClose();
  var btns = window.parent.document.querySelectorAll('[data-testid="stSidebar"] button');
  for (var i = 0; i < btns.length; i++) {{
    if (btns[i].innerText.trim() === section) {{ btns[i].click(); return; }}
  }}
}}

function mnToggle() {{
  if (isOpen) {{ mnClose(); return; }}
  var pd   = window.parent.document;
  var rect = window.frameElement ? window.frameElement.getBoundingClientRect() : {{bottom:55,left:0,right:0}};

  // Build items HTML — use data-sec attribute, NO inline onclick
  var html = '';
  for (var i = 0; i < NAV.length; i++) {{
    html += '<div style="font-size:9px;font-weight:700;color:#475569;letter-spacing:1.5px;text-transform:uppercase;padding:10px 16px 4px">' + NAV[i].group + '</div>';
    for (var j = 0; j < NAV[i].sections.length; j++) {{
      var sec    = NAV[i].sections[j];
      var active = sec === CURRENT ? 'background:#f0f9ff;color:#0ea5e9;border-left-color:#0ea5e9;font-weight:600;' : '';
      html += '<div data-sec="' + sec.replace(/"/g,'&quot;') + '" style="padding:12px 16px;font-size:14px;color:#334155;cursor:pointer;border-left:3px solid transparent;' + active + '">' + sec + '</div>';
    }}
  }}

  // Overlay in parent
  var ov = pd.createElement('div');
  ov.id  = 'mnOv';
  ov.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;z-index:99998;';
  ov.addEventListener('click', function() {{ mnClose(); }});
  pd.body.appendChild(ov);

  // Dropdown in parent
  var dd = pd.createElement('div');
  dd.id  = 'mnDd';
  dd.style.cssText = 'position:fixed;top:' + (rect.bottom + 4) + 'px;left:8px;right:8px;' +
    'background:white;border:1px solid #e2e8f0;border-radius:10px;' +
    'box-shadow:0 8px 24px rgba(0,0,0,0.12);z-index:99999;max-height:70vh;overflow-y:auto;';
  dd.innerHTML = html;

  // Attach click listener from THIS iframe's scope — avoids cross-context issues
  dd.addEventListener('click', function(e) {{
    var t = e.target;
    while (t && t !== dd) {{
      if (t.hasAttribute('data-sec')) {{
        mnSelect(t.getAttribute('data-sec'));
        return;
      }}
      t = t.parentElement;
    }}
  }});

  pd.body.appendChild(dd);

  document.getElementById('ch').classList.add('open');
  isOpen = true;
}}
</script>
</body></html>""", height=55)
