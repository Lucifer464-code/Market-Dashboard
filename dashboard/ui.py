"""Shared UI components."""

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

    def _get_rows(src: pd.DataFrame, n: int) -> list[tuple]:
        # Find first two numeric columns (skip text columns like Country)
        numeric_cols = [c for c in src.columns[1:] if src[c].apply(_is_numeric).mean() > 0.5]
        val_col   = numeric_cols[0] if len(numeric_cols) > 0 else (src.columns[1] if len(src.columns) > 1 else None)
        chg_col   = numeric_cols[1] if len(numeric_cols) > 1 else None
        rows = []
        for _, row in src.head(n).iterrows():
            label  = str(row.iloc[0])
            value  = str(row[val_col]) if val_col else ""
            change = str(row[chg_col]) if chg_col else None
            rows.append((label, value, change))
        return rows

    if secondary_df is not None and not secondary_df.empty:
        primary_rows = _get_rows(df, 2)
        secondary_rows = _get_rows(secondary_df, 2)
        cols = st.columns(4)
        for i, (label, value, change) in enumerate(primary_rows):
            with cols[i]:
                st.markdown(_stat_card_html(label, value, change, "#dcfce7"), unsafe_allow_html=True)
        for i, (label, value, change) in enumerate(secondary_rows):
            with cols[2 + i]:
                st.markdown(_stat_card_html(label, value, change, "#fef2f2"), unsafe_allow_html=True)
    else:
        rows = _get_rows(df, 4)
        cols = st.columns(len(rows)) if rows else []
        for i, (label, value, change) in enumerate(rows):
            with cols[i]:
                st.markdown(_stat_card_html(label, value, change), unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:16px'></div>", unsafe_allow_html=True)


def render_table(df: pd.DataFrame, height: int | None = None, bold_first_col: bool = True):
    """Render a sortable HTML table with red centered headers."""
    if df.empty:
        st.info("No data available.")
        return

    pct_cols = set(_pct_cols(df))

    def _is_numeric_col(col):
        def _ok(v):
            try:
                float(str(v).replace("%", "").replace("+", "").replace(",", "").strip())
                return True
            except (ValueError, TypeError):
                return False
        return df[col].apply(_ok).mean() > 0.5

    numeric_cols = {col for col in df.columns if _is_numeric_col(col)}

    header_cells = "".join(
        f'<th onclick="sortTable({i})" data-col="{i}">'
        f'{col}<span class="sort-icon">⇅</span></th>'
        for i, col in enumerate(df.columns)
    )

    rows_html = ""
    for _, row in df.iterrows():
        cells = ""
        for j, col in enumerate(df.columns):
            val = row[col]
            align = "center" if col in numeric_cols else "left"
            extra = _pct_style(val) if col in pct_cols else ""
            bold = "font-weight:700;" if (j == 0 and bold_first_col) else ""
            cells += f'<td style="text-align:{align};{bold}{extra}">{val}</td>'
        rows_html += f"<tr>{cells}</tr>"

    row_count = len(df)
    frame_height = height if height else min(42 + row_count * 37 + 20, 600)

    html = f"""
<!DOCTYPE html><html><head><style>
  body {{ margin:0; font-family:sans-serif; font-size:13px; }}
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
  .scroll-wrap {{ overflow:auto; max-height:{frame_height}px;
                  border:1px solid #e2e8f0; border-radius:8px; }}
</style></head><body>
<div class="scroll-wrap">
  <table id="t">
    <thead><tr>{header_cells}</tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>
<script>
  var dir = {{}};
  function sortTable(col) {{
    var tb = document.getElementById('t').tBodies[0];
    var rows = Array.from(tb.rows);
    var asc = dir[col] = !dir[col];
    rows.sort(function(a, b) {{
      var av = a.cells[col].innerText.replace(/[%+,\\s]/g,'');
      var bv = b.cells[col].innerText.replace(/[%+,\\s]/g,'');
      var an = parseFloat(av), bn = parseFloat(bv);
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
    components.html(html, height=frame_height + 10, scrolling=False)


def section_header(title: str, subtitle: str = ""):
    sub_html = f"<div style='font-size:12px;color:#94a3b8;margin-top:2px'>{subtitle}</div>" if subtitle else ""
    st.markdown(
        f"""
        <div style="background:white;border-bottom:1px solid #e2e8f0;padding:14px 0;
                    display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
          <div>
            <div style="font-size:18px;font-weight:700;color:#0f172a">{title}</div>
            {sub_html}
          </div>
          <div style="background:#f0f9ff;border:1px solid #bae6fd;color:#0284c7;font-size:10px;
                      font-weight:600;padding:3px 10px;border-radius:20px;letter-spacing:0.5px">
            DAILY DATA
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def secondary_label(text: str):
    st.markdown(
        f"<p style='font-size:13px;font-weight:600;color:#0f172a;margin:16px 0 8px'>{text}</p>",
        unsafe_allow_html=True,
    )
