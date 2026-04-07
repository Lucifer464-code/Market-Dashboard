import base64
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from dashboard import data, ui


# ── Page config ────────────────────────────────────────────
st.set_page_config(
    page_title="IFPL Market Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── One-time JS: sidebar cleanup ───────────────────────────
components.html("""<script>
Object.keys(localStorage).filter(k => k.includes('sidebar') || k.includes('Sidebar')).forEach(k => localStorage.removeItem(k));
</script>""", height=0)

# ── Logo (base64) ───────────────────────────────────────────
_logo_path = Path(__file__).parent / "assets" / "logo.png"
_logo_b64 = ""
if _logo_path.exists():
    _logo_b64 = base64.b64encode(_logo_path.read_bytes()).decode()

_logo_img = (
    f'<img src="data:image/png;base64,{_logo_b64}" width="36" height="36" '
    f'style="border-radius:6px;object-fit:cover;flex-shrink:0" />'
    if _logo_b64 else ""
)

# ── Global CSS ─────────────────────────────────────────────
st.markdown(
    """
<style>
/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }

/* Hide sidebar collapse button */
[data-testid="stSidebarCollapseButton"] { display: none !important; }

/* Main background */
[data-testid="stAppViewContainer"] { background: #f8fafc; }
[data-testid="block-container"] { background: transparent; padding-top: 0 !important; }

/* Sidebar background */
[data-testid="stSidebar"] {
    background-color: #0f172a;
    border-right: 1px solid #1e293b;
}

/* Sidebar nav buttons — inactive */
[data-testid="stSidebar"] button[kind="secondary"] {
    background:  transparent !important;
    border:      none        !important;
    color:       #94a3b8     !important;
    text-align:  left        !important;
    font-size:   13px        !important;
    padding:     7px 10px   !important;
    border-radius: 6px      !important;
    justify-content: flex-start !important;
    width: 100%;
}
[data-testid="stSidebar"] button[kind="secondary"]:hover {
    background: #1e293b !important;
    color:      #cbd5e1 !important;
}

/* Sidebar nav buttons — active */
[data-testid="stSidebar"] button[kind="primary"] {
    background:    #0ea5e9   !important;
    border:        none      !important;
    color:         #ffffff   !important;
    text-align:    left      !important;
    font-size:     13px      !important;
    font-weight:   600       !important;
    padding:       7px 10px  !important;
    border-radius: 6px       !important;
    justify-content: flex-start !important;
    width: 100%;
}

/* Dividers */
[data-testid="stSidebar"] hr { border-color: #1e293b; }

/* Remove excess padding from sidebar button containers */
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
    gap: 2px;
}

/* Data table card styling */
[data-testid="stDataFrame"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
    overflow: hidden !important;
}

/* ── Collapse zero-height component wrappers ── */
[data-testid="stCustomComponentV1"][style*="height: 0"] {
    display: none !important;
}

/* ── Mobile: push sidebar off-screen, keep buttons clickable ── */
@media (max-width: 768px) {
    [data-testid="stSidebar"] {
        position: fixed !important;
        left: -9999px !important;
        width: 1px !important;
        overflow: hidden !important;
    }
    section[data-testid="stMain"] > div:first-child {
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        padding-top: 56px !important;
    }
    .ifpl-section-header {
        display: none !important;
    }
    .ifpl-mobile-header {
        display: flex !important;
    }
}
.ifpl-mobile-header {
    display: none;
    position: fixed;
    top: 0; left: 0; right: 0;
    z-index: 99990;
    background: #0f172a;
    padding: 10px 16px;
    align-items: center;
    gap: 10px;
    border-bottom: 1px solid #1e293b;
}
</style>
""",
    unsafe_allow_html=True,
)

# ── Mobile fixed header ─────────────────────────────────────
st.markdown(
    f"""
<div class="ifpl-mobile-header">
  {_logo_img}
  <div>
    <div style="color:#f1f5f9;font-size:13px;font-weight:700;letter-spacing:0.3px">IFPL</div>
    <div style="color:#38bdf8;font-size:10px;font-weight:500;letter-spacing:1px;text-transform:uppercase;margin-top:1px">Market Dashboard</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)


# ── Session defaults ────────────────────────────────────────
if "section" not in st.session_state:
    st.session_state.section = "Global Indices"

# ── Sidebar navigation ─────────────────────────────────────
NAV = {
    "MARKETS": [
        ("Global Indices",                    "Global Indices"),
        ("Additional Global Indices",         "Additional Global Indices"),
        ("NIFTY Sectoral Indices",            "NIFTY Sectors"),
        ("Additional NIFTY Sectoral Indices", "Additional NIFTY Sector Indices"),
        ("Broad Market Indices",              "Broad Market Indices"),
        ("S&P 500 Sectors",                   "S&P 500 Sectors"),
    ],
    "FUNDS": [
        ("ETFs US",            "ETFs US"),
        ("Leveraged Funds",    "Leveraged Funds"),
        ("ETFs India",         "ETFs India"),
        ("Mutual Funds India", "Mutual Funds India"),
    ],
    "CRYPTO": [
        ("Crypto", "Crypto"),
    ],
    "STOCKS": [
        ("Gainers & Losers US",    "Gainers & Losers US"),
        ("Gainers & Losers India", "Gainers & Losers India"),
        ("ATH US",                 "ATH US"),
        ("ATH India",              "ATH India"),
    ],
}

with st.sidebar:
    _last_updated = data.load_last_updated() or "—"
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:10px;padding:20px 16px 12px;
                    border-bottom:1px solid #1e293b;margin-bottom:4px">
          {_logo_img}
          <div>
            <div style="color:#f1f5f9;font-size:13px;font-weight:700;letter-spacing:0.3px">IFPL</div>
            <div style="color:#38bdf8;font-size:10px;font-weight:500;letter-spacing:1px;
                        text-transform:uppercase;margin-top:1px">Market Dashboard</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div style='padding:6px 16px 12px;border-bottom:1px solid #1e293b;margin-bottom:8px'>"
        f"<div style='color:#475569;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px'>Last updated</div>"
        f"<div style='color:#94a3b8;font-size:10px;margin-top:2px'>{_last_updated}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    for group, items in NAV.items():
        st.markdown(
            f"<p style='color:#475569;font-size:9px;font-weight:700;"
            f"letter-spacing:1.5px;text-transform:uppercase;"
            f"margin:14px 0 4px 10px'>{group}</p>",
            unsafe_allow_html=True,
        )
        for label, key in items:
            active = st.session_state.section == key
            if st.button(
                label,
                key=f"nav_{key}",
                use_container_width=True,
                type="primary" if active else "secondary",
            ):
                st.session_state.section = key
                st.rerun()

    st.divider()
    if st.button("Refresh Data", key="refresh_cache", use_container_width=True, type="secondary"):
        st.cache_data.clear()
        st.rerun()
    st.markdown(
        "<p style='color:#94a3b8;font-size:10px;padding:0 10px'>Data refreshes daily</p>",
        unsafe_allow_html=True,
    )

# ── Mobile nav ──────────────────────────────────────────────
ui.mobile_nav(st.session_state.section)

# ── Main content ────────────────────────────────────────────
section = st.session_state.section

if section == "Global Indices":
    t1, _ = data.load_global_indices()
    price_as_of, updated_at = data.load_stocks_metadata("Global Indices")
    ui.section_header("Global Indices", "Major market indices worldwide",
                      price_as_of=price_as_of, updated_at=updated_at)
    t1 = ui.sort_by_keyword(t1, "5d")
    ui.render_stat_cards(t1)
    ui.render_table(t1, bold_first_col=False)

elif section == "Additional Global Indices":
    _, t2 = data.load_global_indices()
    price_as_of, updated_at = data.load_stocks_metadata("Global Indices")
    ui.section_header("Additional Global Indices", "More market indices worldwide",
                      price_as_of=price_as_of, updated_at=updated_at)
    if not t2.empty:
        t2 = ui.sort_by_keyword(t2, "5d")
        ui.render_stat_cards(t2)
        ui.render_table(t2, height=600, bold_first_col=False)

elif section == "S&P 500 Sectors":
    df = data.load_sp500_sectors()
    price_as_of, updated_at = data.load_stocks_metadata("S&P500 Sectors")
    ui.section_header("S&P 500 Sectors", "S&P 500 sector returns — GICS classification",
                      price_as_of=price_as_of, updated_at=updated_at)
    df = df.drop(df.columns[1], axis=1)
    df = ui.sort_by_keyword(df, "5d")
    ui.render_stat_cards(df)
    ui.render_table(df, bold_first_col=False)

elif section == "Additional NIFTY Sector Indices":
    t1, _ = data.load_nifty_indices()
    price_as_of, updated_at = data.load_stocks_metadata("NIFTY Indices")
    ui.section_header("Additional NIFTY Sector Indices", "NSE India sector index returns",
                      price_as_of=price_as_of, updated_at=updated_at)
    t1 = t1.drop(t1.columns[1], axis=1)
    t1 = ui.sort_by_keyword(t1, "5d")
    ui.render_stat_cards(t1)
    ui.render_table(t1, bold_first_col=False)

elif section == "Broad Market Indices":
    _, t2 = data.load_nifty_indices()
    price_as_of, updated_at = data.load_stocks_metadata("NIFTY Indices")
    ui.section_header("Broad Market Indices", "NSE India broad market index returns",
                      price_as_of=price_as_of, updated_at=updated_at)
    if not t2.empty:
        t2 = t2.drop(t2.columns[1], axis=1)
        t2 = ui.sort_by_keyword(t2, "5d")
        ui.render_stat_cards(t2)
        ui.render_table(t2, bold_first_col=False)

elif section == "NIFTY Sectors":
    df = data.load_nifty_sectors()
    price_as_of, updated_at = data.load_stocks_metadata("NIFTY Sectors")
    ui.section_header("NIFTY Sectors", "Sector-wise returns — India",
                      price_as_of=price_as_of, updated_at=updated_at)
    df = df.drop(df.columns[1], axis=1)
    df = ui.sort_by_keyword(df, "5d")
    ui.render_stat_cards(df)
    ui.render_table(df, bold_first_col=False)

elif section == "ETFs US":
    df = data.load_etfs_us()
    price_as_of, updated_at = data.load_stocks_metadata("ETFs US")
    ui.section_header("ETFs US", "Top US ETFs by AUM",
                      price_as_of=price_as_of, updated_at=updated_at)
    df = df.drop(df.columns[2], axis=1)
    ui.render_table(df, height=620)

elif section == "Leveraged Funds":
    df = data.load_leveraged_funds()
    price_as_of, updated_at = data.load_stocks_metadata("Biggest Leveraged Funds ")
    ui.section_header("Leveraged Funds", "Biggest leveraged ETFs",
                      price_as_of=price_as_of, updated_at=updated_at)
    df = df.drop(df.columns[3], axis=1)
    ui.render_table(df)

elif section == "ETFs India":
    df = data.load_etfs_india()
    price_as_of, updated_at = data.load_stocks_metadata("ETFs India")
    ui.section_header("ETFs India", "Indian exchange-listed ETFs",
                      price_as_of=price_as_of, updated_at=updated_at)
    df = df.drop(df.columns[1], axis=1)
    ui.render_table(df, bold_first_col=False)

elif section == "Crypto":
    df = data.load_crypto()
    price_as_of, updated_at = data.load_stocks_metadata("Crypto")
    ui.section_header("Crypto", "Top cryptocurrencies by market cap",
                      price_as_of=price_as_of, updated_at=updated_at)
    ui.render_table(df)

elif section == "Mutual Funds India":
    df = data.load_mutual_funds()
    price_as_of, updated_at = data.load_stocks_metadata("Mutual Funds India")
    ui.section_header("Mutual Funds India", "NAV and returns",
                      price_as_of=price_as_of, updated_at=updated_at)
    ui.render_table(df, height=620, bold_first_col=False)

elif section == "Gainers & Losers US":
    gainers, losers = data.load_gl_us()
    price_as_of, updated_at = data.load_stocks_metadata("Top G&L US")
    ui.section_header(
        "Gainers & Losers — US",
        "Top 15 weekly gainers and losers · Russell 3000 ($2B+ market cap)",
        price_as_of=price_as_of,
        updated_at=updated_at,
    )
    col1, col2 = st.columns(2)
    with col1:
        ui.secondary_label("Gainers")
        ui.render_table(gainers)
    with col2:
        ui.secondary_label("Losers")
        ui.render_table(losers)

elif section == "Gainers & Losers India":
    gainers, losers = data.load_gl_india()
    price_as_of, updated_at = data.load_stocks_metadata("Top G&L India")
    ui.section_header(
        "Gainers & Losers — India",
        "Top 15 weekly gainers and losers · NIFTY 500 (Rs1000Cr+ market cap)",
        price_as_of=price_as_of,
        updated_at=updated_at,
    )
    col1, col2 = st.columns(2)
    with col1:
        ui.secondary_label("Gainers")
        ui.render_table(gainers)
    with col2:
        ui.secondary_label("Losers")
        ui.render_table(losers)

elif section == "ATH US":
    df = data.load_ath_us()
    price_as_of, updated_at = data.load_stocks_metadata("ATH US")
    ui.section_header(
        "All-Time High — US",
        "Stocks within 1% of all-time high · sorted by 1W%",
        price_as_of=price_as_of,
        updated_at=updated_at,
    )
    ui.render_table(df, height=620)

elif section == "ATH India":
    df = data.load_ath_india()
    price_as_of, updated_at = data.load_stocks_metadata("ATH India")
    ui.section_header(
        "All-Time High — India",
        "Stocks within 1% of all-time high · sorted by 1W%",
        price_as_of=price_as_of,
        updated_at=updated_at,
    )
    ui.render_table(df, height=620)
