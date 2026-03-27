import base64
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from dashboard.auth import login_wall
from dashboard import data, ui

# ── Page config ────────────────────────────────────────────
st.set_page_config(
    page_title="IFPL Market Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Clear sidebar localStorage state ───────────────────────
components.html(
    "<script>Object.keys(localStorage).filter(k => k.includes('sidebar') || k.includes('Sidebar')).forEach(k => localStorage.removeItem(k));</script>",
    height=0,
)

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
    }
    .ifpl-section-header {
        display: none !important;
    }
}
</style>
""",
    unsafe_allow_html=True,
)

# ── Auth ────────────────────────────────────────────────────
if not login_wall():
    st.stop()

# ── Session defaults ────────────────────────────────────────
if "section" not in st.session_state:
    st.session_state.section = "Global Indices"

# ── Sidebar navigation ─────────────────────────────────────
NAV = {
    "MARKETS": [
        ("Global Indices",     "Global Indices"),
        ("NIFTY Sectors",      "NIFTY Sectors"),
        ("NIFTY Indices",      "NIFTY Indices"),
    ],
    "ASSETS": [
        ("ETFs US",            "ETFs US"),
        ("Leveraged Funds",    "Leveraged Funds"),
        ("ETFs India",         "ETFs India"),
        ("Mutual Funds India", "Mutual Funds India"),
        ("Crypto",             "Crypto"),
    ],
    "STOCKS": [
        ("Gainers & Losers US",    "Gainers & Losers US"),
        ("Gainers & Losers India", "Gainers & Losers India"),
        ("ATH US",                 "ATH US"),
        ("ATH India",              "ATH India"),
    ],
}

with st.sidebar:
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:10px;padding:20px 16px 16px;
                    border-bottom:1px solid #1e293b;margin-bottom:8px">
          {_logo_img}
          <div>
            <div style="color:#f1f5f9;font-size:13px;font-weight:700;letter-spacing:0.3px">IFPL Markets</div>
            <div style="color:#38bdf8;font-size:10px;font-weight:500;letter-spacing:1px;
                        text-transform:uppercase;margin-top:1px">Dashboard</div>
          </div>
        </div>
        """,
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

# ── Main content ────────────────────────────────────────────
section = st.session_state.section

if section == "Global Indices":
    ui.section_header("Global Indices", "Major market indices worldwide")
    t1, t2 = data.load_global_indices()
    ui.render_stat_cards(t1)
    ui.render_table(t1, bold_first_col=False)
    if not t2.empty:
        ui.secondary_label("More Indices")
        ui.render_table(t2, height=600, bold_first_col=False)

elif section == "NIFTY Indices":
    ui.section_header("NIFTY Indices", "NSE India index returns")
    t1, t2 = data.load_nifty_indices()
    t1 = t1.drop(t1.columns[1], axis=1)
    t2 = t2.drop(t2.columns[1], axis=1) if not t2.empty else t2
    ui.render_stat_cards(t1)
    ui.render_table(t1, bold_first_col=False)
    if not t2.empty:
        ui.render_table(t2, bold_first_col=False)

elif section == "NIFTY Sectors":
    ui.section_header("NIFTY Sectors", "Sector-wise returns — India")
    df = data.load_nifty_sectors()
    df = df.drop(df.columns[1], axis=1)
    ui.render_stat_cards(df)
    ui.render_table(df, bold_first_col=False)

elif section == "ETFs US":
    ui.section_header("ETFs US", "Top US ETFs by AUM")
    df = data.load_etfs_us()
    df = df.drop(df.columns[2], axis=1)
    ui.render_stat_cards(df)
    ui.render_table(df, height=620)

elif section == "Leveraged Funds":
    ui.section_header("Leveraged Funds", "Biggest leveraged ETFs")
    df = data.load_leveraged_funds()
    df = df.drop(df.columns[3], axis=1)
    ui.render_stat_cards(df)
    ui.render_table(df)

elif section == "ETFs India":
    ui.section_header("ETFs India", "Indian exchange-listed ETFs")
    df = data.load_etfs_india()
    df = df.drop(df.columns[1], axis=1)
    ui.render_stat_cards(df)
    ui.render_table(df, bold_first_col=False)

elif section == "Crypto":
    ui.section_header("Crypto", "Top cryptocurrencies by market cap")
    df = data.load_crypto()
    ui.render_stat_cards(df)
    ui.render_table(df)

elif section == "Mutual Funds India":
    ui.section_header("Mutual Funds India", "NAV and returns")
    df = data.load_mutual_funds()
    ui.render_stat_cards(df)
    ui.render_table(df, height=620, bold_first_col=False)

elif section == "Gainers & Losers US":
    ui.section_header(
        "Gainers & Losers — US",
        "Top 15 weekly gainers and losers · Russell 3000 ($2B+ market cap)",
    )
    gainers, losers = data.load_gl_us()
    col1, col2 = st.columns(2)
    with col1:
        ui.secondary_label("Gainers")
        ui.render_table(gainers)
    with col2:
        ui.secondary_label("Losers")
        ui.render_table(losers)

elif section == "Gainers & Losers India":
    ui.section_header(
        "Gainers & Losers — India",
        "Top 15 weekly gainers and losers · NIFTY 500 (Rs1000Cr+ market cap)",
    )
    gainers, losers = data.load_gl_india()
    col1, col2 = st.columns(2)
    with col1:
        ui.secondary_label("Gainers")
        ui.render_table(gainers)
    with col2:
        ui.secondary_label("Losers")
        ui.render_table(losers)

elif section == "ATH US":
    ui.section_header(
        "All-Time High — US",
        "Stocks within 1% of all-time high · sorted by 1W%",
    )
    df = data.load_ath_us()
    ui.render_table(df, height=620)

elif section == "ATH India":
    ui.section_header(
        "All-Time High — India",
        "Stocks within 1% of all-time high · sorted by 1W%",
    )
    df = data.load_ath_india()
    ui.render_table(df, height=620)
