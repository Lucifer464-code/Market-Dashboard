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

# ── One-time JS: sidebar cleanup + PWA manifest injection ──
components.html("""<script>
// Sidebar cleanup
Object.keys(localStorage).filter(k => k.includes('sidebar') || k.includes('Sidebar')).forEach(k => localStorage.removeItem(k));

// PWA: inject manifest + meta tags into parent document head
(function() {
  var doc = window.parent.document;
  if (doc.querySelector('link[rel="manifest"]')) return;

  var manifest = {
    name: "IFPL Market Dashboard",
    short_name: "IFPL Markets",
    start_url: "/",
    display: "standalone",
    background_color: "#0f172a",
    theme_color: "#0f172a",
    icons: [{
      src: "https://ui-avatars.com/api/?name=IFPL&background=0ea5e9&color=fff&size=192&bold=true&format=png",
      sizes: "192x192",
      type: "image/png"
    }, {
      src: "https://ui-avatars.com/api/?name=IFPL&background=0ea5e9&color=fff&size=512&bold=true&format=png",
      sizes: "512x512",
      type: "image/png"
    }]
  };

  var blob = new Blob([JSON.stringify(manifest)], {type: 'application/json'});
  var link = doc.createElement('link');
  link.rel = 'manifest';
  link.href = URL.createObjectURL(blob);
  doc.head.appendChild(link);

  // Mobile meta tags
  var metas = [
    {name: 'mobile-web-app-capable', content: 'yes'},
    {name: 'apple-mobile-web-app-capable', content: 'yes'},
    {name: 'apple-mobile-web-app-status-bar-style', content: 'black-translucent'},
    {name: 'apple-mobile-web-app-title', content: 'IFPL Markets'},
    {name: 'theme-color', content: '#0f172a'},
    {name: 'viewport', content: 'width=device-width, initial-scale=1, viewport-fit=cover'}
  ];
  metas.forEach(function(m) {
    var tag = doc.createElement('meta');
    tag.name = m.name;
    tag.content = m.content;
    doc.head.appendChild(tag);
  });
})();
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

/* Force inner label container to left-align (Streamlit centers the inner p by default) */
[data-testid="stSidebar"] button > div,
[data-testid="stSidebar"] button [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"] button p {
    text-align: left !important;
    justify-content: flex-start !important;
    margin-left: 0 !important;
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

/* ── PWA standalone: safe area insets for notch/home indicator ── */
@media all and (display-mode: standalone) {
    section[data-testid="stMain"] > div:first-child {
        padding-top: 0 !important;
    }
    .ifpl-mobile-header {
        padding-top: calc(10px + env(safe-area-inset-top)) !important;
    }
    body { padding-bottom: env(safe-area-inset-bottom); }
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
        padding-left: 10px !important;
        padding-right: 10px !important;
        padding-top: 56px !important;
    }
    .ifpl-section-header {
        display: none !important;
    }
    .ifpl-mobile-header {
        display: flex !important;
    }
    /* Tighter block spacing */
    [data-testid="stVerticalBlock"] > div { gap: 0.5rem !important; }
    /* Let iframes fill width without overflow */
    [data-testid="stCustomComponentV1"] { max-width: 100vw !important; }
    iframe { max-width: 100% !important; }
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
        ("Live Market",                       "Live Market"),
        ("Global Indices",                    "Global Indices"),
        ("Additional Global Indices",         "Additional Global Indices"),
        ("NIFTY Sectoral Indices",            "NIFTY Sectors"),
        ("Additional NIFTY Sectoral Indices", "Additional NIFTY Sector Indices"),
        ("Broad Market Indices",              "Broad Market Indices"),
        ("NIFTY 500 Momentum 50",             "NIFTY 500 Momentum 50"),
        ("S&P 500 Sectors",                   "S&P 500 Sectors"),
    ],
    "FUNDS": [
        ("ETFs US",                    "ETFs US"),
        ("Leveraged Funds",            "Leveraged Funds"),
        ("ETFs India",                 "ETFs India"),
        ("Commodity ETFs",             "Commodity ETFs"),
        ("Leveraged Commodity Funds",  "Leveraged Commodity Funds"),
        ("Mutual Funds India",         "Mutual Funds India"),
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
    "INSTITUTIONAL INVESTORS": [
        ("Hedge Funds",                 "Hedge Funds"),
        ("Top Hedge Fund Investments",  "Top Hedge Fund Investments"),
        ("Indian Investors",            "Indian Investors"),
    ],
}

with st.sidebar:
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:10px;padding:20px 16px 12px;
                    border-bottom:1px solid #1e293b;margin-bottom:8px">
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

# ── Resilient sheet load ────────────────────────────────────
def safe_load(fn, *args):
    """Call a data loader, retrying once on any error. Returns None on
    final failure so the calling section can show an inline notice instead
    of crashing the whole page."""
    for attempt in range(2):
        try:
            return fn(*args)
        except Exception:
            if attempt == 0:
                continue
            return None


# Firebase WEB config for the Live Market page (StockPulse Firestore project).
# The apiKey is provided ONLY via st.secrets["FIREBASE_WEB_CONFIG"] — never
# hard-coded in source (GitHub secret scanning flags Google API keys). The
# non-secret identifiers below are used as defaults for any keys the secret
# omits; without a configured apiKey the Live Market page shows a setup notice.
_FIREBASE_WEB_CONFIG_DEFAULTS = {
    "authDomain":        "stockpulse-aa107.firebaseapp.com",
    "projectId":         "stockpulse-aa107",
    "storageBucket":     "stockpulse-aa107.firebasestorage.app",
    "messagingSenderId": "971560924277",
    "appId":             "1:971560924277:web:efadf939b3642af316343c",
}


def firebase_web_config() -> dict:
    """Return the Firebase web config, merging st.secrets over the non-secret
    defaults. Returns {} (no apiKey) if the secret isn't configured."""
    try:
        cfg = st.secrets.get("FIREBASE_WEB_CONFIG")
        if cfg:
            return {**_FIREBASE_WEB_CONFIG_DEFAULTS, **dict(cfg)}
    except Exception:
        pass
    return {}


def auto_refresh(interval_ms: int):
    """Reload the page every interval_ms (no extra dependency). A tiny
    self-reloading iframe triggers a full Streamlit rerun, which re-reads the
    short-TTL live caches. Used by the live Global Indices pages."""
    components.html(
        f"<script>setTimeout(function(){{window.parent.location.reload();}}, {interval_ms});</script>",
        height=0,
    )


_HEATMAP_PERIODS = ["1D", "5D", "1M", "3M", "6M", "1Y", "3Y"]


def _heatmap_supported() -> bool:
    """True only if the ui module exposes the heatmap helpers. Guards
    against a stale-module deploy on Streamlit Cloud where dashboard.py
    is reloaded but the cached dashboard.ui predates these functions —
    in that case we silently fall back to the table view instead of
    crashing with an AttributeError."""
    return all(hasattr(ui, attr) for attr in ("render_heatmap", "resolve_period_col"))


def heatmap_controls(key: str):
    """Render the Table/Heatmap toggle + period selector. Returns
    (view, period) where view is 'Table' or 'Heatmap'. If the heatmap
    helpers aren't available (stale module), only Table is offered."""
    if not _heatmap_supported():
        return "Table", "5D"
    c1, c2 = st.columns([1, 1])
    with c1:
        view = st.radio("View", ["Table", "Heatmap"], horizontal=True,
                        label_visibility="collapsed", key=f"view_{key}")
    period = "5D"
    if view == "Heatmap":
        with c2:
            period = st.selectbox("Color by", _HEATMAP_PERIODS, index=1,
                                  key=f"period_{key}")
    return view, period


# ── Mobile nav (derived from the single-source NAV) ─────────
# Map section key -> label for header lookups, and build the mobile
# dropdown structure from NAV so it can never drift from the sidebar.
_KEY_TO_LABEL = {key: label for items in NAV.values() for label, key in items}
_MOBILE_NAV   = {group: [label for label, _ in items] for group, items in NAV.items()}
ui.mobile_nav(_KEY_TO_LABEL.get(st.session_state.section, st.session_state.section), _MOBILE_NAV)

# ── Main content ────────────────────────────────────────────
section = st.session_state.section

if section == "Live Market":
    ui.section_header("Live Market", "Live NSE prices · updates while the market is open")
    _fb_cfg = firebase_web_config()
    if not _fb_cfg.get("apiKey"):
        st.info("Live Market is not configured. Add FIREBASE_WEB_CONFIG "
                "(including apiKey) to the app secrets to enable it.")
    else:
        ui.render_live_market(_fb_cfg)

elif section == "Global Indices":
    res = safe_load(data.load_global_indices)
    price_as_of, _ = data.load_stocks_metadata("Global Indices")
    ui.section_header("Global Indices", "Major market indices worldwide",
                      price_as_of=price_as_of)
    if res is None:
        ui.load_error()
    else:
        t1, _ = res
        t1 = data.apply_global_live(t1, t1.columns[0])   # live Price + 1D%
        st.caption("Live price & 1D% · delayed ~15 min · auto-refreshes every 30s")
        t1 = ui.sort_by_keyword(t1, "5d")
        ui.render_stat_cards(t1)
        ui.render_table(t1, bold_first_col=False)
        auto_refresh(30000)

elif section == "Additional Global Indices":
    res = safe_load(data.load_global_indices)
    price_as_of, _ = data.load_stocks_metadata("Global Indices")
    ui.section_header("Additional Global Indices", "More market indices worldwide",
                      price_as_of=price_as_of)
    if res is None:
        ui.load_error()
    else:
        _, t2 = res
        if not t2.empty:
            t2 = data.apply_global_live(t2, t2.columns[0])   # live Price + 1D%
            st.caption("Live price & 1D% · delayed ~15 min · auto-refreshes every 30s")
            t2 = ui.sort_by_keyword(t2, "5d")
            ui.render_stat_cards(t2)
            ui.render_table(t2, height=600, bold_first_col=False)
        auto_refresh(30000)

elif section == "S&P 500 Sectors":
    df = safe_load(data.load_sp500_sectors)
    price_as_of, _ = data.load_stocks_metadata("S&P500 Sectors")
    ui.section_header("S&P 500 Sectors", "S&P 500 sector returns — GICS classification",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        df = df.drop(df.columns[1], axis=1)   # drop Ticker
        view, period = heatmap_controls("sp500")
        if view == "Heatmap":
            pcol = ui.resolve_period_col(df, period)
            df = ui.sort_by_keyword(df, period)
            link = "__link__Sector" if "__link__Sector" in df.columns else None
            ui.render_heatmap(df, df.columns[0], pcol, link_col=link)
        else:
            df = ui.sort_by_keyword(df, "5d")
            ui.render_stat_cards(df)
            ui.render_table(df, bold_first_col=False)

elif section == "Additional NIFTY Sector Indices":
    res = safe_load(data.load_nifty_indices)
    price_as_of, _ = data.load_stocks_metadata("NIFTY Indices")
    ui.section_header("Additional NIFTY Sector Indices", "NSE India sector index returns",
                      price_as_of=price_as_of)
    if res is None:
        ui.load_error()
    else:
        t1, _ = res
        t1 = t1.drop(t1.columns[1], axis=1)
        t1 = ui.sort_by_keyword(t1, "5d")
        ui.render_stat_cards(t1.drop(t1.columns[2], axis=1))   # exclude P/E from cards
        ui.render_table(t1, bold_first_col=False)

elif section == "Broad Market Indices":
    res = safe_load(data.load_nifty_indices)
    price_as_of, _ = data.load_stocks_metadata("NIFTY Indices")
    ui.section_header("Broad Market Indices", "NSE India broad market index returns",
                      price_as_of=price_as_of)
    if res is None:
        ui.load_error()
    else:
        _, t2 = res
        if not t2.empty:
            t2 = t2.drop(t2.columns[1], axis=1)
            t2 = ui.sort_by_keyword(t2, "5d")
            ui.render_stat_cards(t2.drop(t2.columns[2], axis=1))   # exclude P/E from cards
            ui.render_table(t2, bold_first_col=False)

elif section == "NIFTY 500 Momentum 50":
    df = safe_load(data.load_nifty_momentum_50)
    price_as_of, _ = data.load_stocks_metadata("NIFTY500Moment.50")
    ui.section_header("NIFTY 500 Momentum 50", "Momentum stocks — NSE",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    elif not df.empty:
        ui.render_table(df, bold_first_col=False)

    sectors_500    = safe_load(data.load_nifty500_sectors)
    sectors_moment = safe_load(data.load_nifty_momentum_sectors)

    # 42px header + 10 rows * 37px + 20px padding = 432 (render_table auto-calc formula)
    _SIDE_TABLE_HEIGHT = 432
    _title_style = (
        "font-size:15px;font-weight:600;margin:16px 0 8px;"
        "height:22px;line-height:22px;white-space:nowrap;"
        "overflow:hidden;text-overflow:ellipsis;"
    )

    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown(f"<div style='{_title_style}'>NIFTY 500 Sectors</div>",
                    unsafe_allow_html=True)
        if sectors_500 is not None and not sectors_500.empty:
            df_500 = ui.sort_by_keyword(sectors_500.iloc[:, [0, 2]], "weight")
            ui.render_table(df_500, bold_first_col=False,
                            height=_SIDE_TABLE_HEIGHT, fixed_height=True)
    with col_right:
        st.markdown(f"<div style='{_title_style}'>NIFTY Momentum Sectors</div>",
                    unsafe_allow_html=True)
        if sectors_moment is not None and not sectors_moment.empty:
            df_moment = ui.sort_by_keyword(sectors_moment.iloc[:, [0, 2]], "weight")
            ui.render_table(df_moment, bold_first_col=False,
                            height=_SIDE_TABLE_HEIGHT, fixed_height=True)

elif section == "NIFTY Sectors":
    df = safe_load(data.load_nifty_sectors)
    price_as_of, _ = data.load_stocks_metadata("NIFTY Sectors")
    ui.section_header("NIFTY Sectors", "Sector-wise returns — India",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        df = df.drop(df.columns[1], axis=1)   # drop Ticker
        view, period = heatmap_controls("nifty_sectors")
        if view == "Heatmap":
            pcol = ui.resolve_period_col(df, period)
            df = ui.sort_by_keyword(df, period)
            link = "__link__Index" if "__link__Index" in df.columns else None
            ui.render_heatmap(df, df.columns[0], pcol, link_col=link)
        else:
            df = ui.sort_by_keyword(df, "5d")
            ui.render_stat_cards(df.drop(df.columns[2], axis=1))   # exclude P/E from cards
            ui.render_table(df, bold_first_col=False)

elif section == "ETFs US":
    df = safe_load(data.load_etfs_us)
    price_as_of, _ = data.load_stocks_metadata("ETFs US")
    ui.section_header("ETFs US", "Top US ETFs by AUM",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        df = df.drop(df.columns[2], axis=1)
        ui.render_table(df, height=620, searchable=True)

elif section == "Leveraged Funds":
    df = safe_load(data.load_leveraged_funds)
    price_as_of, _ = data.load_stocks_metadata("Biggest Leveraged Funds ")
    ui.section_header("Leveraged Funds", "Biggest leveraged ETFs",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        df = df.drop(df.columns[3], axis=1)
        ui.render_table(df, searchable=True)

elif section == "ETFs India":
    df = safe_load(data.load_etfs_india)
    price_as_of, _ = data.load_stocks_metadata("ETFs India")
    ui.section_header("ETFs India", "Indian exchange-listed ETFs",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        df = df.drop(df.columns[1], axis=1)
        ui.render_table(df, bold_first_col=False, searchable=True)

elif section == "Commodity ETFs":
    df = safe_load(data.load_commodity_etfs)
    price_as_of, _ = data.load_stocks_metadata("Commodity ETFs")
    ui.section_header("Commodity ETFs", "Metals & commodity ETFs by exposure",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        # Columns: Ticker, ETF name, Metal, Ticker(exch), Price, 1D, 5D, ... — drop col 3
        df = df.drop(df.columns[3], axis=1)
        df = ui.sort_by_keyword(df, "5d")
        ui.render_table(df, bold_first_col=False)

elif section == "Leveraged Commodity Funds":
    df = safe_load(data.load_leveraged_commodity_funds)
    price_as_of, _ = data.load_stocks_metadata("Biggest Leveraged Funds(Com.)")
    ui.section_header("Leveraged Commodity Funds", "Biggest leveraged commodity ETFs",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        # Columns: Ticker, Name, Leverage, Commodity, AUM(USD numeric), AUM (fmt), ...
        # Drop the redundant numeric AUM column (index 4); keep Commodity + formatted AUM.
        if len(df.columns) > 4:
            df = df.drop(df.columns[4], axis=1)
        ui.render_table(df)

elif section == "Crypto":
    df = safe_load(data.load_crypto)
    price_as_of, _ = data.load_stocks_metadata("Crypto")
    ui.section_header("Crypto", "Top cryptocurrencies by market cap",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        ui.render_table(df)

elif section == "Mutual Funds India":
    df = safe_load(data.load_mutual_funds)
    price_as_of, _ = data.load_stocks_metadata("Mutual Funds India")
    ui.section_header("Mutual Funds India", "NAV and returns",
                      price_as_of=price_as_of)
    if df is None:
        ui.load_error()
    else:
        ui.render_table(df, height=620, bold_first_col=False, searchable=True)

elif section == "Gainers & Losers US":
    res = safe_load(data.load_gl_us)
    price_as_of, _ = data.load_stocks_metadata("Top G&L US")
    ui.section_header(
        "Gainers & Losers — US",
        "Top 15 weekly gainers and losers · Russell 3000 ($2B+ market cap)",
        price_as_of=price_as_of,
    )
    if res is None:
        ui.load_error()
    else:
        gainers, losers = res
        col1, col2 = st.columns(2)
        with col1:
            ui.secondary_label("Gainers")
            ui.render_table(gainers)
        with col2:
            ui.secondary_label("Losers")
            ui.render_table(losers)

elif section == "Gainers & Losers India":
    res = safe_load(data.load_gl_india)
    price_as_of, _ = data.load_stocks_metadata("Top G&L India")
    ui.section_header(
        "Gainers & Losers — India",
        "Top 15 weekly gainers and losers · NIFTY 500 (Rs1000Cr+ market cap)",
        price_as_of=price_as_of,
    )
    if res is None:
        ui.load_error()
    else:
        gainers, losers = res
        col1, col2 = st.columns(2)
        with col1:
            ui.secondary_label("Gainers")
            ui.render_table(gainers)
        with col2:
            ui.secondary_label("Losers")
            ui.render_table(losers)

elif section == "ATH US":
    df = safe_load(data.load_ath_us)
    price_as_of, _ = data.load_stocks_metadata("ATH US")
    ui.section_header(
        "All-Time High — US",
        "Stocks within 5% of all-time high · sorted by 1W%",
        price_as_of=price_as_of,
    )
    if df is None:
        ui.load_error()
    else:
        ui.render_table(df, height=620, searchable=True)

elif section == "ATH India":
    df = safe_load(data.load_ath_india)
    price_as_of, _ = data.load_stocks_metadata("ATH India")
    ui.section_header(
        "All-Time High — India",
        "Stocks within 5% of all-time high · sorted by 1W%",
        price_as_of=price_as_of,
    )
    if df is None:
        ui.load_error()
    else:
        ui.render_table(df, height=620, searchable=True)

elif section == "Indian Investors":
    df = safe_load(data.load_investor_holdings)
    ui.section_header(
        "Indian Institutional Investors",
        "Quarterly 1%+ holdings and changes",
    )
    if df is None:
        ui.load_error()
    elif df.empty:
        st.info("No data available.")
    else:
        investors = sorted(df["Investor"].dropna().unique())
        col1, col2 = st.columns([2, 1])
        with col1:
            selected = st.selectbox("Investor", ["All investors"] + investors)
        with col2:
            change_sel = st.selectbox(
                "Change type",
                ["All", "Increase", "Decrease", "New Entry", "Exit / Changed", "No Change"],
            )

        view = df if selected == "All investors" else df[df["Investor"] == selected]
        if change_sel != "All":
            view = view[view["Change"] == change_sel]

        if selected != "All investors":
            view = view.drop(columns=["Investor"])

        st.caption(f"{len(view)} position{'s' if len(view) != 1 else ''}")
        change_colors = {
            "Change": {
                "Increase":       ui.GREEN,
                "New Entry":      ui.GREEN,
                "Decrease":       ui.RED,
                "Exit / Changed": ui.RED,
            }
        }
        ui.render_table(view, height=620, bold_first_col=False, cell_color_map=change_colors)

elif section == "Hedge Funds":
    df = safe_load(data.load_hedge_funds)
    ui.section_header("Hedge Funds", "Top hedge funds overview")
    if df is None:
        ui.load_error()
    else:
        ui.render_table(df, bold_first_col=False)

elif section == "Top Hedge Fund Investments":
    df = safe_load(data.load_top_hedge_fund_investments)
    ui.section_header("Top Hedge Fund Investments", "Largest hedge fund positions")
    if df is None:
        ui.load_error()
    else:
        ui.render_table(df, bold_first_col=False)
