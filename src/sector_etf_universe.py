"""
Curated US sector-ETF universe.

Why this exists
---------------
The ETFs US list is sourced from etfdb's "top equity ETFs by AUM" page, which
is dominated by broad-market / style / international funds. Only the three or
four largest funds in any GICS sector clear that cut, so the per-sector views
(ETFs US by Sector, ETFs US Sector Leaders) had almost nothing to show:
Real Estate had zero ETFs, Consumer Discretionary and Communication Services
one each, and six sectors had two or fewer.

This module pins a hand-picked set of liquid, real sector ETFs per GICS sector
so every sector has usable depth. The sector is pinned here rather than derived
from yfinance's `category`, because `category` disagrees with GICS for a number
of funds (PAVE and GRID report "Infrastructure", URA/COPX/XLB report "Natural
Resources"); routing those through the category map scatters them into the
wrong bucket.

Returns are NOT fetched here. ETFsManualUpdater.update_sheet already computes
1D/5D/1M/3M/6M/1Y/3Y from yfinance price history for whatever tickers sit in
the sheet, so this module only supplies ticker -> (sector, sub-industry).

SUB_INDUSTRY_RULES adds a second, finer level used by the dashboard dropdown.
The GICS sector is kept intact alongside it, because the Sector Leaders boards
and the S&P 500 Sectors page are built on the 11 GICS sectors.
"""

# ── GICS sector -> tickers ────────────────────────────────────
# Every ticker below was verified against yfinance (AUM, price and name all
# resolve). Ordering is not meaningful; the sheet is sorted by 5D downstream.
SECTOR_ETFS = {
    "Health Care": [
        "XLV", "VHT", "IBB", "XBI", "IHI", "IHF", "PPH", "XPH", "PJP", "FBT",
        "ARKG", "BBH", "GNOM", "IYH", "FHLC", "SBIO", "IDNA", "CNCR", "MEDX",
        "HELX",
    ],
    "Information Technology": [
        "XLK", "VGT", "SMH", "SOXX", "IGV", "CIBR", "HACK", "SKYY", "ARKW",
        "FTEC", "IYW", "QTEC", "PSI", "XSD", "WCLD", "BUG", "ROBO", "BOTZ",
        "AIQ", "IRBO",
    ],
    "Financials": [
        "XLF", "VFH", "KRE", "KBE", "IAI", "IAK", "KBWB", "FNCL", "IYF",
        "KCE", "BIZD", "FTXO", "EUFN", "KIE", "RYF",
    ],
    "Energy": [
        "XLE", "VDE", "XOP", "OIH", "AMLP", "IYE", "FENY", "IEO", "PXE",
        "FCG", "MLPX", "CRAK", "PXJ", "URA", "NLR",
    ],
    "Industrials": [
        "XLI", "VIS", "ITA", "JETS", "IYJ", "PPA", "FIDU", "XAR", "IYT",
        "FXR", "PAVE", "EVX", "SEA", "XTN", "DFEN",
    ],
    "Consumer Discretionary": [
        "XLY", "VCR", "XRT", "FDIS", "IYC", "ITB", "XHB", "PEJ", "FXD",
        "BETZ", "IEDI", "RTH", "ONLN",
    ],
    "Consumer Staples": [
        "XLP", "VDC", "FSTA", "IYK", "KXI", "PBJ", "FTXG", "IECS", "RHS",
    ],
    "Communication Services": [
        "XLC", "VOX", "FCOM", "IYZ", "SOCL", "IXP", "PBS", "NXTG", "ESPO",
    ],
    "Utilities": [
        "XLU", "VPU", "IDU", "FUTY", "JXI", "RYU", "UTES", "PUI", "GRID",
    ],
    "Real Estate": [
        "XLRE", "VNQ", "IYR", "SCHH", "RWR", "ICF", "REM", "VNQI", "FREL",
        "REZ", "MORT", "INDS", "SRVR", "NETL", "PSR",
    ],
    "Materials": [
        "XLB", "VAW", "GDX", "GDXJ", "SIL", "XME", "LIT", "COPX", "IYM",
        "FMAT", "PICK", "SLX", "REMX", "URNM", "WOOD",
    ],
}


# ── Sub-industry rules (matched against the fund NAME) ────────
# Ordered per sector: the first bucket whose keywords appear in the lowercased
# fund name wins. A fund matching nothing falls back to "Broad <sector>", which
# is correct for the plain sector trackers (XLV, XLK, XLF ...).
#
# Keyword notes: use stems ("semicondu", "biotech") so both "Semiconductor" and
# "Semiconductors" match, and keep the more specific bucket first where names
# overlap — "uranium" precedes the generic mining rule so URNM (Uranium Miners)
# lands in Uranium & Nuclear rather than Steel & Mining.
SUB_INDUSTRY_RULES = {
    "Health Care": [
        ("Pharmaceuticals", ["pharmaceutic", "pharma"]),
        ("Medical Devices", ["medical device", "medical breakthrough",
                             "medical", "device", "equipment"]),
        ("Biotech", ["biotech", "genomic", "genom", "oncology", "cancer",
                     "immunolog", "therapeutic"]),
        ("Health Care Providers", ["provider", "insurance"]),
    ],
    "Information Technology": [
        ("Semiconductors", ["semicondu", "sox ", "chip"]),
        ("Cybersecurity", ["cyber", "security"]),
        ("Software & Cloud", ["software", "cloud", "saas", "internet"]),
        ("AI & Robotics", ["robot", "artificial intelligence", "automation"]),
    ],
    "Financials": [
        ("Banks", ["bank", "regional"]),
        ("Insurance", ["insurance", "insurer"]),
        ("Capital Markets", ["broker", "capital market", "exchange",
                             "asset manage", "bdc"]),
    ],
    "Energy": [
        ("Uranium & Nuclear", ["uranium", "nuclear"]),
        ("MLP & Midstream", ["mlp", "midstream", "pipeline", "alerian",
                             "infrastructure"]),
        ("Equipment & Services", ["oil services", "equipment", "services",
                                  "refiner"]),
        ("Oil & Gas E&P", ["exploration", "e&p", "oil & gas", "oil and gas",
                           "natural gas"]),
    ],
    "Materials": [
        ("Uranium & Nuclear", ["uranium"]),
        ("Gold & Precious Metals", ["gold", "silver", "precious"]),
        ("Copper & Base Metals", ["copper", "base metal", "lithium",
                                  "rare earth"]),
        ("Steel & Mining", ["steel", "mining", "miner", "metals"]),
    ],
    "Real Estate": [
        ("Mortgage REITs", ["mortgage"]),
        ("Specialized REITs", ["data center", "data &", "industrial",
                               "infrastructure", "storage", "net lease",
                               "residential"]),
    ],
    "Industrials": [
        ("Aerospace & Defense", ["aerospace", "defense", "defence"]),
        ("Transportation", ["transport", "airline", "jet", "rail",
                            "shipping", "sea to sky", "cargo"]),
        ("Infrastructure & Construction", ["infrastructure", "construction"]),
    ],
    "Consumer Discretionary": [
        ("Retail", ["retail", "online retail", "e-commerce"]),
        ("Homebuilders", ["home construction", "homebuild", "home"]),
        ("Leisure & Travel", ["leisure", "travel", "betting", "gaming",
                              "restaurant"]),
    ],
}


def ticker_to_sector() -> dict:
    """{TICKER: gics_sector} for the whole curated universe."""
    return {t.upper(): sec for sec, ts in SECTOR_ETFS.items() for t in ts}


def all_tickers() -> list:
    """Every curated ticker, de-duplicated, order preserved."""
    seen, out = set(), []
    for ts in SECTOR_ETFS.values():
        for t in ts:
            u = t.upper()
            if u not in seen:
                seen.add(u)
                out.append(u)
    return out


def sub_industry(sector: str, fund_name: str) -> str:
    """Finer bucket for a fund, from its name. Falls back to
    'Broad <sector>' when no rule matches (the plain sector trackers) and for
    sectors that have no rules defined."""
    rules = SUB_INDUSTRY_RULES.get(sector)
    if not rules:
        return f"Broad {sector}"
    low = (fund_name or "").lower()
    for bucket, keywords in rules:
        if any(k in low for k in keywords):
            return bucket
    return f"Broad {sector}"
