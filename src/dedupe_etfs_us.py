"""
One-off repair: de-duplicate and compact the 'ETFs US' sheet.

A bug in ManualETFEngine.ensure_sector_etfs_listed computed its append row
from a COUNT of non-blank rows rather than the LAST occupied row. Because
gspread trims trailing blank cells, that count under-shot the true extent of
the list, so the append landed inside existing data and rewrote live rows —
leaving each affected ETF duplicated on the row directly beneath itself.
The duplicate is identifiable: it carries the ticker/name but a blank AUM.

This script rewrites the ticker block with one row per ticker, preferring the
copy that still has AUM, and preserving the original ordering. Returns are
left alone: database.py recomputes them for every row on the next run.

It also COMPACTS the list. The same bug stranded part of its append far down
the sheet (rows 394-400), leaving a gap between the main list and the tail.
A gap is not harmless: the top-up appends after the LAST occupied row, so a
stranded tail at row 400 leaves no room beneath it and the top-up cannot run.
Rewriting the block as one contiguous run from START_ROW fixes that.

Usage:
    python src/dedupe_etfs_us.py            # dry run, prints the plan
    python src/dedupe_etfs_us.py --apply    # write the cleaned list back
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import gspread                                    # noqa: E402
from google.oauth2.service_account import Credentials   # noqa: E402

SHEET_ID = "1uJoD2JRvzRpn2KHJa80aZADQ2DfRwm2qbZKMuv0PKBM"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
TAB = "ETFs US"
START_ROW = 3
END_ROW = 400
LAST_COL = "M"          # B..M is the full per-ETF record


def _client():
    creds = Credentials.from_service_account_file("service_account.json",
                                                  scopes=SCOPES)
    return gspread.authorize(creds)


def main() -> int:
    apply = "--apply" in sys.argv
    ws = _client().open_by_key(SHEET_ID).worksheet(TAB)

    width = ord(LAST_COL) - ord("B") + 1
    raw = ws.get(f"B{START_ROW}:{LAST_COL}{END_ROW}")
    rows = [(list(r) + [""] * width)[:width] for r in raw]

    # Keep first occurrence per ticker, but upgrade to a later copy if the
    # kept one has no AUM and the later one does (col E == index 3).
    order, best = [], {}
    for row in rows:
        ticker = (row[0] or "").strip().upper()
        if not ticker:
            continue
        aum = (row[3] or "").strip()
        if ticker not in best:
            best[ticker] = row
            order.append(ticker)
        elif not (best[ticker][3] or "").strip() and aum:
            best[ticker] = row

    cleaned = [best[t] for t in order]
    removed = sum(1 for r in rows if (r[0] or "").strip()) - len(cleaned)

    print(f"{TAB}: {sum(1 for r in rows if (r[0] or '').strip())} ticker rows "
          f"-> {len(cleaned)} unique  ({removed} duplicate rows removed)")
    no_aum = [r[0] for r in cleaned if not (r[3] or "").strip()]
    if no_aum:
        print(f"  note: {len(no_aum)} kept row(s) still have no AUM: "
              f"{', '.join(no_aum[:12])}{'...' if len(no_aum) > 12 else ''}")

    if not apply:
        print("\nDry run. Re-run with --apply to write the cleaned list.")
        return 0

    end = START_ROW + len(cleaned) - 1
    ws.batch_clear([f"B{START_ROW}:{LAST_COL}{END_ROW}"])
    ws.batch_update([{
        "range": f"B{START_ROW}:{LAST_COL}{end}",
        "values": cleaned,
    }])
    print(f"\nWrote {len(cleaned)} rows to B{START_ROW}:{LAST_COL}{end}. "
          f"Run database.py to recompute prices and returns.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
