"""
Run the sheet-update jobs concurrently, and optionally the live ticker.

    python run_all.py                # database.py + stocks_data.py
    python run_all.py --with-live    # also start the live market ticker
    python run_all.py --live-only    # ONLY the live ticker
    python run_all.py -- --us-only   # forward args to stocks_data.py

Each script's stdout/stderr is streamed live, prefixed with its label so
the output streams remain readable when interleaved. Exit code is the max
of the child exit codes (non-zero if any failed).

Why the ticker is opt-in
------------------------
database.py and stocks_data.py are batch jobs: they run once and exit, so
run_all can wait on both and report a summary. The live ticker
(Random/scripts/ticker.py) is a different animal — it holds a Zerodha
WebSocket open and streams into Firestore until the market closes, so it
blocks for hours by design. Including it by default would leave the plain
`python run_all.py` appearing to hang long after the sheets were written.

Its exit codes are also meaningful rather than pass/fail:
    0 = clean shutdown (market closed)
    1 = crash — a supervisor may restart it
    2 = Zerodha token missing/expired — needs `python zerodha.py`, so
        restarting on a loop would be pointless
A code of 2 is surfaced as-is so callers can tell "needs the morning login"
apart from "genuinely broke".
"""

import os
import subprocess
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

# The ticker lives in the StockPulse repo alongside this one and resolves
# both zerodha.py and zerodha_access_token.txt relative to its own root, so
# it has to be launched with that directory as cwd.
LIVE_ROOT   = ROOT / "Random"
LIVE_SCRIPT = LIVE_ROOT / "scripts" / "ticker.py"


def _live_python() -> str:
    """Interpreter for the ticker.

    The ticker needs firebase-admin and kiteconnect, which live in the
    StockPulse venv rather than this project's — running it under
    sys.executable fails immediately on `import firebase_admin`. Prefer that
    venv (this is the same choice run_ticker.ps1 makes), falling back to the
    current interpreter so a single shared environment still works.
    """
    venv_python = LIVE_ROOT / ".venv" / "Scripts" / "python.exe"      # Windows
    if not venv_python.exists():
        venv_python = LIVE_ROOT / ".venv" / "bin" / "python"          # POSIX
    return str(venv_python) if venv_python.exists() else PYTHON

BATCH_JOBS = [
    ("database", ["database.py"]),
    ("stocks",   ["stocks_data.py"]),
]


def _stream(label: str, proc: subprocess.Popen, lock: threading.Lock):
    for raw in proc.stdout:
        line = raw.rstrip("\n")
        with lock:
            print(f"[{label}] {line}", flush=True)


def _build_jobs(with_live: bool, live_only: bool, extra_stocks_args: list):
    """(label, cmd, cwd) for each job to launch."""
    jobs = []
    if not live_only:
        for label, argv in BATCH_JOBS:
            cmd = [PYTHON, "-u", *argv]
            if label == "stocks":
                cmd += extra_stocks_args
            jobs.append((label, cmd, ROOT))

    if with_live or live_only:
        if not LIVE_SCRIPT.exists():
            print(f"[run_all] live ticker not found at {LIVE_SCRIPT} — skipping.",
                  flush=True)
        else:
            jobs.append(("live", [_live_python(), "-u", str(LIVE_SCRIPT)], LIVE_ROOT))
    return jobs


def main() -> int:
    # Anything after a literal "--" gets forwarded to stocks_data.py
    extra_stocks_args: list[str] = []
    argv = sys.argv[1:]
    if "--" in sys.argv:
        idx = sys.argv.index("--")
        extra_stocks_args = sys.argv[idx + 1:]
        argv = sys.argv[1:idx]

    with_live = "--with-live" in argv
    live_only = "--live-only" in argv

    jobs = _build_jobs(with_live, live_only, extra_stocks_args)
    if not jobs:
        print("[run_all] nothing to run.", flush=True)
        return 0

    if with_live or live_only:
        print("[run_all] the live ticker streams until the market closes — "
              "this will not return until then (Ctrl+C to stop).", flush=True)

    procs: list[tuple[str, subprocess.Popen]] = []
    for label, cmd, cwd in jobs:
        # The ticker prints status glyphs; force UTF-8 so a cp1252 console
        # cannot kill it mid-stream (same reason run_ticker.ps1 sets these).
        env = None
        if label == "live":
            env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
        procs.append((label, proc))
        print(f"[run_all] started {label} (pid={proc.pid}): {' '.join(cmd[1:])}", flush=True)

    lock = threading.Lock()
    threads = [
        threading.Thread(target=_stream, args=(label, proc, lock), daemon=True)
        for label, proc in procs
    ]
    for t in threads:
        t.start()

    exit_codes: dict[str, int] = {}
    for label, proc in procs:
        exit_codes[label] = proc.wait()
    for t in threads:
        t.join()

    print("\n[run_all] summary:")
    for label, code in exit_codes.items():
        if code == 0:
            status = "ok"
        elif label == "live" and code == 2:
            # Not a crash: the Zerodha token needs the morning login. Say so,
            # because a restart loop would never fix it.
            status = "token expired — run 'python zerodha.py' (exit 2)"
        else:
            status = f"FAILED (exit {code})"
        print(f"  {label}: {status}")
    return max(exit_codes.values())


if __name__ == "__main__":
    sys.exit(main())
