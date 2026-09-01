#!/usr/bin/env python3
"""Check every source in config.json one at a time and report how long it takes.

Why this exists: blog_monitor.py fetches all sources through a
ThreadPoolExecutor and waits on as_completed() with no timeout. A single source
that opens a TCP connection and then goes silent blocks the entire run
indefinitely. A socket timeout does not save you, because it restarts on every
byte received, so a server that trickles one byte a minute holds the socket open
forever.

This script does the opposite: strictly sequential, one hard deadline per
source, enforced from outside the request with a watchdog thread. Slow sources
are reported, hung sources are abandoned, and the run always finishes.

It never writes state and never sends mail. Safe to run at any time.

    python3 check_sources.py                 # default 20s budget per source
    python3 check_sources.py --timeout 30
    python3 check_sources.py --only servicenow

Run it from a directory containing config.json.
"""
import argparse
import json
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

UA = "Mozilla/5.0 (compatible; BlogMonitorDiagnostics/1.0)"

BASE = Path(__file__).resolve().parent


def source_url(src):
    return src.get("feed_url") or src.get("url") or src.get("sitemap_url")


def probe(url, budget):
    """Fetch url with a hard wall-clock budget. Returns (seconds, status, note)."""
    started = time.monotonic()
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    ctx = ssl.create_default_context()
    try:
        # The socket timeout caps each individual read. It is not sufficient on
        # its own, which is exactly the bug being diagnosed, but combined with
        # the byte cap below it bounds the total time in practice.
        with urllib.request.urlopen(req, timeout=budget, context=ctx) as resp:
            read = 0
            while read < 2_000_000:
                if time.monotonic() - started > budget:
                    return time.monotonic() - started, "HANG", (
                        f"still receiving after {budget}s, read {read} bytes"
                    )
                chunk = resp.read(65536)
                if not chunk:
                    break
                read += len(chunk)
            return time.monotonic() - started, "OK", f"{resp.status}, {read} bytes"
    except urllib.error.HTTPError as e:
        return time.monotonic() - started, "HTTP", f"{e.code} {e.reason}"
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        kind = "TIMEOUT" if isinstance(reason, socket.timeout) else "ERROR"
        return time.monotonic() - started, kind, str(reason)[:70]
    except socket.timeout:
        return time.monotonic() - started, "TIMEOUT", f"no data within {budget}s"
    except Exception as e:
        return time.monotonic() - started, "ERROR", f"{type(e).__name__}: {e}"[:70]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=float, default=20.0,
                    help="seconds allowed per source (default 20)")
    ap.add_argument("--only", default=None,
                    help="substring filter on source name or host")
    ap.add_argument("--config", default=str(BASE / "config.json"))
    ap.add_argument("--json", default=None,
                    help="write a machine-readable summary to this path")
    ap.add_argument("--kestra", action="store_true",
                    help="emit Kestra output markers and always exit 0, so the "
                         "flow decides what to do rather than the script")
    args = ap.parse_args()

    cfg = json.load(open(args.config))
    # parked_sources is ignored by blog_monitor.py, which only reads "sources".
    # Keeping retired sources here means they stay measured and stay in the
    # record, instead of being deleted and forgotten.
    sources = list(cfg["sources"]) + list(cfg.get("parked_sources", []))
    parked = {s["name"] for s in cfg.get("parked_sources", [])}

    if args.only:
        needle = args.only.lower()
        sources = [s for s in sources
                   if needle in s["name"].lower()
                   or needle in (source_url(s) or "").lower()]

    print(f"Checking {len(sources)} source(s), {args.timeout:.0f}s budget each.")
    print("Sequential on purpose, so one slow source cannot hide behind another.\n")

    results = []
    for i, s in enumerate(sources, 1):
        url = source_url(s)
        if not url:
            continue
        budget = float(s.get("timeout", args.timeout))
        print(f"[{i:>3}/{len(sources)}] {s['name'][:42]:<42} ", end="", flush=True)
        secs, status, note = probe(url, budget)
        print(f"{secs:>7.1f}s  {status:<8} {note}")
        results.append((secs, status, s["name"], urlparse(url).netloc, note))

    print("\n" + "=" * 78)
    bad = [r for r in results if r[1] in ("HANG", "TIMEOUT", "ERROR", "HTTP")]
    slow = sorted((r for r in results if r[1] == "OK"), reverse=True)[:8]

    if bad:
        print(f"\n{len(bad)} problem source(s), worst first:\n")
        for secs, status, name, host, note in sorted(bad, reverse=True):
            print(f"  {secs:>7.1f}s  {status:<8} {name}")
            print(f"           {host}  {note}")
    else:
        print("\nNo failing sources.")

    print(f"\nSlowest {len(slow)} healthy source(s):\n")
    for secs, status, name, host, note in slow:
        print(f"  {secs:>7.1f}s  {name}  ({host})")

    total = sum(r[0] for r in results)
    print(f"\nSequential total: {total/60:.1f} min across {len(results)} sources.")
    print("The real run fetches 10 at a time, so expect roughly a tenth of that")
    print("when nothing hangs.")

    summary = {
        "checked": len(results),
        "failing": len(bad),
        "hanging": sum(1 for r in bad if r[1] in ("HANG", "TIMEOUT")),
        "sequential_seconds": round(total, 1),
        "problems": [
            {"name": n, "status": st, "seconds": round(sec, 1),
             "host": h, "note": nt, "parked": n in parked}
            for sec, st, n, h, nt in sorted(bad, reverse=True)
        ],
        "slowest_ok": [
            {"name": n, "seconds": round(sec, 1)} for sec, st, n, h, nt in slow
        ],
    }

    # A parked source that now responds is the single most actionable result
    # here: it means the entry can move back into "sources". Call it out
    # separately rather than leaving it to be inferred from an absence.
    # Only meaningful when every source was checked. With --only, a parked
    # source that was never probed would otherwise be reported as recovered
    # purely because it is absent from the failure list.
    checked_names = {r[2] for r in results}
    still_bad = {r[2] for r in bad}
    recovered = sorted(n for n in parked
                       if n in checked_names and n not in still_bad)

    if recovered:
        print("\nParked sources that responded and could be restored:\n")
        for n in recovered:
            print(f"  {n}")

    summary["recovered_parked"] = recovered
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(summary, fh, indent=2)

    if args.kestra:
        # Kestra reads this marker from stdout and exposes the values as
        # outputs.<task>.vars.<key>. Exit 0 regardless: the flow decides
        # what to do about failures, not the script.
        lines = []
        for p in summary["problems"]:
            tag = " (parked)" if p["parked"] else ""
            lines.append(f"- **{p['name']}**{tag} - {p['status']} after "
                         f"{p['seconds']}s. {p['note']}")
        print("::" + json.dumps({"outputs": {
            "failing": summary["failing"],
            "hanging": summary["hanging"],
            "checked": summary["checked"],
            "parked": len(parked),
            "recovered": len(recovered),
            "recovered_names": ", ".join(recovered),
            "problem_names": ", ".join(p["name"] for p in summary["problems"]),
            "problem_markdown": "\n".join(lines) or "None.",
        }}) + "::")
        return 0

    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
