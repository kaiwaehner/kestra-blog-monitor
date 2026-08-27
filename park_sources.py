#!/usr/bin/env python3
"""Move retired sources into a parked_sources block instead of deleting them.

blog_monitor.py reads only config["sources"], so anything in parked_sources is
invisible to the digest run and cannot hang it. check_sources.py reads both, so
a parked source stays measured and stays in the record.

Deleting a broken source loses the reason it was removed. Three months later
nobody remembers why EnvisionSCADA is missing or whether it is worth retrying.

Run it in a directory that has config.json plus a config.json.bak* to read the
original entries from.

    python3 park_sources.py --dry-run
    python3 park_sources.py
"""
import argparse
import glob
import json
import shutil
import sys
from datetime import date
from pathlib import Path

BASE = Path(__file__).resolve().parent

# name substring -> why it was parked and what a fix would need
PARK = {
    "EnvisionSCADA": {
        "reason": "Renders client-side, no feed, no sitemap. Opened a TCP "
                  "connection and then went silent, which hung the whole run "
                  "because as_completed() has no timeout. 13 stacked sockets "
                  "observed.",
        "fix": "Needs a headless browser, or find a Substack/Medium/Ghost mirror.",
    },
    "ServiceNow": {
        "reason": "No data within its own 45s budget. Same hang pattern, "
                  "single socket. Failing in the digest for weeks.",
        "fix": "Check for an RSS feed or sitemap.xml on servicenow.com, or drop.",
    },
}


def find_backup(cfg_path):
    cands = sorted(glob.glob(str(cfg_path) + ".bak*"), reverse=True)
    if not cands:
        sys.exit(f"No backup found next to {cfg_path}. Expected {cfg_path}.bak*")
    return cands[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(BASE / "config.json"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg_path = Path(args.config)
    cfg = json.load(open(cfg_path))
    backup = find_backup(cfg_path)
    old = json.load(open(backup))
    print(f"Reading original entries from {Path(backup).name}")

    live_names = {s["name"] for s in cfg["sources"]}
    parked = {s["name"]: s for s in cfg.get("parked_sources", [])}
    added = []

    for src in old["sources"]:
        for needle, meta in PARK.items():
            if needle.lower() not in src["name"].lower():
                continue
            if src["name"] in live_names:
                print(f"  still live, skipping: {src['name']}")
                break
            if src["name"] in parked:
                print(f"  already parked:       {src['name']}")
                break
            entry = dict(src)
            entry["parked_on"] = date.today().isoformat()
            entry["parked_reason"] = meta["reason"]
            entry["parked_fix"] = meta["fix"]
            parked[src["name"]] = entry
            added.append(src["name"])
            print(f"  parking:              {src['name']}")
            break

    if not added:
        print("\nNothing to do.")
        return 0

    cfg["parked_sources"] = [parked[k] for k in sorted(parked)]

    print(f"\nsources: {len(cfg['sources'])}   "
          f"parked_sources: {len(cfg['parked_sources'])}")

    if args.dry_run:
        print("\nDry run, nothing written.")
        return 0

    shutil.copy2(cfg_path, str(cfg_path) + f".prepark.{date.today().isoformat()}")
    with open(cfg_path, "w") as fh:
        json.dump(cfg, fh, indent=2, ensure_ascii=False)
    print(f"\nWritten. blog_monitor.py ignores parked_sources, so the digest "
          f"still fetches {len(cfg['sources'])} sources.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
