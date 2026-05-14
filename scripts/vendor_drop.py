"""
vendor_drop.py - simulate an upstream vendor delivering daily CSVs.

Run this from the HOST (not inside Docker). Uses only Python stdlib.

Examples
--------
    # Drop a single day's file right now
    python scripts/vendor_drop.py --date 2026-05-19

    # Drop a range of days (10 inclusive)
    python scripts/vendor_drop.py --range 2026-05-10:2026-05-19

    # Drop a corrupt file (all amounts = 0) -- used in Q2.6
    python scripts/vendor_drop.py --date 2026-05-18 --corrupt

    # Drop the file after N seconds (simulate a late vendor) -- used in Q2.4
    python scripts/vendor_drop.py --date 2026-05-19 --delay 60

    # Split the daily batch into 3 files -- used in bonus B1
    python scripts/vendor_drop.py --date 2026-05-21 --split 3

    # Slow upload: writes in 3 chunks separated by 10 s -- used in Q2.7
    python scripts/vendor_drop.py --date 2026-05-23 --slow
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path


HERE = Path(__file__).resolve().parent.parent
INCOMING = HERE / "data" / "incoming"

CATEGORIES = ["electronics", "groceries", "fashion", "books", "sports", "home", "toys"]
PAYMENT_METHODS = ["card", "cash", "wallet", "transfer"]
COUNTRIES = ["FR", "DE", "ES", "IT", "BE", "NL"]


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _seeded_rng(d: date, salt: int = 0) -> random.Random:
    """Deterministic generator: same date -> same data (idempotence-friendly).

    This is what makes the lab work nicely with backfills: regenerating a day's
    file produces the exact same CSV.
    """
    return random.Random(int(d.strftime("%Y%m%d")) + salt)


def generate_rows(d: date, n: int = 200, corrupt: bool = False, salt: int = 0) -> list[dict]:
    rng = _seeded_rng(d, salt=salt)
    rows = []
    for i in range(n):
        amount = 0.0 if corrupt else round(rng.uniform(2.0, 250.0), 2)
        rows.append({
            "tx_id": f"{d.strftime('%Y%m%d')}-{salt:02d}-{i:05d}",
            "category": rng.choice(CATEGORIES),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "country": rng.choice(COUNTRIES),
            "amount_eur": amount,
            "ts": f"{d.isoformat()}T{rng.randint(0,23):02d}:{rng.randint(0,59):02d}:00",
        })
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    # atomic rename so the FileSensor never sees a half-written file
    tmp.replace(path)


def write_csv_slow(rows: list[dict], path: Path, chunks: int = 3, pause: float = 10.0) -> None:
    """Write a CSV in `chunks` chunks separated by `pause` seconds, no atomic rename.

    This is intentionally bad: the file grows in front of any naive FileSensor,
    and a downstream task that triggers on existence alone will read a partial file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    chunk_size = max(1, len(rows) // chunks)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        f.flush()
        for i in range(chunks):
            start = i * chunk_size
            end = len(rows) if i == chunks - 1 else (i + 1) * chunk_size
            writer.writerows(rows[start:end])
            f.flush()
            os.fsync(f.fileno())
            size = path.stat().st_size
            print(f"[vendor_drop --slow] wrote chunk {i+1}/{chunks} -> {size} bytes ({path})")
            if i < chunks - 1:
                time.sleep(pause)


def drop_one(d: date, *, corrupt: bool, delay: int, split: int, n: int, slow: bool) -> list[Path]:
    if delay > 0:
        print(f"[vendor_drop] waiting {delay}s before dropping {d.isoformat()}...")
        time.sleep(delay)

    if slow:
        rows = generate_rows(d, n=n, corrupt=corrupt)
        target = INCOMING / f"transactions_{d.isoformat()}.csv"
        write_csv_slow(rows, target)
        print(f"[vendor_drop] slow write finished ({len(rows)} rows total) -> {target}")
        return [target]

    if split <= 1:
        rows = generate_rows(d, n=n, corrupt=corrupt)
        target = INCOMING / f"transactions_{d.isoformat()}.csv"
        write_csv(rows, target)
        print(f"[vendor_drop] wrote {len(rows):>4} rows -> {target}")
        return [target]

    # Split into N files
    paths = []
    per_file = max(1, n // split)
    for i in range(split):
        rows = generate_rows(d, n=per_file, corrupt=corrupt, salt=i + 1)
        target = INCOMING / f"transactions_{d.isoformat()}_part{i+1}.csv"
        write_csv(rows, target)
        paths.append(target)
        print(f"[vendor_drop] wrote {len(rows):>4} rows -> {target}")
    return paths


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Simulate vendor drops for Lab 3.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=_parse_date, help="Single date YYYY-MM-DD")
    g.add_argument("--range", dest="rng", help="Date range YYYY-MM-DD:YYYY-MM-DD inclusive")

    parser.add_argument("--corrupt", action="store_true",
                        help="Make all amounts zero (Q2.6)")
    parser.add_argument("--delay", type=int, default=0,
                        help="Wait N seconds before writing (Q2.4)")
    parser.add_argument("--split", type=int, default=1,
                        help="Split into N files (bonus B1)")
    parser.add_argument("--slow", action="store_true",
                        help="Write the CSV in 3 chunks separated by 10 s pauses (Q2.7)")
    parser.add_argument("--rows", type=int, default=200,
                        help="Approximate number of rows per day (default 200)")
    args = parser.parse_args(argv)

    if args.rng:
        try:
            a, b = args.rng.split(":")
            start, end = _parse_date(a), _parse_date(b)
        except ValueError:
            print("Range must look like YYYY-MM-DD:YYYY-MM-DD", file=sys.stderr)
            return 2
        if end < start:
            print("End date is before start date.", file=sys.stderr)
            return 2
        d = start
        while d <= end:
            drop_one(d, corrupt=args.corrupt, delay=0,
                     split=args.split, n=args.rows, slow=args.slow)
            d += timedelta(days=1)
    else:
        drop_one(args.date, corrupt=args.corrupt, delay=args.delay,
                 split=args.split, n=args.rows, slow=args.slow)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
