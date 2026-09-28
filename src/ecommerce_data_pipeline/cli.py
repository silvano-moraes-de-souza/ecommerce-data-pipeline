"""ecom-pipeline command line.

    ecom-pipeline run --scale 1                 generate a snapshot and load it
    ecom-pipeline run --source data/sf1         load an existing snapshot
    ecom-pipeline runs                          show the last runs

The database comes from DATABASE_URL (set in .env or environment).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from .pipeline import ReconciliationError, extract, init_db, run


def _connect() -> psycopg.Connection:
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL is not set (see .env.example)")
    return psycopg.connect(url)


def cmd_run(args: argparse.Namespace) -> int:
    source = args.source
    if source is None:
        source = Path(tempfile.mkdtemp(prefix="ecom-src-"))
        print(f"extract: generating scale {args.scale:g} (seed {args.seed}) into {source}")
        extract(source, args.scale, args.seed)
    batch_id = args.batch_id or f"sf{args.scale:g}-seed{args.seed}"
    with _connect() as conn:
        try:
            result = run(conn, source, batch_id, args.method)
        except ReconciliationError as exc:
            print(f"FAILED: {exc}", file=sys.stderr)
            return 1
    print(f"run {result.run_id} batch {batch_id}: success")
    for table, n in result.row_counts.items():
        print(f"  loaded {table:<12} {n:>12,}")
    for stage, s in result.stage_seconds.items():
        print(f"  {stage:<10} {s:8.2f} s")
    for c in result.checks:
        print(f"  check {c['check']:<26} source={c['source']:>14,} mart={c['mart']:>14,} ok")
    return 0


def cmd_runs(_: argparse.Namespace) -> int:
    with _connect() as conn:
        init_db(conn)
        rows = conn.execute(
            "SELECT run_id, batch_id, load_method, status, started_at, "
            "round(extract(epoch FROM finished_at - started_at)::numeric, 2) "
            "FROM meta.pipeline_runs ORDER BY run_id DESC LIMIT 20"
        ).fetchall()
    for r in rows:
        print(f"{r[0]:>5}  {r[1]:<20} {r[2]:<7} {r[3]:<8} {r[4]:%Y-%m-%d %H:%M:%S}  {r[5]} s")
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # values already in the environment take precedence
    p = argparse.ArgumentParser(
        prog="ecom-pipeline",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="extract (optional), load, transform, reconcile")
    r.add_argument("--source", type=Path, help="existing shopflow-datagen output folder")
    r.add_argument("--scale", type=float, default=1.0)
    r.add_argument("--seed", type=int, default=42)
    r.add_argument("--batch-id")
    r.add_argument("--method", choices=["copy", "insert"], default="copy")
    r.set_defaults(func=cmd_run)
    sub.add_parser("runs", help="list recent runs").set_defaults(func=cmd_runs)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
