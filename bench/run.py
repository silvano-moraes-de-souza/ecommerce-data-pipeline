"""Load strategy and end-to-end benchmarks against a real PostgreSQL 16.

    uv run python -m bench.run

Uses TEST_DATABASE_URL if set, otherwise starts an embedded PostgreSQL 16
(pgserver) in a temporary folder. Source data is generated once per scale and
is not part of the timings.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import psycopg
from shopflow_datagen import GenConfig, write

from bench.harness import measure, save
from bench.plot import plot
from ecommerce_data_pipeline.load import load_batch
from ecommerce_data_pipeline.pipeline import init_db, run

LOAD_SCALES = [0.1, 1]
E2E_SCALES = [0.1, 1, 10]


def _database_url() -> tuple[str, str]:
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        return url, "provided PostgreSQL"
    import pgserver  # noqa: PLC0415

    server = pgserver.get_server(tempfile.mkdtemp(prefix="ecom-bench-pg-"), cleanup_mode="stop")
    return server.get_uri(), "embedded PostgreSQL 16 (pgserver), same machine"


def _reset(conn: psycopg.Connection) -> None:
    for schema in ("raw", "mart", "meta"):
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.commit()
    init_db(conn)


def main() -> None:
    url, where = _database_url()
    sources = {}
    for scale in sorted(set(LOAD_SCALES + E2E_SCALES)):
        out = Path(tempfile.mkdtemp(prefix=f"ecom-src-{scale:g}-"))
        write(GenConfig(scale=scale), out)
        sources[scale] = out

    with psycopg.connect(url) as conn:
        load_cases = []
        for scale in LOAD_SCALES:
            for method in ("copy", "insert"):

                def fn(scale=scale, method=method):
                    counts = load_batch(conn, sources[scale], "bench", method)
                    return {"rows": sum(counts.values())}

                case = measure(fn, label=f"{method} · scale {scale:g}",
                               params={"scale": scale, "method": method},
                               runs=3, setup=lambda: _reset(conn))  # fmt: skip
                case.extra["rows_per_s"] = case.extra["rows"] / case.median_s
                load_cases.append(case)
                print(f"load {case.label}: {case.median_s:.2f} s")
        path = save("load_copy_vs_insert", load_cases,
                    notes=f"Raw layer load only (5 tables). Database: {where}.")  # fmt: skip
        print(plot(path, "rows_per_s", title="Raw load throughput: COPY vs INSERT (rows/s)"))

        e2e_cases = []
        for scale in E2E_SCALES:

            def fn(scale=scale):
                result = run(conn, sources[scale], "bench", "copy")
                return {
                    "rows": sum(result.row_counts.values()),
                    **{f"{k}_s": v for k, v in result.stage_seconds.items()},
                }

            case = measure(fn, label=f"scale {scale:g}", params={"scale": scale},
                           runs=3, setup=lambda: _reset(conn))  # fmt: skip
            e2e_cases.append(case)
            print(f"e2e {case.label}: {case.median_s:.2f} s {case.extra}")
        path = save("end_to_end_by_scale", e2e_cases,
                    notes=f"load (COPY) + transform + reconcile. Database: {where}.")  # fmt: skip
        print(plot(path, "median_s", title="End-to-end pipeline time by scale"))


if __name__ == "__main__":
    main()
