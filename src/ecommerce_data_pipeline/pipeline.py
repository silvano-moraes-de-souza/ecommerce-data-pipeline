"""Extract, load, transform and reconcile one batch, recording the run in meta.pipeline_runs."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from importlib.resources import files
from pathlib import Path

import psycopg
from shopflow_datagen import GenConfig, write

from .load import Method, load_batch

SQL_DIR = files("ecommerce_data_pipeline") / "sql"


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def _statements(text: str) -> list[str]:
    """Split a SQL file into statements. Parameterized queries go through the
    extended protocol, which accepts one statement per call."""
    return [s.strip() for s in text.split(";") if s.strip() and not _only_comments(s)]


def _only_comments(chunk: str) -> bool:
    return all(not line.strip() or line.strip().startswith("--") for line in chunk.splitlines())


def init_db(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(_sql("schema.sql"))


class ReconciliationError(RuntimeError):
    """Raw and mart disagree after the transform."""


@dataclass
class RunResult:
    run_id: int
    batch_id: str
    stage_seconds: dict[str, float] = field(default_factory=dict)
    row_counts: dict[str, int] = field(default_factory=dict)
    checks: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c["ok"] for c in self.checks)


def extract(out: Path, scale: float, seed: int) -> Path:
    """Generate one source snapshot with shopflow-datagen (Parquet part files)."""
    write(GenConfig(scale=scale, seed=seed), out)
    return out


def transform(conn: psycopg.Connection, batch_id: str) -> None:
    with conn.transaction():
        for stmt in _statements(_sql("transform.sql")):
            conn.execute(stmt, {"batch": batch_id}, prepare=False)


def reconcile(conn: psycopg.Connection, batch_id: str) -> list[dict]:
    rows = conn.execute(_sql("checks.sql"), {"batch": batch_id}).fetchall()
    return [{"check": n, "source": int(s), "mart": int(m), "ok": int(s) == int(m)}
            for n, s, m in rows]  # fmt: skip


def run(
    conn: psycopg.Connection, source: Path, batch_id: str, method: Method = "copy"
) -> RunResult:
    """Load ``source`` as ``batch_id``, build the mart and reconcile.

    Raises ReconciliationError if any check fails; the run is recorded either way.
    """
    init_db(conn)
    run_id = conn.execute(
        "INSERT INTO meta.pipeline_runs (batch_id, load_method, status) "
        "VALUES (%s, %s, 'running') RETURNING run_id",
        (batch_id, method),
    ).fetchone()[0]
    conn.commit()
    result = RunResult(run_id, batch_id)
    try:
        t0 = time.perf_counter()
        result.row_counts = load_batch(conn, source, batch_id, method)
        t1 = time.perf_counter()
        transform(conn, batch_id)
        t2 = time.perf_counter()
        result.checks = reconcile(conn, batch_id)
        t3 = time.perf_counter()
        result.stage_seconds = {"load": t1 - t0, "transform": t2 - t1, "reconcile": t3 - t2}
        status = "success" if result.ok else "failed"
        error = None if result.ok else "reconciliation failed"
    except Exception as exc:
        conn.rollback()
        status, error = "failed", f"{type(exc).__name__}: {exc}"
        _finish(conn, result, status, error)
        raise
    _finish(conn, result, status, error)
    if not result.ok:
        bad = [c for c in result.checks if not c["ok"]]
        raise ReconciliationError(f"batch {batch_id}: {bad}")
    return result


def _finish(conn: psycopg.Connection, result: RunResult, status: str, error: str | None) -> None:
    conn.execute(
        "UPDATE meta.pipeline_runs SET status = %s, finished_at = now(), stage_seconds = %s, "
        "row_counts = %s, checks = %s, error = %s WHERE run_id = %s",
        (status, json.dumps(result.stage_seconds), json.dumps(result.row_counts),
         json.dumps(result.checks), error, result.run_id),
    )  # fmt: skip
    conn.commit()
