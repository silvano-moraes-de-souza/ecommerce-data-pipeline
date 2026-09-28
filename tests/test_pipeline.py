import json

import pytest

from ecommerce_data_pipeline import cli
from ecommerce_data_pipeline.load import SchemaMismatchError
from ecommerce_data_pipeline.pipeline import ReconciliationError, _statements, reconcile, run

MART_TABLES = ("dim_customer", "dim_product", "dim_date", "fact_orders", "fact_order_items")


def _counts(conn):
    return {t: conn.execute(f"SELECT count(*) FROM mart.{t}").fetchone()[0] for t in MART_TABLES}


def _manifest_rows(source):
    manifest = json.loads((source / "_manifest.json").read_text())
    return {t: info["rows"] for t, info in manifest["tables"].items()}


def test_run_loads_everything_and_reconciles(conn, source):
    result = run(conn, source, "b1")
    assert result.ok
    assert result.row_counts == _manifest_rows(source)
    assert {c["check"] for c in result.checks} >= {"orders_row_count", "orders_total_cents"}
    status = conn.execute("SELECT status FROM meta.pipeline_runs WHERE run_id = %s",
                          (result.run_id,)).fetchone()[0]  # fmt: skip
    assert status == "success"


def test_rerunning_a_batch_is_idempotent(conn, source):
    run(conn, source, "b1")
    first = _counts(conn)
    raw_first = conn.execute("SELECT count(*) FROM raw.orders").fetchone()[0]
    run(conn, source, "b1")
    assert _counts(conn) == first
    assert conn.execute("SELECT count(*) FROM raw.orders").fetchone()[0] == raw_first
    assert conn.execute("SELECT count(*) FROM meta.pipeline_runs").fetchone()[0] == 2


def test_copy_and_insert_build_the_same_mart(conn, source):
    run(conn, source, "b-copy", method="copy")
    by_copy = conn.execute(
        "SELECT count(*), sum(total_cents), sum(item_count) FROM mart.fact_orders"
    ).fetchone()
    for schema in ("raw", "mart", "meta"):
        conn.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.commit()
    run(conn, source, "b-insert", method="insert")
    by_insert = conn.execute(
        "SELECT count(*), sum(total_cents), sum(item_count) FROM mart.fact_orders"
    ).fetchone()
    assert by_copy == by_insert


def test_star_schema_invariants(conn, source):
    run(conn, source, "b1")
    q = conn.execute
    # every fact date exists in dim_date (also enforced by the FK)
    assert q("SELECT count(*) FROM mart.fact_order_items f LEFT JOIN mart.dim_date d "
             "USING (date_key) WHERE d.date_key IS NULL").fetchone()[0] == 0  # fmt: skip
    # margin is price minus cost, line by line
    bad_margin = q(
        "SELECT count(*) FROM mart.fact_order_items "
        "WHERE gross_margin_cents <> line_total_cents - line_cost_cents"
    ).fetchone()[0]
    assert bad_margin == 0
    # item_count on the order matches the item fact
    assert (
        q("""SELECT count(*) FROM mart.fact_orders o
                JOIN (SELECT order_id, count(*) n FROM mart.fact_order_items GROUP BY 1) i
                USING (order_id) WHERE o.item_count <> i.n""").fetchone()[0]
        == 0
    )
    # deliveries never happen before the order
    assert q("SELECT count(*) FROM mart.fact_orders WHERE delivery_days < 0").fetchone()[0] == 0
    # business date is Brasilia time: an order at 01:00 UTC belongs to the previous day
    late = q("""SELECT count(*) FROM mart.fact_orders
                WHERE date_key <> to_char(ordered_at AT TIME ZONE INTERVAL '-03:00',
                                          'YYYYMMDD')::int""").fetchone()[0]
    assert late == 0


def test_duplicates_are_collapsed_and_orphans_fail_reconciliation(conn, dirty_source):
    with pytest.raises(ReconciliationError, match="items_unknown_product"):
        run(conn, dirty_source, "dirty")
    raw_orders, distinct_orders = conn.execute(
        "SELECT count(*), count(DISTINCT order_id) FROM raw.orders"
    ).fetchone()
    assert raw_orders > distinct_orders  # duplicates reached raw
    assert conn.execute("SELECT count(*) FROM mart.fact_orders").fetchone()[0] == distinct_orders
    unknown = conn.execute(
        "SELECT count(*) FROM mart.fact_order_items WHERE product_key = -1"
    ).fetchone()[0]
    assert unknown > 0  # orphans kept, pointing at the Unknown member
    status, checks = conn.execute(
        "SELECT status, checks FROM meta.pipeline_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    assert status == "failed"
    assert any(c["check"] == "items_unknown_product" and not c["ok"] for c in checks)


def test_schema_drift_is_rejected_and_nothing_is_loaded(conn, drifted_source):
    with pytest.raises(SchemaMismatchError, match="sales_channel"):
        run(conn, drifted_source, "drift")
    assert conn.execute("SELECT count(*) FROM raw.orders").fetchone()[0] == 0
    status, error = conn.execute(
        "SELECT status, error FROM meta.pipeline_runs ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    assert status == "failed" and "SchemaMismatchError" in error


def test_reconcile_detects_a_missing_fact_row(conn, source):
    run(conn, source, "b1")
    conn.execute("DELETE FROM mart.fact_order_items WHERE order_item_id = "
                 "(SELECT min(order_item_id) FROM mart.fact_order_items)")  # fmt: skip
    bad = {c["check"] for c in reconcile(conn, "b1") if not c["ok"]}
    assert bad == {"items_row_count", "items_line_total_cents"}


def test_statement_splitter_skips_comment_only_chunks():
    sql = "-- header\nSELECT 1;\n-- only a comment\n;\nSELECT 2;"
    assert _statements(sql) == ["-- header\nSELECT 1", "SELECT 2"]


def test_cli_run_and_runs(db_url, conn, source, monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", db_url)
    assert cli.main(["run", "--source", str(source), "--batch-id", "cli"]) == 0
    assert cli.main(["runs"]) == 0
    out = capsys.readouterr().out
    assert "batch cli: success" in out and "orders_total_cents" in out
