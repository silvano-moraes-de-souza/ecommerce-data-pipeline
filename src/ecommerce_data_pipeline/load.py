"""Load Parquet part files into the raw layer.

Two strategies, kept side by side so the benchmark can compare them:

- ``copy``: stream each Arrow record batch as CSV into ``COPY ... FROM STDIN``
- ``insert``: ``executemany`` of parameterized INSERTs (psycopg pipelines them)

Both are idempotent per batch: the batch's rows are deleted and reloaded in
the same transaction, so a rerun never duplicates data.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg import sql

Method = Literal["copy", "insert"]

# The contract with the source. A file with other columns is rejected.
COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "full_name", "email", "state", "signup_at"],
    "products": ["product_id", "sku", "name", "category", "brand", "price_cents", "cost_cents",
                 "active"],
    "orders": ["order_id", "customer_id", "ordered_at", "status", "channel", "subtotal_cents",
               "discount_cents", "shipping_cents", "total_cents", "delivered_at"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price_cents",
                    "line_total_cents"],
    "payments": ["payment_id", "order_id", "method", "installments", "status", "amount_cents",
                 "paid_at"],
}  # fmt: skip
TABLES = list(COLUMNS)

CSV_OPTIONS = pacsv.WriteOptions(include_header=False)


class SchemaMismatchError(ValueError):
    """A source file does not have the columns the raw table expects."""


def _batches(folder: Path, table: str, batch_size: int) -> Iterator[pa.RecordBatch]:
    expected = COLUMNS[table]
    for part in sorted(folder.glob("part-*.parquet")):
        pf = pq.ParquetFile(part)
        found = pf.schema_arrow.names
        if set(found) != set(expected):
            missing = sorted(set(expected) - set(found))
            extra = sorted(set(found) - set(expected))
            raise SchemaMismatchError(
                f"{table}/{part.name}: missing {missing or 'none'}, unexpected {extra or 'none'}"
            )
        yield from pf.iter_batches(batch_size=batch_size, columns=expected)


def _with_batch(rb: pa.RecordBatch, batch_id: str) -> pa.RecordBatch:
    return rb.append_column("_batch_id", pa.array([batch_id] * rb.num_rows, pa.string()))


def _load_copy(
    cur: psycopg.Cursor, table: str, batches: Iterator[pa.RecordBatch], batch_id: str
) -> int:
    cols = [*COLUMNS[table], "_batch_id"]
    stmt = sql.SQL("COPY raw.{} ({}) FROM STDIN WITH (FORMAT csv)").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, cols))
    )
    rows = 0
    with cur.copy(stmt) as copy:
        for rb in batches:
            buf = io.BytesIO()
            pacsv.write_csv(_with_batch(rb, batch_id), buf, CSV_OPTIONS)
            copy.write(buf.getvalue())
            rows += rb.num_rows
    return rows


def _load_insert(
    cur: psycopg.Cursor, table: str, batches: Iterator[pa.RecordBatch], batch_id: str
) -> int:
    cols = [*COLUMNS[table], "_batch_id"]
    stmt = sql.SQL("INSERT INTO raw.{} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )
    rows = 0
    for rb in batches:
        columns = [rb.column(i).to_pylist() for i in range(rb.num_columns)]
        cur.executemany(stmt, [(*r, batch_id) for r in zip(*columns, strict=True)])
        rows += rb.num_rows
    return rows


def load_batch(
    conn: psycopg.Connection,
    source: Path,
    batch_id: str,
    method: Method = "copy",
    batch_size: int = 100_000,
) -> dict[str, int]:
    """Load every table under ``source`` (``<table>/part-*.parquet``) as one batch.

    Runs in a single transaction: either the whole batch lands or none of it.
    Returns rows loaded per table.
    """
    loader = _load_copy if method == "copy" else _load_insert
    counts: dict[str, int] = {}
    with conn.transaction(), conn.cursor() as cur:
        for table in TABLES:
            cur.execute(
                sql.SQL("DELETE FROM raw.{} WHERE _batch_id = %s").format(sql.Identifier(table)),
                (batch_id,),
            )
            counts[table] = loader(cur, table, _batches(source / table, table, batch_size),
                                   batch_id)  # fmt: skip
    return counts
