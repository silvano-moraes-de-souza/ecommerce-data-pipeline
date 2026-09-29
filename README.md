<p align="center">
  <img src="docs/assets/banner.svg" alt="E-commerce Data Pipeline" width="100%">
</p>

<p align="center">
  <a href="https://github.com/silvano-moraes-de-souza/ecommerce-data-pipeline/actions/workflows/ci.yml"><img src="https://github.com/silvano-moraes-de-souza/ecommerce-data-pipeline/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/python-3.11%2B-2a78d6" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169e1?logo=postgresql&logoColor=white" alt="PostgreSQL 16">
  <img src="https://img.shields.io/badge/license-MIT-52514e" alt="MIT">
  <a href="https://github.com/silvano-moraes-de-souza/30-days-data-eng"><img src="https://img.shields.io/badge/30%20days-day%2001-0b0b0b" alt="30 Days of Data & Software Engineering"></a>
</p>

> Raw e-commerce files in, a PostgreSQL star schema out. Every batch loads in one transaction, reruns change nothing, and the run is only marked successful when row counts and money totals match between source and mart.

<table>
<tr>
<td align="center"><b>2.7x</b><br/>faster raw load with COPY<br/>than batched INSERT</td>
<td align="center"><b>3.9M rows</b><br/>end to end in 248 s<br/>(scale 10, median of 3)</td>
<td align="center"><b>6 checks</b><br/>reconcile counts and cents<br/>between raw and mart</td>
<td align="center"><b>13 tests</b><br/>95% coverage, against<br/>real PostgreSQL 16</td>
</tr>
</table>

<sub>All numbers come from <a href="bench/run.py">bench/run.py</a> and <a href="results/">results/</a>. Details in <a href="#results">Results</a>.</sub>

**Contents:** [Problem](#problem) · [Solution](#solution) · [Quickstart](#quickstart) · [Results](#results) · [How it works](#how-it-works) · [Engineering decisions](#engineering-decisions) · [Tests](#tests) · [Limitations](#limitations)

## Problem

An analytics team needs order, customer, product and payment data in a shape it can query: a star schema with clean dimensions and facts at a known grain. The load has to be safe to rerun after a failure, it has to reject files whose structure changed upstream, and someone has to be able to prove that no order and no cent was lost on the way.

## Solution

```mermaid
flowchart LR
    SRC[shopflow-datagen<br/>Parquet snapshot] -->|column contract| L[Load<br/>COPY per batch,<br/>one transaction]
    L --> RAW[(raw.*<br/>append-only,<br/>tagged by batch)]
    RAW --> T[Transform<br/>SQL upserts]
    T --> DIM[(dim_date<br/>dim_customer<br/>dim_product)]
    T --> FACT[(fact_orders<br/>fact_order_items)]
    RAW --> R{Reconcile<br/>counts + money}
    FACT --> R
    R --> META[(meta.pipeline_runs<br/>timings, checks, errors)]
```

| Layer | Tables | Grain |
|---|---|---|
| raw | customers, products, orders, order_items, payments | one row per source row, tagged with `_batch_id` |
| mart | `dim_date`, `dim_customer`, `dim_product` | one row per day / customer / product (surrogate keys, SCD type 1) |
| mart | `fact_orders` | one row per order, with payment and delivery attached |
| mart | `fact_order_items` | one row per order line, with cost and gross margin |
| meta | `pipeline_runs` | one row per run: stage timings, row counts, reconciliation results, error |

Data comes from [shopflow-datagen](https://github.com/silvano-moraes-de-souza/shopflow-datagen), the shared dataset of this series.

## Quickstart

With Docker:

```bash
cp .env.example .env
docker compose up -d db
docker compose run --rm pipeline run --scale 1
docker compose run --rm pipeline runs
```

Without Docker, against any PostgreSQL:

```bash
uv sync
export DATABASE_URL=postgresql://user:pass@localhost:5432/ecom_pipeline
uv run ecom-pipeline run --scale 1                # generate a snapshot and load it
uv run ecom-pipeline run --source data/sf1        # load an existing snapshot
uv run ecom-pipeline run --scale 1 --method insert
uv run ecom-pipeline runs                         # last 20 runs
```

Tests need no setup on Python 3.11 and 3.12: without `TEST_DATABASE_URL` they start an embedded PostgreSQL 16 ([pgserver](https://github.com/orm011/pgserver)). On 3.13, which pgserver does not support yet, point `TEST_DATABASE_URL` at any PostgreSQL.

```bash
uv run pytest
```

Once loaded, the mart answers questions like this one (real output at scale 1):

```sql
SELECT p.category,
       count(DISTINCT f.order_id)                                              AS orders,
       round(sum(f.line_total_cents) / 100.0)                                  AS revenue_brl,
       round(100.0 * sum(f.gross_margin_cents) / sum(f.line_total_cents), 1)   AS margin_pct
FROM mart.fact_order_items f
JOIN mart.dim_product p USING (product_key)
JOIN mart.dim_date d USING (date_key)
WHERE d.year = 2025
GROUP BY p.category
ORDER BY revenue_brl DESC
LIMIT 5;
```

| category | orders | revenue_brl | margin_pct |
|---|---:|---:|---:|
| electronics | 3,522 | 5,042,638 | 41.1 |
| computers | 1,430 | 3,802,596 | 39.6 |
| fashion | 14,676 | 2,444,972 | 39.6 |
| home | 10,509 | 1,936,824 | 39.7 |
| sports | 6,276 | 1,568,845 | 41.7 |

## Results

Measured with [`bench/run.py`](bench/run.py): 1 warmup and 3 timed runs per case, source files generated beforehand and not timed. Database: embedded PostgreSQL 16 on the same laptop (Intel Tiger Lake-H, 6 cores / 12 threads, 23.8 GB RAM, Windows 11). Raw numbers: [`results/load_copy_vs_insert.json`](results/load_copy_vs_insert.json), [`results/end_to_end_by_scale.json`](results/end_to_end_by_scale.json).

### COPY vs INSERT for the raw load

| Method | Scale | Rows | Median | Min / max | Rows per second |
|---|---:|---:|---:|---:|---:|
| COPY | 0.1 | 40,867 | 0.73 s | 0.69 / 0.81 s | 56,133 |
| INSERT (executemany) | 0.1 | 40,867 | 2.45 s | 2.04 / 2.53 s | 16,706 |
| COPY | 1 | 394,351 | 8.63 s | 8.41 / 9.61 s | 45,715 |
| INSERT (executemany) | 1 | 394,351 | 23.28 s | 17.50 / 26.09 s | 16,940 |

![COPY vs INSERT](docs/assets/load_copy_vs_insert_rows_per_s.png)

COPY is 2.7x faster at scale 1, even with psycopg pipelining the INSERTs. The pipeline uses COPY by default; `--method insert` stays for comparison and for tests, which check that both methods build an identical mart.

### End to end by scale

| Scale | Rows | Median total | Min / max | Load | Transform | Reconcile | Peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.1 | 40,867 | 2.55 s | 2.11 / 2.69 s | 1.07 s | 1.40 s | 0.05 s | 219 MB |
| 1 | 394,351 | 25.24 s | 24.62 / 27.30 s | 10.14 s | 16.70 s | 0.38 s | 245 MB |
| 10 | 3,915,163 | 248.43 s | 240.85 / 276.55 s | 89.52 s | 183.48 s | 3.44 s | 354 MB |

<sub>The stage split comes from the last of the three runs; totals are medians.</sub>

![End to end by scale](docs/assets/end_to_end_by_scale_median_s.png)

Time grows linearly with volume: 10x the rows takes about 10x as long. The load was not the bottleneck: the SQL transform takes about two thirds of the time (66% at scale 10, 61% at scale 1). Its upserts sort each raw table with `DISTINCT ON` and update every conflicting row. Profiling and tuning that is the subject of day 05 (SQL Performance Lab). Python memory stays under 360 MB at 3.9 million rows because Parquet is streamed in batches of 100k rows.

## How it works

**Load.** Each Parquet part file is checked against a fixed column list first. A file with a missing or extra column (the upstream schema drift that shopflow-datagen can simulate) raises `SchemaMismatchError` before any row lands. Record batches of 100k rows are converted to CSV in memory by PyArrow and streamed into `COPY raw.<table> FROM STDIN`. The batch's previous rows are deleted in the same transaction, so a rerun replaces the batch instead of appending to it.

**Transform.** Five upserts in one transaction, keyed on natural keys: `dim_date` from the order date range, `dim_customer` and `dim_product` as SCD type 1 (an update only touches rows whose attributes changed), then `fact_orders` and `fact_order_items`. Duplicate source rows collapse with `DISTINCT ON`, keeping the latest load. Business dates are in Brasilia time.

**Reconcile.** Six checks compare raw and mart for the batch: order and item row counts, order totals and line totals in cents, and the number of facts pointing at the Unknown customer or product. The run is marked `success` only if all of them match, and every check is stored in `meta.pipeline_runs.checks`.

## Engineering decisions

| Decision | Alternatives | Reason |
|---|---|---|
| ELT: land raw, transform in SQL | Transform in pandas before loading | The database does the joins and aggregations it is built for, raw stays available for replays and audits, and the logic lives in versioned SQL. |
| COPY for the load | INSERT, `executemany`, pandas `to_sql` | 2.7x the throughput of pipelined INSERTs in the benchmark above. |
| Delete and reload the batch in one transaction | Upsert every raw row | Raw is a log of what arrived. Replacing the batch atomically makes reruns safe without needing a key on source rows that may themselves be duplicated. |
| Upserts on natural keys in the mart | Truncate and rebuild the mart | Other batches already in the mart are untouched, and a rerun of the same batch is a no-op. |
| Surrogate keys plus an Unknown member (-1) | Natural keys only, or dropping orphan facts | A line whose product is missing still loads and still counts in revenue; the reconciliation check reports it instead of hiding it. |
| Money as integer cents | `numeric` or `float` | Exact sums, and the same representation as the source. |
| Fixed UTC-3 offset for business dates | `AT TIME ZONE 'America/Sao_Paulo'` | Brazil has had no daylight saving time since 2019, so the offset is exact for this data. It also works on PostgreSQL builds without a time zone database, such as the embedded server used in tests on Windows. |
| Real PostgreSQL in tests | SQLite or mocks | Upserts, `DISTINCT ON`, identity columns and COPY behave differently or do not exist elsewhere. pgserver locally and a service container in CI keep the tests honest. |

## Tests

13 tests (9 for the pipeline, 4 for the shared benchmark tooling), 95% coverage, all pipeline tests against PostgreSQL 16:

| Test | What it proves |
|---|---|
| run and reconcile | every table loads, row counts match the source manifest, the run is recorded as success |
| idempotent rerun | running the same batch twice leaves raw and mart counts unchanged |
| COPY vs INSERT | both methods produce the same order count and totals |
| star schema invariants | every fact date exists in `dim_date`, margin = revenue − cost on every line, `item_count` matches the item fact, no negative delivery times, dates in Brasilia time |
| dirty data | duplicated orders collapse to one fact each; orphan products load against the Unknown member and fail reconciliation |
| schema drift | a renamed column is rejected, nothing is loaded, the run is recorded as failed |
| reconciliation | deleting one fact row is detected by exactly the two checks it should trip |

CI runs the suite on Python 3.11, 3.12 and 3.13 against a PostgreSQL service container, then runs the pipeline twice through `docker compose` to check the rerun.

## Limitations

- The transform is the bottleneck (66% of the time at scale 10) and has not been tuned yet.
- Each run is a full snapshot of the source. Incremental loading with change data capture is day 03.
- Dimensions are SCD type 1: when a product's cost changes, historical margins are recomputed with the new cost on the next load of those lines. Type 2 dimensions would keep history.
- Data quality beyond structure and reconciliation (nulls, outliers, value ranges) is day 02.
- Benchmarks ran on an embedded PostgreSQL on Windows with default settings, not on a tuned server.

## Project structure

```
src/ecommerce_data_pipeline/
  sql/schema.sql     raw, mart and meta DDL, Unknown members
  sql/transform.sql  star schema upserts
  sql/checks.sql     reconciliation queries
  load.py            COPY and INSERT loaders, column contract
  pipeline.py        run orchestration and run log
  cli.py             ecom-pipeline command
tests/               integration tests on PostgreSQL
bench/               benchmark scripts and harness
results/             raw benchmark JSON
docs/assets/         charts generated from results/
```

## Part of the series

This is day 01 of [30 Days of Data & Software Engineering](https://github.com/silvano-moraes-de-souza/30-days-data-eng). Previous: day 00, [shopflow-datagen](https://github.com/silvano-moraes-de-souza/shopflow-datagen). Next: day 02, Data Quality Engine.

## Author

Silvano Moraes de Souza · [GitHub](https://github.com/silvano-moraes-de-souza) · [Portfolio](https://silvanomsouza.vercel.app/) · [LinkedIn](https://www.linkedin.com/in/silvano-moraes-de-souza)
