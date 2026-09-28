-- Reconciliation between raw and mart for one batch.
-- Each row: check name, value on the source side, value on the mart side.
WITH ro AS (
    SELECT DISTINCT ON (order_id) order_id, total_cents
    FROM raw.orders WHERE _batch_id = %(batch)s
    ORDER BY order_id, _loaded_at DESC
), ri AS (
    SELECT DISTINCT ON (order_item_id) order_item_id, line_total_cents
    FROM raw.order_items WHERE _batch_id = %(batch)s
    ORDER BY order_item_id, _loaded_at DESC
), fo AS (
    SELECT order_id, total_cents FROM mart.fact_orders WHERE _batch_id = %(batch)s
), fi AS (
    SELECT order_item_id, line_total_cents, product_key, customer_key
    FROM mart.fact_order_items WHERE _batch_id = %(batch)s
)
SELECT 'orders_row_count', (SELECT count(*) FROM ro), (SELECT count(*) FROM fo)
UNION ALL
SELECT 'orders_total_cents', (SELECT coalesce(sum(total_cents), 0) FROM ro), (SELECT coalesce(sum(total_cents), 0) FROM fo)
UNION ALL
SELECT 'items_row_count', (SELECT count(*) FROM ri), (SELECT count(*) FROM fi)
UNION ALL
SELECT 'items_line_total_cents', (SELECT coalesce(sum(line_total_cents), 0) FROM ri), (SELECT coalesce(sum(line_total_cents), 0) FROM fi)
UNION ALL
SELECT 'items_unknown_product', 0, (SELECT count(*) FROM fi WHERE product_key = -1)
UNION ALL
SELECT 'orders_unknown_customer', 0, (SELECT count(*) FROM mart.fact_orders WHERE _batch_id = %(batch)s AND customer_key = -1);
