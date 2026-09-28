-- Build the star schema for one batch. Every statement is an upsert keyed on
-- the natural key, so running the same batch twice leaves the mart unchanged.
-- Business dates use Brasilia time, the same calendar the source is modeled on.
-- Brazil has had no DST since 2019, so a fixed UTC-3 offset is exact for this
-- data and does not depend on the server's time zone database.

-- dim_date: every day between the first and last order of the batch
INSERT INTO mart.dim_date (date_key, full_date, year, quarter, month, day, iso_weekday, is_weekend)
SELECT to_char(d, 'YYYYMMDD')::int,
       d::date,
       extract(year FROM d),
       extract(quarter FROM d),
       extract(month FROM d),
       extract(day FROM d),
       extract(isodow FROM d),
       extract(isodow FROM d) IN (6, 7)
FROM generate_series(
        (SELECT min((ordered_at AT TIME ZONE INTERVAL '-03:00')::date) FROM raw.orders WHERE _batch_id = %(batch)s),
        (SELECT max((ordered_at AT TIME ZONE INTERVAL '-03:00')::date) FROM raw.orders WHERE _batch_id = %(batch)s),
        interval '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- dim_customer (SCD type 1: latest attributes win)
INSERT INTO mart.dim_customer (customer_id, full_name, email, state, signup_at, updated_at)
SELECT DISTINCT ON (customer_id) customer_id, full_name, email, state, signup_at, now()
FROM raw.customers
WHERE _batch_id = %(batch)s
ORDER BY customer_id, _loaded_at DESC
ON CONFLICT (customer_id) DO UPDATE SET
    full_name = EXCLUDED.full_name,
    email     = EXCLUDED.email,
    state     = EXCLUDED.state,
    signup_at = EXCLUDED.signup_at,
    updated_at = now()
WHERE (mart.dim_customer.full_name, mart.dim_customer.email, mart.dim_customer.state, mart.dim_customer.signup_at)
      IS DISTINCT FROM (EXCLUDED.full_name, EXCLUDED.email, EXCLUDED.state, EXCLUDED.signup_at);

-- dim_product (SCD type 1)
INSERT INTO mart.dim_product (product_id, sku, name, category, brand, price_cents, cost_cents, active, updated_at)
SELECT DISTINCT ON (product_id) product_id, sku, name, category, brand, price_cents, cost_cents, active, now()
FROM raw.products
WHERE _batch_id = %(batch)s
ORDER BY product_id, _loaded_at DESC
ON CONFLICT (product_id) DO UPDATE SET
    sku = EXCLUDED.sku, name = EXCLUDED.name, category = EXCLUDED.category, brand = EXCLUDED.brand,
    price_cents = EXCLUDED.price_cents, cost_cents = EXCLUDED.cost_cents, active = EXCLUDED.active,
    updated_at = now()
WHERE (mart.dim_product.sku, mart.dim_product.name, mart.dim_product.category, mart.dim_product.brand,
       mart.dim_product.price_cents, mart.dim_product.cost_cents, mart.dim_product.active)
      IS DISTINCT FROM (EXCLUDED.sku, EXCLUDED.name, EXCLUDED.category, EXCLUDED.brand,
       EXCLUDED.price_cents, EXCLUDED.cost_cents, EXCLUDED.active);

-- fact_orders: one row per order, with payment and delivery attached
INSERT INTO mart.fact_orders (
    order_id, date_key, customer_key, status, channel, item_count,
    subtotal_cents, discount_cents, shipping_cents, total_cents,
    payment_method, payment_status, installments,
    ordered_at, paid_at, delivered_at, delivery_days, _batch_id)
WITH o AS (
    SELECT DISTINCT ON (order_id) *
    FROM raw.orders
    WHERE _batch_id = %(batch)s
    ORDER BY order_id, _loaded_at DESC
), p AS (
    SELECT DISTINCT ON (order_id) order_id, method, status, installments, paid_at
    FROM raw.payments
    WHERE _batch_id = %(batch)s
    ORDER BY order_id, paid_at DESC NULLS LAST
), i AS (
    SELECT order_id, count(DISTINCT order_item_id) AS item_count
    FROM raw.order_items
    WHERE _batch_id = %(batch)s
    GROUP BY order_id
)
SELECT o.order_id,
       to_char(o.ordered_at AT TIME ZONE INTERVAL '-03:00', 'YYYYMMDD')::int,
       coalesce(c.customer_key, -1),
       o.status,
       o.channel,
       coalesce(i.item_count, 0),
       o.subtotal_cents, o.discount_cents, o.shipping_cents, o.total_cents,
       p.method, p.status, p.installments,
       o.ordered_at, p.paid_at, o.delivered_at,
       round(extract(epoch FROM o.delivered_at - o.ordered_at) / 86400.0, 2),
       %(batch)s
FROM o
LEFT JOIN mart.dim_customer c ON c.customer_id = o.customer_id
LEFT JOIN p ON p.order_id = o.order_id
LEFT JOIN i ON i.order_id = o.order_id
ON CONFLICT (order_id) DO UPDATE SET
    date_key = EXCLUDED.date_key, customer_key = EXCLUDED.customer_key, status = EXCLUDED.status,
    channel = EXCLUDED.channel, item_count = EXCLUDED.item_count,
    subtotal_cents = EXCLUDED.subtotal_cents, discount_cents = EXCLUDED.discount_cents,
    shipping_cents = EXCLUDED.shipping_cents, total_cents = EXCLUDED.total_cents,
    payment_method = EXCLUDED.payment_method, payment_status = EXCLUDED.payment_status,
    installments = EXCLUDED.installments, ordered_at = EXCLUDED.ordered_at,
    paid_at = EXCLUDED.paid_at, delivered_at = EXCLUDED.delivered_at,
    delivery_days = EXCLUDED.delivery_days, _batch_id = EXCLUDED._batch_id;

-- fact_order_items: one row per order line, with cost and margin at load time
INSERT INTO mart.fact_order_items (
    order_item_id, order_id, date_key, customer_key, product_key,
    quantity, unit_price_cents, line_total_cents, line_cost_cents, gross_margin_cents, _batch_id)
WITH li AS (
    SELECT DISTINCT ON (order_item_id) *
    FROM raw.order_items
    WHERE _batch_id = %(batch)s
    ORDER BY order_item_id, _loaded_at DESC
)
SELECT li.order_item_id,
       li.order_id,
       f.date_key,
       f.customer_key,
       coalesce(dp.product_key, -1),
       li.quantity,
       li.unit_price_cents,
       li.line_total_cents,
       li.quantity * coalesce(dp.cost_cents, 0),
       li.line_total_cents - li.quantity * coalesce(dp.cost_cents, 0),
       %(batch)s
FROM li
JOIN mart.fact_orders f ON f.order_id = li.order_id
LEFT JOIN mart.dim_product dp ON dp.product_id = li.product_id
ON CONFLICT (order_item_id) DO UPDATE SET
    order_id = EXCLUDED.order_id, date_key = EXCLUDED.date_key,
    customer_key = EXCLUDED.customer_key, product_key = EXCLUDED.product_key,
    quantity = EXCLUDED.quantity, unit_price_cents = EXCLUDED.unit_price_cents,
    line_total_cents = EXCLUDED.line_total_cents, line_cost_cents = EXCLUDED.line_cost_cents,
    gross_margin_cents = EXCLUDED.gross_margin_cents, _batch_id = EXCLUDED._batch_id;
