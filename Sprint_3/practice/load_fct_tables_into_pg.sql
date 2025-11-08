BEGIN;

-- =========================================================
-- f_activity: user actions by day
-- =========================================================
DELETE FROM mart.f_activity;

WITH
source_actions AS (
    SELECT
        ual.action_id            AS activity_id,
        DATE(ual.date_time)      AS fact_date
    FROM stage.user_activity_log ual
    WHERE ual.action_id  IS NOT NULL
      AND ual.date_time IS NOT NULL
),
actions_with_date_id AS (
    SELECT
        sa.activity_id,
        dc.date_id
    FROM source_actions sa
    JOIN mart.d_calendar dc
      ON dc.fact_date = sa.fact_date
),
activity_agg AS (
    SELECT
        activity_id,
        date_id,
        COUNT(*) AS click_number
    FROM actions_with_date_id
    GROUP BY activity_id, date_id
)
INSERT INTO mart.f_activity (activity_id, date_id, click_number)
SELECT activity_id, date_id, click_number
FROM activity_agg
ORDER BY activity_id, date_id;

-- =========================================================
-- f_daily_sales: sales by day, item, customer
-- =========================================================
DELETE FROM mart.f_daily_sales;

WITH
source_orders AS (
    SELECT
        DATE(uol.date_time) AS fact_date,
        uol.item_id,
        uol.customer_id,
        uol.quantity,
        uol.payment_amount
    FROM stage.user_order_log uol
    WHERE uol.date_time   IS NOT NULL
      AND uol.item_id     IS NOT NULL
      AND uol.customer_id IS NOT NULL
),
orders_agg AS (
    SELECT
        fact_date,
        item_id,
        customer_id,
        -- average unit price across rows; guard divide-by-zero
        AVG(payment_amount / NULLIF(quantity, 0.0))             AS price,
        SUM(quantity)                                           AS quantity,
        SUM(payment_amount)                                     AS payment_amount
    FROM source_orders
    GROUP BY fact_date, item_id, customer_id
),
orders_with_date_id AS (
    SELECT
        dc.date_id,
        oa.item_id,
        oa.customer_id,
        oa.price,
        oa.quantity,
        oa.payment_amount
    FROM orders_agg oa
    JOIN mart.d_calendar dc
      ON dc.fact_date = oa.fact_date
)
INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, payment_amount)
SELECT date_id, item_id, customer_id, price, quantity, payment_amount
FROM orders_with_date_id
ORDER BY date_id, item_id, customer_id;

COMMIT;