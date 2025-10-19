-- Weekly customer retention (light, idempotent)
-- Depends on: mart.f_sales (transactional; status in ['shipped','refunded']), mart.d_calendar

BEGIN;

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.f_customer_retention(
    id                          SERIAL PRIMARY KEY,
    new_customers_count         INT,
    returning_customers_count   INT,
    refunded_customers_count    INT,
    period_name                 VARCHAR(6),
    period_id                   BIGINT,
    item_id                     BIGINT,
    new_customers_revenue       NUMERIC(10, 2),
    returning_customers_revenue NUMERIC(10, 2),
    customers_refunded          INT
);

-- 1) Clear target week to keep reruns clean
DELETE FROM mart.f_customer_retention
WHERE period_name = 'weekly'
  AND period_id   = DATE_PART('week', '{{ ds }}'::date);

-- 2) Insert fresh weekly metrics
INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customers_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)
SELECT
    'weekly'                                                                                        AS period_name,
    DATE_PART('week', '{{ ds }}'::date)                                                             AS period_id,
    item_id,
    COUNT(DISTINCT CASE WHEN shipped_cnt = 1 THEN customer_id END)                                  AS new_customers_count,
    COUNT(DISTINCT CASE WHEN shipped_cnt > 1 THEN customer_id END)                                  AS returning_customers_count,
    COUNT(DISTINCT CASE WHEN refunded_cnt > 0 THEN customer_id END)                                 AS refunded_customers_count,
    COALESCE(SUM(CASE WHEN shipped_cnt = 1 THEN shipped_revenue ELSE 0 END), 0)::numeric(10,2)      AS new_customers_revenue,
    COALESCE(SUM(CASE WHEN shipped_cnt > 1 THEN shipped_revenue ELSE 0 END), 0)::numeric(10,2)      AS returning_customers_revenue,
    COALESCE(SUM(refunded_cnt), 0)                                                                  AS customers_refunded
FROM (
    SELECT
        fs.customer_id,
        fs.item_id,
        COUNT(*) FILTER (WHERE fs.status = 'shipped')                                                AS shipped_cnt,
        COUNT(*) FILTER (WHERE fs.status = 'refunded')                                               AS refunded_cnt,
        SUM(CASE WHEN fs.status = 'shipped' THEN fs.payment_amount ELSE 0 END)                       AS shipped_revenue
    FROM mart.f_sales fs
    JOIN mart.d_calendar dc ON dc.date_id = fs.date_id
    WHERE DATE_PART('week', dc.date_actual) = DATE_PART('week', '{{ ds }}'::date)
    GROUP BY fs.customer_id, fs.item_id
) t
GROUP BY item_id;

COMMIT;
