-- Create table if not exists
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

-- Delete existing data for this week to make script idempotent
DELETE FROM mart.f_customer_retention
WHERE period_id = DATE_PART('week', '{{ ds }}'::DATE);

-- Insert weekly aggregates
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
    'weekly'                                                                       AS period_name,
    DATE_PART('week', '{{ ds }}'::DATE)                                            AS period_id,
    item_id,    
    COUNT(DISTINCT CASE WHEN cnt_orders = 1 THEN customer_id END)                  AS new_customers_count,
    COUNT(DISTINCT CASE WHEN cnt_orders > 1 THEN customer_id END)                  AS returning_customers_count,
    COUNT(DISTINCT CASE WHEN refunded_orders > 0 THEN customer_id END)             AS refunded_customers_count,
    COALESCE(SUM(CASE WHEN cnt_orders = 1 THEN shipped_revenue ELSE 0 END), 0)     AS new_customers_revenue,
    COALESCE(SUM(CASE WHEN cnt_orders > 1 THEN shipped_revenue ELSE 0 END), 0)     AS returning_customers_revenue,
    COALESCE(SUM(refunded_orders), 0)                                              AS customers_refunded
FROM (
    SELECT
        fs.customer_id,
        fs.item_id,
        COUNT(*) FILTER (WHERE fs.status = 'shipped')          AS cnt_orders,
        COUNT(*) FILTER (WHERE fs.status = 'refunded')         AS refunded_orders,
        SUM(CASE WHEN fs.status = 'shipped' THEN fs.payment_amount ELSE 0 END) AS shipped_revenue
    FROM mart.f_sales fs
    JOIN mart.d_calendar dc ON fs.date_id = dc.date_id
    WHERE DATE_PART('week', dc.date_actual) = DATE_PART('week', '{{ ds }}'::DATE)
    GROUP BY fs.customer_id, fs.item_id
) sub
GROUP BY item_id;
