-- 0) Run as one transaction (optional but safer)
BEGIN;

-- =========================================================
-- 1) CALENDAR DIMENSION
-- =========================================================

-- 1.1 Clear target
DELETE FROM mart.d_calendar;

-- 1.2 Build and load
WITH
-- a) Dates from each source
source_orders AS (
    SELECT DATE(uol.date_time) AS fact_date
    FROM stage.user_order_log uol
    WHERE uol.date_time IS NOT NULL
),
source_activity AS (
    SELECT DATE(ual.date_time) AS fact_date
    FROM stage.user_activity_log ual
    WHERE ual.date_time IS NOT NULL
),
source_research AS (
    SELECT DATE(cr.date_id) AS fact_date
    FROM stage.customer_research cr
    WHERE cr.date_id IS NOT NULL
),
-- b) Union then deduplicate
source_dates AS (
    SELECT fact_date FROM source_orders
    UNION ALL
    SELECT fact_date FROM source_activity
    UNION ALL
    SELECT fact_date FROM source_research
),
dedup_dates AS (
    SELECT DISTINCT fact_date
    FROM source_dates
),
-- c) Enrich
calendar_enriched AS (
    SELECT
        TO_CHAR(d.fact_date, 'YYYYMMDD')::INT AS date_id,
        d.fact_date                           AS fact_date,
        EXTRACT(DAY   FROM d.fact_date)::INT  AS day_num,
        EXTRACT(MONTH FROM d.fact_date)::INT  AS month_num,
        TO_CHAR(d.fact_date, 'FMMonth')       AS month_name,
        EXTRACT(YEAR  FROM d.fact_date)::INT  AS year_num
    FROM dedup_dates d
)
INSERT INTO mart.d_calendar (
    date_id, fact_date, day_num, month_num, month_name, year_num
)
SELECT
    date_id, fact_date, day_num, month_num, month_name, year_num
FROM calendar_enriched
ORDER BY fact_date;

-- =========================================================
-- 2) CUSTOMER DIMENSION
-- =========================================================

-- 2.1 Clear target
DELETE FROM mart.d_customer;

-- 2.2 Build and load
WITH
-- a) Raw customer-city pairs from orders
source_orders AS (
    SELECT
        uol.customer_id AS customer_id,
        uol.city_id     AS city_id
    FROM stage.user_order_log uol
    WHERE uol.customer_id IS NOT NULL
),
-- b) One city per customer (rule: MAX city_id)
customer_city AS (
    SELECT
        customer_id,
        MAX(city_id) AS city_id
    FROM source_orders
    GROUP BY customer_id
)
INSERT INTO mart.d_customer (customer_id, city_id)
SELECT
    customer_id,
    city_id
FROM customer_city
ORDER BY customer_id;

-- =========================================================
-- 3) ITEM DIMENSION
-- =========================================================

-- 3.1 Clear target
DELETE FROM mart.d_item;

-- 3.2 Build and load
WITH
-- a) Items from orders
source_items AS (
    SELECT
        uol.item_id,
        uol.item_name
    FROM stage.user_order_log uol
    WHERE uol.item_id  IS NOT NULL
      AND uol.item_name IS NOT NULL
),
-- b) Deduplicate
dedup_items AS (
    SELECT DISTINCT
        item_id,
        item_name
    FROM source_items
)
INSERT INTO mart.d_item (item_id, item_name)
SELECT
    item_id,
    item_name
FROM dedup_items
ORDER BY item_id;

COMMIT;