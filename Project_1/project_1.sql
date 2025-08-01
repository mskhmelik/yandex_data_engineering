-- 1. Create single table from all sources. Bring additional columns if they are missing in source
DROP TABLE IF EXISTS df_temp_summary;

CREATE TEMP TABLE df_temp_summary AS
WITH 
df_temp_1 AS (
    SELECT 
        order_id,
        order_created_date,
        order_completion_date,
        order_status,
        craftsman_id,
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        product_id,
        product_name,
        product_description,
        product_type,
        product_price,
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email
    FROM source1.craft_market_wide
),
df_temp_2 AS (
    SELECT 
        o.order_id,
        o.order_created_date,
        o.order_completion_date,
        o.order_status,
        p.craftsman_id,
        p.craftsman_name,
        p.craftsman_address,
        p.craftsman_birthday,
        p.craftsman_email,
        p.product_id,
        p.product_name,
        p.product_description,
        p.product_type,
        p.product_price,
        o.customer_id,
        o.customer_name,
        o.customer_address,
        o.customer_birthday,
        o.customer_email
    FROM source2.craft_market_masters_products p
    JOIN source2.craft_market_orders_customers o 
        ON o.product_id = p.product_id 
        AND p.craftsman_id = o.craftsman_id
),
df_temp_3 AS (
    SELECT 
        o.order_id,
        o.order_created_date,
        o.order_completion_date,
        o.order_status,
        c.craftsman_id,
        c.craftsman_name,
        c.craftsman_address,
        c.craftsman_birthday,
        c.craftsman_email,
        o.product_id,
        o.product_name,
        o.product_description,
        o.product_type,
        o.product_price,
        cu.customer_id,
        cu.customer_name,
        cu.customer_address,
        cu.customer_birthday,
        cu.customer_email
    FROM source3.craft_market_orders o
    JOIN source3.craft_market_craftsmans c 
        ON o.craftsman_id = c.craftsman_id
    JOIN source3.craft_market_customers cu 
        ON o.customer_id = cu.customer_id
),
df_temp_4 AS (
    SELECT  
        cpo.order_id,
        cpo.order_created_date,
        cpo.order_completion_date,
        cpo.order_status,
        cpo.craftsman_id,
        cpo.craftsman_name,
        cpo.craftsman_address,
        cpo.craftsman_birthday,
        cpo.craftsman_email,
        cpo.product_id,
        cpo.product_name,
        cpo.product_description,
        cpo.product_type,
        cpo.product_price,
        c.customer_id,
        c.customer_name,
        c.customer_address,
        c.customer_birthday,
        c.customer_email 
    FROM external_source.craft_products_orders cpo
    JOIN external_source.customers c 
        USING (customer_id)
)
SELECT * FROM df_temp_1
UNION
SELECT * FROM df_temp_2
UNION
SELECT * FROM df_temp_3
UNION
SELECT * FROM df_temp_4;

-- Update records for craftsmen, but only if they don't exist in DWH
MERGE INTO dwh.d_craftsman d
USING (
    SELECT DISTINCT 
        craftsman_name, 
        craftsman_address, 
        craftsman_birthday, 
        craftsman_email 
    FROM df_temp_summary
) AS df_temp_craftsman
ON d.craftsman_name = df_temp_craftsman.craftsman_name
   AND d.craftsman_email = df_temp_craftsman.craftsman_email

WHEN MATCHED THEN
    UPDATE SET 
        craftsman_address = df_temp_craftsman.craftsman_address,
        craftsman_birthday = df_temp_craftsman.craftsman_birthday,
        load_dttm = current_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        craftsman_name, 
        craftsman_address, 
        craftsman_birthday, 
        craftsman_email, 
        load_dttm
    )
    VALUES (
        df_temp_craftsman.craftsman_name, 
        df_temp_craftsman.craftsman_address, 
        df_temp_craftsman.craftsman_birthday, 
        df_temp_craftsman.craftsman_email, 
        current_timestamp
    );

-- 3. Update records for craftsmen, but only if they don't exist in DWH
MERGE INTO dwh.d_product d
USING (
    SELECT DISTINCT 
        product_name, 
        product_description, 
        product_type, 
        product_price 
    FROM df_temp_summary
) AS df_temp_product
ON d.product_name = df_temp_product.product_name
   AND d.product_description = df_temp_product.product_description
   AND d.product_price = df_temp_product.product_price

WHEN MATCHED THEN
    UPDATE SET 
        product_type = df_temp_product.product_type,
        load_dttm = current_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        product_name, 
        product_description, 
        product_type, 
        product_price, 
        load_dttm
    )
    VALUES (
        df_temp_product.product_name, 
        df_temp_product.product_description, 
        df_temp_product.product_type, 
        df_temp_product.product_price, 
        current_timestamp
    );

-- 4. Update records for customers, but only if they don't exist in DWH
MERGE INTO dwh.d_customer d
USING (
    SELECT DISTINCT 
        customer_name, 
        customer_address, 
        customer_birthday, 
        customer_email 
    FROM df_temp_summary
) AS df_temp_customer
ON d.customer_name = df_temp_customer.customer_name
   AND d.customer_email = df_temp_customer.customer_email

WHEN MATCHED THEN
    UPDATE SET 
        customer_address = df_temp_customer.customer_address,
        customer_birthday = df_temp_customer.customer_birthday,
        load_dttm = current_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        customer_name, 
        customer_address, 
        customer_birthday, 
        customer_email, 
        load_dttm
    )
    VALUES (
        df_temp_customer.customer_name, 
        df_temp_customer.customer_address, 
        df_temp_customer.customer_birthday, 
        df_temp_customer.customer_email, 
        current_timestamp
    );

-- 5. Create staging fact table
DROP TABLE IF EXISTS df_temp_fact;

CREATE TEMP TABLE df_temp_fact AS 
SELECT  
    dp.product_id,
    dc.craftsman_id,
    dcust.customer_id,
    src.order_created_date,
    src.order_completion_date,
    src.order_status,
    current_timestamp AS load_dttm
FROM df_temp_summary src
JOIN dwh.d_craftsman dc 
    ON dc.craftsman_name = src.craftsman_name 
   AND dc.craftsman_email = src.craftsman_email
JOIN dwh.d_customer dcust 
    ON dcust.customer_name = src.customer_name 
   AND dcust.customer_email = src.customer_email
JOIN dwh.d_product dp 
    ON dp.product_name = src.product_name 
   AND dp.product_description = src.product_description 
   AND dp.product_price = src.product_price;

-- 6. Update all records and load new records в dwh.f_order
MERGE INTO dwh.f_order f
USING df_temp_fact t
ON f.product_id = t.product_id 
   AND f.craftsman_id = t.craftsman_id 
   AND f.customer_id = t.customer_id 
   AND f.order_created_date = t.order_created_date

WHEN MATCHED THEN
    UPDATE SET 
        order_completion_date = t.order_completion_date,
        order_status = t.order_status,
        load_dttm = current_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        product_id, 
        craftsman_id, 
        customer_id, 
        order_created_date, 
        order_completion_date, 
        order_status, 
        load_dttm
    )
    VALUES (
        t.product_id, 
        t.craftsman_id, 
        t.customer_id, 
        t.order_created_date, 
        t.order_completion_date, 
        t.order_status, 
        current_timestamp
    );

-- 7. Create or update customer report datamart
BEGIN TRANSACTION;

-- Step 1: Create load log table if not exists
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

COMMENT ON TABLE dwh.load_dates_customer_report_datamart IS 'дата загрузки витрины по покупателям';

-- Step 2: Get latest load timestamp
WITH max_load_dttm AS (
    SELECT COALESCE(MAX(load_dttm), DATE '1900-01-01') AS max_ts
    FROM dwh.load_dates_customer_report_datamart
),

-- Step 3: Extract changed/new records from DWH
dwh_delta AS (
    SELECT
        dcs.customer_id,
        dcs.customer_name,
        dcs.customer_address,
        dcs.customer_birthday,
        dcs.customer_email,
        dcs.load_dttm AS customers_load_dttm,
        dp.product_id,
        dp.product_price,
        dp.product_type,
        dp.load_dttm AS products_load_dttm,
        dc.craftsman_id,
        dc.load_dttm AS craftsman_load_dttm,
        fo.order_id,
        fo.order_completion_date - fo.order_created_date AS diff_order_date,
        fo.order_status,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        crd.customer_id AS exist_customer_id
    FROM dwh.f_order fo
    JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id
    JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id
    JOIN dwh.d_product dp ON fo.product_id = dp.product_id
    LEFT JOIN dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id,
    max_load_dttm ml
    WHERE fo.load_dttm > ml.max_ts
       OR dc.load_dttm > ml.max_ts
       OR dcs.load_dttm > ml.max_ts
       OR dp.load_dttm > ml.max_ts
),

-- Step 4: Determine updated customers
dwh_update_delta AS (
    SELECT DISTINCT exist_customer_id AS customer_id
    FROM dwh_delta
    WHERE exist_customer_id IS NOT NULL
),

-- Step 5: Build base aggregation (used for both insert and update)
customer_aggregates AS (
    SELECT
        dd.customer_id,
        dd.customer_name,
        dd.customer_address,
        dd.customer_birthday,
        dd.customer_email,
        SUM(dd.product_price) AS customer_money,
        SUM(dd.product_price) * 0.1 AS platform_money,
        COUNT(dd.order_id)::BIGINT AS count_order,
        AVG(dd.product_price) AS avg_price_order,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dd.diff_order_date) AS median_time_order_completed,
        SUM(CASE WHEN dd.order_status = 'created' THEN 1 ELSE 0 END)::BIGINT AS count_order_created,
        SUM(CASE WHEN dd.order_status = 'in progress' THEN 1 ELSE 0 END)::BIGINT AS count_order_in_progress,
        SUM(CASE WHEN dd.order_status = 'delivery' THEN 1 ELSE 0 END)::BIGINT AS count_order_delivery,
        SUM(CASE WHEN dd.order_status = 'done' THEN 1 ELSE 0 END)::BIGINT AS count_order_done,
        SUM(CASE WHEN dd.order_status != 'done' THEN 1 ELSE 0 END)::BIGINT AS count_order_not_done,
        dd.report_period
    FROM dwh_delta dd
    GROUP BY
        dd.customer_id, dd.customer_name, dd.customer_address,
        dd.customer_birthday, dd.customer_email, dd.report_period
),

-- Step 6: Get top product category per customer
top_product_categories AS (
    SELECT 
        customer_id,
        product_type,
        COUNT(product_id) AS count_product,
        RANK() OVER (PARTITION BY customer_id ORDER BY COUNT(product_id) DESC) AS rnk
    FROM dwh_delta
    GROUP BY customer_id, product_type
),

-- Step 7: Get top craftsman per customer
top_craftsmen AS (
    SELECT 
        customer_id,
        craftsman_id,
        COUNT(craftsman_id) AS count_craftsman,
        RANK() OVER (PARTITION BY customer_id ORDER BY COUNT(craftsman_id) DESC) AS rnk
    FROM dwh_delta
    GROUP BY customer_id, craftsman_id
),

-- Step 8: Join aggregates with top product and top craftsman
delta_full AS (
    SELECT 
        ag.customer_id,
        ag.customer_name,
        ag.customer_address,
        ag.customer_birthday,
        ag.customer_email,
        ag.customer_money,
        ag.platform_money,
        ag.count_order,
        ag.avg_price_order,
        ag.median_time_order_completed,
        cat.product_type AS top_product_category,
        cr.craftsman_id AS top_craftsman_id,
        ag.count_order_created,
        ag.count_order_in_progress,
        ag.count_order_delivery,
        ag.count_order_done,
        ag.count_order_not_done,
        ag.report_period
    FROM customer_aggregates ag
    LEFT JOIN top_product_categories cat 
        ON ag.customer_id = cat.customer_id AND cat.rnk = 1
    LEFT JOIN top_craftsmen cr 
        ON ag.customer_id = cr.customer_id AND cr.rnk = 1
),

-- Step 9: Separate inserts (new customers)
dwh_delta_insert_result AS (
    SELECT * FROM delta_full
    WHERE customer_id NOT IN (SELECT customer_id FROM dwh_update_delta)
),

-- Step 10: Separate updates (existing customers)
dwh_delta_update_result AS (
    SELECT * FROM delta_full
    WHERE customer_id IN (SELECT customer_id FROM dwh_update_delta)
),

-- Step 11: Perform INSERT
insert_delta AS (
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_money,
        platform_money,
        count_order,
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_period
    )
    SELECT * FROM dwh_delta_insert_result
),

-- Step 12: Perform UPDATE
update_delta AS (
    UPDATE dwh.customer_report_datamart d
    SET
        customer_name = u.customer_name,
        customer_address = u.customer_address,
        customer_birthday = u.customer_birthday,
        customer_email = u.customer_email,
        customer_money = u.customer_money,
        platform_money = u.platform_money,
        count_order = u.count_order,
        avg_price_order = u.avg_price_order,
        median_time_order_completed = u.median_time_order_completed,
        top_product_category = u.top_product_category,
        top_craftsman_id = u.top_craftsman_id,
        count_order_created = u.count_order_created,
        count_order_in_progress = u.count_order_in_progress,
        count_order_delivery = u.count_order_delivery,
        count_order_done = u.count_order_done,
        count_order_not_done = u.count_order_not_done,
        report_period = u.report_period
    FROM dwh_delta_update_result u
    WHERE d.customer_id = u.customer_id
),

-- Step 13: Record max load timestamp
insert_load_date AS (
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT 
        GREATEST(
            COALESCE(MAX(craftsman_load_dttm), CURRENT_DATE),
            COALESCE(MAX(customers_load_dttm), CURRENT_DATE),
            COALESCE(MAX(products_load_dttm), CURRENT_DATE)
        )
    FROM dwh_delta
)

-- Trigger CTE execution
SELECT 'increment datamart';

COMMIT TRANSACTION;

--Rollback