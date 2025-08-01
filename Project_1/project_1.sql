/* Create single table from all sources. Bring additional columns if they are missing in source */

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
)
SELECT * FROM df_temp_1
UNION
SELECT * FROM df_temp_2
UNION
SELECT * FROM df_temp_3;

/* 2. Update records for craftsmen, but only if they don't exist in DWH */
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

/* 3. Update records for craftsmen, but only if they don't exist in DWH */
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

/* 4. Update records for customers, but only if they don't exist in DWH */
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