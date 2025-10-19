-- Rerun mart.d_customer
INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT
    u.customer_id,
    u.first_name,
    u.last_name,
    MAX(u.city_id) AS city_id
FROM staging.user_order_log AS u
WHERE u.customer_id NOT IN (
    SELECT c.customer_id
    FROM mart.d_customer AS c
)
GROUP BY
    u.customer_id,
    u.first_name,
    u.last_name;
