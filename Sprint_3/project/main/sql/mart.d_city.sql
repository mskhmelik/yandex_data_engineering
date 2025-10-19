-- Rerun mart.d_city
INSERT INTO mart.d_city (city_id, city_name)
SELECT
    u.city_id,
    u.city_name
FROM staging.user_order_log AS u
WHERE u.city_id NOT IN (
    SELECT c.city_id
    FROM mart.d_city AS c
)
GROUP BY
    u.city_id,
    u.city_name;
