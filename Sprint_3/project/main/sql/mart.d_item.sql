-- Insert new items into mart.d_item
INSERT INTO mart.d_item (item_id, item_name)
SELECT
    u.item_id,
    u.item_name
FROM staging.user_order_log AS u
WHERE u.item_id NOT IN (
    SELECT i.item_id
    FROM mart.d_item AS i
)
GROUP BY
    u.item_id,
    u.item_name;
