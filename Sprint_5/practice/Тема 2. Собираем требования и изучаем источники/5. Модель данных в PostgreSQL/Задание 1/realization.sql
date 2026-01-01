SELECT DISTINCT
    elem ->> 'product_name' AS product_name
FROM outbox
CROSS JOIN LATERAL jsonb_array_elements((event_value::jsonb) -> 'product_payments') AS elem
WHERE elem ->> 'product_name' IS NOT NULL;
