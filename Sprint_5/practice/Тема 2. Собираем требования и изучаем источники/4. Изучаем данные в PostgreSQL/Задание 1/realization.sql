SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.column_default,
    c.is_nullable
FROM information_schema.columns AS c
JOIN information_schema.tables AS t
    ON  c.table_name  = t.table_name
    AND c.table_schema = t.table_schema
WHERE c.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
ORDER BY
    c.table_name ASC,
    c.ordinal_position ASC;