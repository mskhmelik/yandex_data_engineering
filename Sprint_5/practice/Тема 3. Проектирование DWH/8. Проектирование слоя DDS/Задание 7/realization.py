import psycopg2

SQL = """
CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id serial PRIMARY KEY,
    user_id integer NOT NULL,
    restaurant_id integer NOT NULL,
    timestamp_id integer NOT NULL,
    order_key varchar NOT NULL,
    order_status varchar NOT NULL
);
"""

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=15432,
        database="de",
        user="jovyan",
        password="jovyan"
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(SQL)
    conn.close()
    print("Table dds.dm_orders created successfully.")

if __name__ == "__main__":
    main()
