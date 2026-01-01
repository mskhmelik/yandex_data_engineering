import psycopg2

SQL = """
DROP TABLE IF EXISTS dds.dm_products;

CREATE TABLE dds.dm_products (
    id serial PRIMARY KEY,
    restaurant_id integer NOT NULL,
    product_id varchar NOT NULL,
    product_name varchar NOT NULL,
    product_price numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT dm_products_product_price_check CHECK (product_price >= 0),
    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
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
    print("dds.dm_products recreated.")

if __name__ == "__main__":
    main()
