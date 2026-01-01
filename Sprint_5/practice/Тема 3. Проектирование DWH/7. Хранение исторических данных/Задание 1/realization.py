import psycopg2

SQL = """
-- drop in correct order because of FKs
DROP TABLE IF EXISTS de.public.sales;
DROP TABLE IF EXISTS de.public.products;
DROP TABLE IF EXISTS de.public.clients;

-- recreate tables
CREATE TABLE de.public.clients
(
    client_id INTEGER NOT NULL
        CONSTRAINT clients_pk PRIMARY KEY,
    name      TEXT    NOT NULL,
    login     TEXT    NOT NULL
);

CREATE TABLE de.public.products
(
    product_id INTEGER        NOT NULL
        CONSTRAINT products_pk PRIMARY KEY,
    name       TEXT           NOT NULL,
    price      NUMERIC(14, 2) NOT NULL
);

CREATE TABLE de.public.sales
(
    client_id  INTEGER        NOT NULL
        CONSTRAINT sales_clients_client_id_fk REFERENCES de.public.clients,
    product_id INTEGER        NOT NULL
        CONSTRAINT sales_products_product_id_fk REFERENCES de.public.products,
    amount     INTEGER        NOT NULL,
    total_sum  NUMERIC(14, 2) NOT NULL,
    CONSTRAINT sales_pk PRIMARY KEY (client_id, product_id)
);

-- SCD Type 1 update
UPDATE de.public.clients
SET login = 'arthur_dent'
WHERE client_id = 42;
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
    print("Reset completed and SCD Type 1 update executed.")

if __name__ == "__main__":
    main()
