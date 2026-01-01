import psycopg2

SQL = """
CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id serial PRIMARY KEY,

    product_id integer NOT NULL,
    order_id integer NOT NULL,

    count integer DEFAULT 0 NOT NULL
        CONSTRAINT fct_product_sales_count_check
        CHECK (count >= 0),

    price numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT fct_product_sales_price_check
        CHECK (price >= 0),

    total_sum numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT fct_product_sales_total_sum_check
        CHECK (total_sum >= 0),

    bonus_payment numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT fct_product_sales_bonus_payment_check
        CHECK (bonus_payment >= 0),

    bonus_grant numeric(14, 2) DEFAULT 0 NOT NULL
        CONSTRAINT fct_product_sales_bonus_grant_check
        CHECK (bonus_grant >= 0)
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
    print("Table dds.fct_product_sales created successfully.")

if __name__ == "__main__":
    main()
