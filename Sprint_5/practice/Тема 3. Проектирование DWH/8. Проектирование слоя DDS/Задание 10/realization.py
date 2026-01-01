import psycopg2

SQL = """
ALTER TABLE dds.fct_product_sales
ADD CONSTRAINT fct_product_sales_product_id_fkey
FOREIGN KEY (product_id)
REFERENCES dds.dm_products (id);

ALTER TABLE dds.fct_product_sales
ADD CONSTRAINT fct_product_sales_order_id_fkey
FOREIGN KEY (order_id)
REFERENCES dds.dm_orders (id);

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
    print("Executed")

if __name__ == "__main__":
    main()
