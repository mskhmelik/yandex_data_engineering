import psycopg2

SQL = """
ALTER TABLE dds.dm_products
DROP CONSTRAINT IF EXISTS dm_products_restaurant_id_fkey;

ALTER TABLE dds.dm_products
ADD CONSTRAINT dm_products_restaurant_id_fkey
FOREIGN KEY (restaurant_id)
REFERENCES dds.dm_restaurants (id);
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
    print("Foreign key fixed: dm_products.restaurant_id → dm_restaurants.id")

if __name__ == "__main__":
    main()
