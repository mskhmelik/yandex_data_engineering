import psycopg2

SQL = """
ALTER TABLE dds.dm_orders
ADD CONSTRAINT dm_orders_user_id_fkey
FOREIGN KEY (user_id)
REFERENCES dds.dm_users (id);

ALTER TABLE dds.dm_orders
ADD CONSTRAINT dm_orders_restaurant_id_fkey
FOREIGN KEY (restaurant_id)
REFERENCES dds.dm_restaurants (id);

ALTER TABLE dds.dm_orders
ADD CONSTRAINT dm_orders_timestamp_id_fkey
FOREIGN KEY (timestamp_id)
REFERENCES dds.dm_timestamps (id);

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
    print("Executed.")

if __name__ == "__main__":
    main()
