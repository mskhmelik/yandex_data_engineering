import psycopg2

DDL = """
ALTER TABLE stg.ordersystem_orders
ADD CONSTRAINT ordersystem_orders_object_id_uindex
UNIQUE (object_id);

ALTER TABLE stg.ordersystem_restaurants
ADD CONSTRAINT ordersystem_restaurants_object_id_uindex
UNIQUE (object_id);

ALTER TABLE stg.ordersystem_users
ADD CONSTRAINT ordersystem_users_object_id_uindex
UNIQUE (object_id);
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
        cur.execute(DDL)

    conn.close()
    print("UNIQUE constraints on object_id added successfully.")

if __name__ == "__main__":
    main()
