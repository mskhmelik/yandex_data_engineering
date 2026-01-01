import psycopg2

DDL = """
-- schema already exists, but keep it safe
CREATE SCHEMA IF NOT EXISTS stg;

-- drop old versions if rerun
DROP TABLE IF EXISTS stg.ordersystem_orders;
DROP TABLE IF EXISTS stg.ordersystem_restaurants;
DROP TABLE IF EXISTS stg.ordersystem_users;

-- orders
CREATE TABLE stg.ordersystem_orders (
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL,
    CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);

-- restaurants
CREATE TABLE stg.ordersystem_restaurants (
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL,
    CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);

-- users
CREATE TABLE stg.ordersystem_users (
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL,
    CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
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
        cur.execute(DDL)

    conn.close()
    print("STG order system tables created successfully.")

if __name__ == "__main__":
    main()
