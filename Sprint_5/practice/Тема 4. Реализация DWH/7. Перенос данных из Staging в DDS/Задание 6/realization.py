import logging
import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"
ACTIVE_TO_FAR_FUTURE = "2099-12-31 00:00:00"


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["dds", "full_refresh"],
    is_paused_upon_creation=False,
)
def dds_full_refresh():

    pg = ConnectionBuilder.pg_conn(DWH_CONN_ID)

    @task()
    def load_dm_users():
        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS dds.dm_users CASCADE;")
                cur.execute(
                    """
                    CREATE TABLE dds.dm_users (
                        id serial PRIMARY KEY,
                        user_id varchar NOT NULL,
                        user_name varchar NOT NULL,
                        user_login varchar NOT NULL
                    );
                    """
                )
                # IMPORTANT: user_id from source order is object_value->user->id (mongo id),
                # so we store user_id as object_id (mongo _id) in STG users, which matches.
                cur.execute(
                    """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    SELECT
                        object_id,
                        object_value::jsonb ->> 'name',
                        object_value::jsonb ->> 'login'
                    FROM stg.ordersystem_users;
                    """
                )

    @task()
    def load_dm_restaurants():
        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS dds.dm_restaurants CASCADE;")
                cur.execute(
                    """
                    CREATE TABLE dds.dm_restaurants (
                        id serial PRIMARY KEY,
                        restaurant_id varchar NOT NULL,
                        restaurant_name varchar NOT NULL,
                        active_from timestamp NOT NULL,
                        active_to timestamp NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    INSERT INTO dds.dm_restaurants (
                        restaurant_id,
                        restaurant_name,
                        active_from,
                        active_to
                    )
                    SELECT
                        object_id,
                        object_value::jsonb ->> 'name',
                        update_ts,
                        %(active_to)s::timestamp
                    FROM stg.ordersystem_restaurants;
                    """,
                    {"active_to": ACTIVE_TO_FAR_FUTURE},
                )

    @task()
    def load_dm_timestamps():
        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS dds.dm_timestamps CASCADE;")
                cur.execute(
                    """
                    CREATE TABLE dds.dm_timestamps (
                        id serial PRIMARY KEY,
                        ts timestamp NOT NULL,
                        year int NOT NULL,
                        month int NOT NULL,
                        day int NOT NULL,
                        date date NOT NULL,
                        time time NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                    SELECT DISTINCT
                        date_trunc('second', (object_value::jsonb ->> 'date')::timestamp) AS ts,
                        EXTRACT(YEAR  FROM date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::int,
                        EXTRACT(MONTH FROM date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::int,
                        EXTRACT(DAY   FROM date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::int,
                        (date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::date,
                        (date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::time
                    FROM stg.ordersystem_orders
                    WHERE object_value::jsonb ->> 'final_status' IN ('CLOSED', 'CANCELLED');
                    """
                )

    @task()
    def load_dm_products():
        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS dds.dm_products CASCADE;")
                cur.execute(
                    """
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
                )
                cur.execute(
                    """
                    INSERT INTO dds.dm_products (
                        restaurant_id,
                        product_id,
                        product_name,
                        product_price,
                        active_from,
                        active_to
                    )
                    SELECT DISTINCT ON (r.id, (item.elem ->> 'id'))
                        r.id AS restaurant_id,
                        (item.elem ->> 'id') AS product_id,
                        (item.elem ->> 'name') AS product_name,
                        (item.elem ->> 'price')::numeric(14,2) AS product_price,
                        o.update_ts AS active_from,
                        %(active_to)s::timestamp AS active_to
                    FROM stg.ordersystem_orders o
                    JOIN dds.dm_restaurants r
                      ON r.restaurant_id = (o.object_value::jsonb -> 'restaurant' ->> 'id')
                    CROSS JOIN LATERAL jsonb_array_elements(o.object_value::jsonb -> 'order_items') AS item(elem)
                    ORDER BY
                        r.id,
                        (item.elem ->> 'id'),
                        o.update_ts DESC;
                    """,
                    {"active_to": ACTIVE_TO_FAR_FUTURE},
                )

    @task()
    def load_dm_orders():
        with pg.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS dds.dm_orders CASCADE;")
                cur.execute(
                    """
                    CREATE TABLE dds.dm_orders (
                        id serial PRIMARY KEY,
                        order_key varchar NOT NULL,
                        order_status varchar NOT NULL,
                        restaurant_id integer NOT NULL,
                        timestamp_id integer NOT NULL,
                        user_id integer NOT NULL
                    );
                    """
                )

                # join to dims to get surrogate ids
                cur.execute(
                    """
                    INSERT INTO dds.dm_orders (
                        order_key,
                        order_status,
                        restaurant_id,
                        timestamp_id,
                        user_id
                    )
                    SELECT
                        o.object_id AS order_key,
                        (o.object_value::jsonb ->> 'final_status') AS order_status,
                        r.id AS restaurant_id,
                        ts.id AS timestamp_id,
                        u.id AS user_id
                    FROM stg.ordersystem_orders o
                    JOIN dds.dm_restaurants r
                      ON r.restaurant_id = (o.object_value::jsonb -> 'restaurant' ->> 'id')
                    JOIN dds.dm_users u
                      ON u.user_id = (o.object_value::jsonb -> 'user' ->> 'id')
                    JOIN dds.dm_timestamps ts
                      ON ts.ts = date_trunc('second', (o.object_value::jsonb ->> 'date')::timestamp);
                    """
                )

        log.info("dm_orders full refresh completed")

    u = load_dm_users()
    r = load_dm_restaurants()
    t = load_dm_timestamps()
    p = load_dm_products()
    o = load_dm_orders()

    u >> r >> t >> p >> o


dag = dds_full_refresh()
