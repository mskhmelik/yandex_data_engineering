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
def dds_dimensions_full_refresh():

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
                cur.execute(
                    """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    SELECT
                        object_id,
                        object_value::json ->> 'name',
                        object_value::json ->> 'login'
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
                        object_value::json ->> 'name',
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
                log.info("Dropping dm_timestamps")
                cur.execute("DROP TABLE IF EXISTS dds.dm_timestamps CASCADE;")

                log.info("Creating dm_timestamps")
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

                log.info("Inserting timestamps")
                cur.execute(
                    """
                    INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                    SELECT DISTINCT
                        date_trunc('second', (object_value::jsonb ->> 'date')::timestamp) AS ts,
                        EXTRACT(YEAR  FROM date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::int AS year,
                        EXTRACT(MONTH FROM date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::int AS month,
                        EXTRACT(DAY   FROM date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::int AS day,
                        (date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::date AS date,
                        (date_trunc('second', (object_value::jsonb ->> 'date')::timestamp))::time AS time
                    FROM stg.ordersystem_orders
                    WHERE object_value::jsonb ->> 'final_status' IN ('CLOSED', 'CANCELLED');
                    """
                )

        log.info("dm_timestamps full refresh completed")

    users = load_dm_users()
    restaurants = load_dm_restaurants()
    timestamps = load_dm_timestamps()

    users >> restaurants >> timestamps


dag = dds_dimensions_full_refresh()
