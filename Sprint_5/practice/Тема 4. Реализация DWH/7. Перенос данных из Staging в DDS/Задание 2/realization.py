import logging
import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["dds", "dm_users", "full_refresh"],
    is_paused_upon_creation=False,
)
def dds_dm_users_full_refresh():

    pg = ConnectionBuilder.pg_conn(DWH_CONN_ID)

    @task()
    def load_dm_users():
        with pg.connection() as conn:
            with conn.cursor() as cur:
                log.info("Dropping dm_users")
                cur.execute("DROP TABLE IF EXISTS dds.dm_users CASCADE;")

                log.info("Creating dm_users")
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

                log.info("Inserting users into dm_users")
                cur.execute(
                    """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    SELECT
                        object_id AS user_id,
                        object_value::jsonb ->> 'name'  AS user_name,
                        object_value::jsonb ->> 'login' AS user_login
                    FROM stg.ordersystem_users;
                    """
                )

        log.info("dm_users full refresh completed")

    load_dm_users()


dag = dds_dm_users_full_refresh()
