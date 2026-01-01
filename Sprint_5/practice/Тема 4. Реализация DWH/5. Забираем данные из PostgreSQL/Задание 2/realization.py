import logging
import psycopg2
import pendulum

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

SRC_CONN_ID = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"


def get_pg_kwargs(conn_id: str) -> dict:
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    kwargs = {
        "host": conn.host,
        "port": conn.port,
        "dbname": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }

    if extra.get("sslmode"):
        kwargs["sslmode"] = extra["sslmode"]

    return kwargs


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["stg", "bonus_system"],
    is_paused_upon_creation=False,
)
def load_bonussystem_ranks():

    @task()
    def load_ranks():
        src_conn = psycopg2.connect(**get_pg_kwargs(SRC_CONN_ID))
        dwh_conn = psycopg2.connect(**get_pg_kwargs(DWH_CONN_ID))

        try:
            with src_conn.cursor() as src_cur:
                src_cur.execute("""
                    SELECT
                        id,
                        name,
                        bonus_percent,
                        min_payment_threshold
                    FROM ranks;
                """)
                rows = src_cur.fetchall()

            with dwh_conn.cursor() as dwh_cur:
                dwh_cur.execute("TRUNCATE TABLE stg.bonussystem_ranks;")
                dwh_cur.executemany("""
                    INSERT INTO stg.bonussystem_ranks (
                        id,
                        name,
                        bonus_percent,
                        min_payment_threshold
                    )
                    VALUES (%s, %s, %s, %s);
                """, rows)

            dwh_conn.commit()
            log.info("Loaded %s rows into stg.bonussystem_ranks", len(rows))

        except Exception:
            dwh_conn.rollback()
            raise

        finally:
            src_conn.close()
            dwh_conn.close()

    load_ranks()


dag = load_bonussystem_ranks()
