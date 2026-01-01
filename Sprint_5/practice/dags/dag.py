import logging
import psycopg2
import pendulum

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

SRC_CONN_ID = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"

WF_KEY = "bonussystem_events"
WF_JSON_KEY = "last_loaded_id"


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
def load_bonussystem_data():

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

    @task()
    def load_users():
        src_conn = psycopg2.connect(**get_pg_kwargs(SRC_CONN_ID))
        dwh_conn = psycopg2.connect(**get_pg_kwargs(DWH_CONN_ID))

        try:
            with src_conn.cursor() as src_cur:
                src_cur.execute("""
                    SELECT
                        id,
                        order_user_id
                    FROM users;
                """)
                rows = src_cur.fetchall()

            with dwh_conn.cursor() as dwh_cur:
                dwh_cur.execute("TRUNCATE TABLE stg.bonussystem_users;")
                dwh_cur.executemany("""
                    INSERT INTO stg.bonussystem_users (
                        id,
                        order_user_id
                    )
                    VALUES (%s, %s);
                """, rows)

            dwh_conn.commit()
            log.info("Loaded %s rows into stg.bonussystem_users", len(rows))

        except Exception:
            dwh_conn.rollback()
            raise
        finally:
            src_conn.close()
            dwh_conn.close()

    @task()
    def events_load():
        dwh_conn = psycopg2.connect(**get_pg_kwargs(DWH_CONN_ID))
        dwh_conn.autocommit = False

        try:
            with dwh_conn.cursor() as dwh_cur:
                # create settings table if missing
                dwh_cur.execute("""
                    CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
                        workflow_key varchar PRIMARY KEY,
                        workflow_settings jsonb NOT NULL
                    );
                """)

                # read last loaded id
                dwh_cur.execute(
                    """
                    SELECT COALESCE((workflow_settings->>%s)::bigint, 0) AS last_id
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %s;
                    """,
                    (WF_JSON_KEY, WF_KEY),
                )
                row = dwh_cur.fetchone()
                last_id = int(row[0]) if row else 0

            # read new outbox rows from source
            src_conn = psycopg2.connect(**get_pg_kwargs(SRC_CONN_ID))
            try:
                with src_conn.cursor() as src_cur:
                    src_cur.execute(
                        """
                        SELECT
                            id,
                            event_ts,
                            event_type,
                            event_value
                        FROM outbox
                        WHERE id > %s
                        ORDER BY id;
                        """,
                        (last_id,),
                    )
                    new_rows = src_cur.fetchall()
            finally:
                src_conn.close()

            if not new_rows:
                dwh_conn.commit()
                log.info("No new outbox rows. last_id=%s", last_id)
                return

            new_max_id = int(new_rows[-1][0])

            # insert events + update watermark in ONE transaction
            with dwh_conn.cursor() as dwh_cur:
                dwh_cur.executemany(
                    """
                    INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                    VALUES (%s, %s, %s, %s);
                    """,
                    new_rows,
                )

                dwh_cur.execute(
                    """
                    INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%s, jsonb_build_object(%s, %s))
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings =
                        stg.srv_wf_settings.workflow_settings ||
                        jsonb_build_object(%s, %s);
                    """,
                    (WF_KEY, WF_JSON_KEY, new_max_id, WF_JSON_KEY, new_max_id),
                )

            dwh_conn.commit()
            log.info(
                "Loaded %s outbox rows into stg.bonussystem_events. new_last_id=%s",
                len(new_rows),
                new_max_id,
            )

        except Exception:
            dwh_conn.rollback()
            raise
        finally:
            dwh_conn.close()


    ranks = load_ranks()
    users = load_users()
    events = events_load()

    ranks >> users >> events


dag = load_bonussystem_data()
