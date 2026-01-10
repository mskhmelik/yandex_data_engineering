import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)

WF_KEY = "ordersystem_users"
WF_TS_KEY = "last_update_ts"


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=False,
)
def sprint5_stg_order_system_users():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Mongo variables
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_users():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        with dwh_pg_connect.connection() as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    # create wf settings if missing (safe)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
                            id serial PRIMARY KEY,
                            workflow_key varchar NOT NULL UNIQUE,
                            workflow_settings jsonb NOT NULL
                        );
                    """)

                    # 1) read last watermark
                    cur.execute(
                        """
                        SELECT workflow_settings->>%s
                        FROM stg.srv_wf_settings
                        WHERE workflow_key = %s;
                        """,
                        (WF_TS_KEY, WF_KEY),
                    )
                    row = cur.fetchone()
                    last_ts = row[0] if row and row[0] is not None else None

                # 2) read users from Mongo incrementally
                # update_ts exists in each document
                # if first run -> read all
                query = {}
                if last_ts:
                    query = {"update_ts": {"$gt": last_ts}}

                users = list(mongo_connect.client()[db]["users"].find(query))
                if not users:
                    conn.commit()
                    log.info("No new users. last_ts=%s", last_ts)
                    return

                # 3) write to stg.ordersystem_users
                # table structure: (id serial pk, object_id varchar, object_value text, update_ts timestamp)
                rows = []
                max_ts = last_ts
                for doc in users:
                    object_id = str(doc.get("_id"))
                    update_ts = doc.get("update_ts")

                    # keep full document as string
                    object_value = str(doc)

                    rows.append((object_id, object_value, update_ts))

                    if max_ts is None or (update_ts and str(update_ts) > str(max_ts)):
                        max_ts = update_ts

                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO stg.ordersystem_users (object_id, object_value, update_ts)
                        VALUES (%s, %s, %s);
                        """,
                        rows,
                    )

                    # 4) save watermark (same transaction)
                    cur.execute(
                        """
                        INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                        VALUES (%s, jsonb_build_object(%s, %s::text))
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings =
                            stg.srv_wf_settings.workflow_settings ||
                            jsonb_build_object(%s, %s::text);
                        """,
                        (WF_KEY, WF_TS_KEY, str(max_ts), WF_TS_KEY, str(max_ts)),
                    )

                conn.commit()
                log.info("Loaded %s users into stg.ordersystem_users. new_last_ts=%s", len(rows), max_ts)

            except Exception:
                conn.rollback()
                raise

    load_users()


users_stg_dag = sprint5_stg_order_system_users()
