import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
)
def fix_srv_wf_settings():

    @task()
    def rebuild():
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS stg.srv_wf_settings;")
                cur.execute("""
                    CREATE TABLE stg.srv_wf_settings (
                        id serial PRIMARY KEY,
                        workflow_key varchar NOT NULL UNIQUE,
                        workflow_settings jsonb NOT NULL
                    );
                """)
            conn.commit()

    rebuild()


fix_dag = fix_srv_wf_settings()
