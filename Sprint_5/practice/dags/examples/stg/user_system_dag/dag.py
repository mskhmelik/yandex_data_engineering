import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.restaurant_loader import RestaurantLoader
from examples.stg.order_system_restaurants_dag.restaurant_reader import RestaurantReader

from examples.stg.order_system_users_dag.pg_saver import PgSaverUsers
from examples.stg.order_system_users_dag.user_loader import UserLoader
from examples.stg.order_system_users_dag.user_reader import UserReader

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def sprint5_stg_order_system():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        pg_saver = PgSaver()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task()
    def load_users():
        pg_saver = PgSaverUsers()
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = UserReader(mongo_connect)
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    r = load_restaurants()
    u = load_users()

    r >> u


order_stg_dag = sprint5_stg_order_system()  # noqa
