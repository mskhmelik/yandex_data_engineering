import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=15432,
    database="de",
    user="jovyan",
    password="jovyan"
)

conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("""
        DROP TABLE IF EXISTS cdm.dm_settlement_report CASCADE;
    """)

conn.close()
