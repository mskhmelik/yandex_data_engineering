import psycopg2
from pprint import pprint

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=15432,
        database="de",
        user="jovyan",
        password="jovyan"
    )

    with conn.cursor() as cur:
        print("\n=== stg.bonussystem_events (first 100 rows) ===")
        cur.execute("""
            SELECT *
            FROM stg.bonussystem_events
            LIMIT 1;
        """)
        rows = cur.fetchall()
        for r in rows:
            pprint(r)

        print("\n=== dds.dm_timestamps (first 100 rows) ===")
        cur.execute("""
            SELECT *
            FROM dds.dm_timestamps
            LIMIT 100;
        """)
        rows = cur.fetchall()
        for r in rows:
            pprint(r)

    conn.close()

if __name__ == "__main__":
    main()
