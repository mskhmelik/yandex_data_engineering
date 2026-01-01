import psycopg2

SQL = """
CREATE TABLE IF NOT EXISTS public.outbox (
    id integer NOT NULL,
    object_id integer NOT NULL,
    record_ts timestamp NOT NULL,
    type varchar NOT NULL,
    payload text NOT NULL
);
"""

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=15432,
        database="de",
        user="jovyan",
        password="jovyan"
    )

    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(SQL)

    conn.close()
    print("Table public.outbox created successfully.")

if __name__ == "__main__":
    main()
