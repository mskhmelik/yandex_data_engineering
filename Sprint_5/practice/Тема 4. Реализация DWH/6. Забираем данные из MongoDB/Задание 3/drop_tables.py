import psycopg2

def main():
    conn = psycopg2.connect(
        host="localhost",
        port=15432,
        dbname="de",
        user="jovyan",
        password="jovyan",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stg.srv_wf_settings;")
        cur.execute("""
            CREATE TABLE stg.srv_wf_settings (
                id serial PRIMARY KEY,
                workflow_key varchar NOT NULL UNIQUE,
                workflow_settings jsonb NOT NULL
            );
        """)
    conn.close()
    print("recreated stg.srv_wf_settings on localhost:15432")

if __name__ == "__main__":
    main()
