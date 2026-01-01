import psycopg2

SQL = """
CREATE SCHEMA IF NOT EXISTS dds;
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
    print("Schema dds created successfully.")

if __name__ == "__main__":
    main()
