import psycopg2

def setup_database():
    try:
        # Read schema from .sql file
        with open("create_schema.sql", "r") as f:
            schema_sql = f.read()

        # Connect to your Postgres database
        conn = psycopg2.connect(
            dbname="footy_data",
            user="postgres",
            password="ArnaudZander10!",   
            host="localhost",
            port="5432"
        )

        cur = conn.cursor()
        cur.execute(schema_sql)
        conn.commit()

        print("✅ Database schema created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print("❌ Error during DB setup:", e)

if __name__ == "__main__":
    setup_database()
