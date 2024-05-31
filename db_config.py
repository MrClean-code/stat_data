import psycopg2

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="web",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        return None
