import psycopg2
import settings


def connect():
    try:
        conn = psycopg2.connect(dbname=settings.DBNAME, user=settings.USER, password=settings.PASSWORD,
                                host=settings.HOST, port=settings.PORT)
        print("Database opened successfully")
        return conn
    except psycopg2.OperationalError:
        print("Connection FAILED")

