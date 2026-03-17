import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    "host": "land-options-db-landoptionstrading.h.aivencloud.com",
    "port": 18174,
    "database": "defaultdb",
    "user": "avnadmin",
    "password": "AVNS_N-rJ1JffZJx7M5rsQg2",
    "ssl_disabled": False,
}


def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None
    return None


def execute_query(sql, params=None, fetch_all=False):
    conn = get_db_connection()
    if not conn:
        return [] if fetch_all else False

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(sql, params or ())
        if sql.strip().upper().startswith("SELECT"):
            result = cursor.fetchall() if fetch_all else cursor.fetchone()
            return result
        else:
            conn.commit()
            return True
    except Error as e:
        print(f"Database Error: {e} | Query: {sql}")
        return [] if fetch_all else False
    finally:
        cursor.close()
        conn.close()