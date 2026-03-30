import mysql.connector
from mysql.connector import Error
import os

# Using Environment Variables for security. Fallbacks provided for uninterrupted testing.
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "land-options-db-landoptionstrading.h.aivencloud.com"),
    "port": int(os.environ.get("DB_PORT", 18174)),
    "database": os.environ.get("DB_NAME", "defaultdb"),
    "user": os.environ.get("DB_USER", "avnadmin"),
    "password": os.environ.get("DB_PASSWORD", "AVNS_N-rJ1JffZJx7M5rsQg2"),
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

# NEW: Atomic Transaction Engine (Fixes Bugs 6, 7, 8, 10)
def execute_transaction(queries):
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    try:
        conn.start_transaction()
        for sql, params in queries:
            cursor.execute(sql, params)
            # If an UPDATE statement affected 0 rows, a race-condition check failed (e.g., someone else bought it first)
            if sql.strip().upper().startswith("UPDATE") and cursor.rowcount == 0:
                raise Exception("Transaction condition failed (0 rows affected). Rolling back to prevent data corruption.")
        conn.commit()
        return True
    except Exception as e:
        print(f"Transaction aborted: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
