import mysql.connector
from mysql.connector import Error, ClientFlag
import os

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "land-options-db-landoptionstrading.h.aivencloud.com"),
    "port": int(os.environ.get("DB_PORT", 18174)),
    "database": os.environ.get("DB_NAME", "defaultdb"),
    "user": os.environ.get("DB_USER", "avnadmin"),
    "password": os.environ.get("DB_PASSWORD", "AVNS_N-rJ1JffZJx7M5rsQg2"),
    "ssl_disabled": False,
    "client_flags": [ClientFlag.FOUND_ROWS]
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            return conn
    except Error:
        return None
    return None

def execute_query(sql, params=None, fetch_all=False):
    conn = get_db_connection()
    if not conn:
        return [] if fetch_all else False
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        if sql.strip().upper().startswith("SELECT"):
            return cursor.fetchall() if fetch_all else cursor.fetchone()
        else:
            conn.commit()
            return True
    except Error:
        return [] if fetch_all else False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_transaction(queries):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = None
    try:
        cursor = conn.cursor()
        conn.start_transaction()
        for sql, params in queries:
            cursor.execute(sql, params)
            if sql.strip().upper().startswith("UPDATE") and cursor.rowcount == 0:
                raise Exception()
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
