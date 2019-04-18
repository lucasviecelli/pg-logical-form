import psycopg2
import psycopg2.extras
from psycopg2 import sql

def exec_scripts(connection, scripts):
    connection.autocommit=True
    cursor = connection.cursor()

    for sql in scripts:
        cursor.execute(sql)