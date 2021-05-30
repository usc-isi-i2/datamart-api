# This file contains all the code for encapsulating the two supported db engines - Postgres and SQL Server

import psycopg2
import pyodbc
from flask import current_app

def _get_db_config(config=None):
    if config is None:
        if not current_app or not current_app.config:
            raise ValueError('_get_db_config must receive a config if Flask.current_ap is not defined')
        config = current_app.config

    if 'DB' not in config and 'STORAGE_BACKEND' not in config:
        raise ValueError('Configuration must have both STORAGE_BACKEND and DB defined')

    return config

def _postgres_connection(config):
    if config['STORAGE_BACKEND'] != 'postgres':
        raise ValueError("Storage backend is not set to postgres, can't create a Postgres connection")
        
    postgres = config['DB']
    conn = psycopg2.connect(**postgres)

    return PostgresConnection(conn)


# First wrapping, just forward everything to make sure we covered all the functions
class PostgresConnection():
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._conn.__exit__(exc_type, exc_val, exc_tb)

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

def _sqlserver_connection(config):
    if config['STORAGE_BACKEND'] != 'sql-server':
        raise ValueError("Storage backend is not set to sql-server, can't create an SQL Server connection")

    options = config['DB']
    connstr = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER='{options['host']}';DATABASE='{options['database']}';UID='{options['user']}';PWD='{options['password']}'"
    return pyodbc.connect(connstr)