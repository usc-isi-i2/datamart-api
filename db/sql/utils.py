from typing import Dict, List
from timeit import default_timer

import psycopg2
from flask import current_app
from pandas import DataFrame
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker


def _get_postgres_config(config=None):
    # Returns the postgres configuration, looking at current_app.config by default
    # (which works when the function is called from within a Flask transaction)
    if config is None:
        if not current_app or not current_app.config:
            raise ValueError('postgres_connection must receive a config if Flask.current_app is not defined')
        config = current_app.config

    return config['POSTGRES']

def postgres_connection(config=None):
    postgres = _get_postgres_config(config)
    conn = psycopg2.connect(**postgres)
    return conn

# Store the engine and session class
_engine = None
_session_cls = None
def create_sqlalchemy_session(config=None):
    global _engine, _session_cls
    # Creates a new SQLAlchemy session. The engine and session class are initialized once, during the first call
    if not _session_cls:
        if not _engine:
            if config:
                pg = config
            else:
                pg = _get_postgres_config()
            connstr = f"postgres+psycopg2://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"

            _engine = create_engine(connstr)
        _session_cls = sessionmaker(bind=_engine)

    return _session_cls()

def query_to_dicts(sql: str, conn=None, config=None) -> List[Dict]:
    """ Runs an SQL query, return a list of dictionaries - one for each row """
    # If conn is none, a new connection is opened by using config
    row_dicts = []
    our_conn = False
    if conn is None:
        conn = postgres_connection(config)
        our_conn = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            col_names = [desc[0] for desc in cursor.description] # From here: https://stackoverflow.com/a/10252273/871910
            for row in cursor:
                row_dict = {}
                for idx, field in enumerate(row):
                    row_dict[col_names[idx]] = field
                row_dicts.append(row_dict)
    finally:
        if our_conn:
            conn.close()

    return row_dicts

def query_edges_to_df(where_clause: str, conn=None, config=None, fix=True) -> DataFrame:
    """ Runs an SQL query on 'edges' table, return a dataframe. """
    sql = 'select id,node1,label,node2,data_type from edges ' + where_clause

    # If conn is none, a new connection is opened by using config
    row_dicts = []
    our_conn = False
    if conn is None:
        conn = postgres_connection(config)
        our_conn = True

    results = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            for (id, node1, label, node2, data_type) in cursor:
                if fix:
                    if data_type == 'string':
                        if not node2.startswith('"'):
                            node2 = f'"{node2}"'
                    elif data_type == 'language_qualified_string':
                        if node2.startswith("'") and node2.endswith("'@en"):
                            pass
                        elif node2.startswith('"\'') and node2.endswith('@en\'"'):
                            node2 = node2[1:-1]
                        else:
                            print(f'problem language_qualified_string: {node2}')
                results.append((id, node1, label, node2))
    finally:
        if our_conn:
            conn.close()

    df = DataFrame(results, columns=['id', 'node1', 'label', 'node2'])
    return df

def delete(sql: str, conn=None, config=None) -> int:
    """ Runs an SQL query """
    # If conn is none, a new connection is opened by using config
    row_dicts = []
    our_conn = False
    if conn is None:
        conn = postgres_connection(config)
        our_conn = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows_deleted = cursor.rowcount
            conn.commit()
    finally:
        if our_conn:
            conn.close()
    return rows_deleted
