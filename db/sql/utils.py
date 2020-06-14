from timeit import default_timer

import psycopg2
from flask import current_app
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

def query_to_dicts(sql, conn=None, config=None):
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

