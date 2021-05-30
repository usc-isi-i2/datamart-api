from db.sql.db_encapsulation import _get_db_config, _postgres_connection, _sqlserver_connection
from typing import Dict, List
from timeit import default_timer

from pandas import DataFrame

def db_connection(config=None):
    config = _get_db_config(config)

    if 'STORAGE_BACKEND' not in config:
        raise ValueError('The configuration must have a storage backend')

    if config['STORAGE_BACKEND'] == 'postgres':
        return _postgres_connection(config)
    elif config['STORAGE_BACKEND'] == 'sql-server':
        return _sqlserver_connection(config)
    else:
        raise ValueError('Unsupported storage backend ' + config['STORAGE_BACKEND'])


def query_to_dicts(sql: str, conn=None, config=None) -> List[Dict]:
    """ Runs an SQL query, return a list of dictionaries - one for each row """
    # If conn is none, a new connection is opened by using config
    row_dicts = []
    our_conn = False
    if conn is None:
        conn = db_connection(config)
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
        conn = db_connection(config)
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
        conn = db_connection(config)
        our_conn = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows_deleted = cursor.rowcount
            if our_conn:
                conn.commit()
    finally:
        if our_conn:
            conn.close()
    return rows_deleted
