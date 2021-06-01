# This file provides utilities for defining SQL queries for the various supported backends
from flask import current_app

def get_query_helper(config=None):
    if config is None:
        config = current_app.config
    if not 'STORAGE_BACKEND' in config:
        raise ValueError("Can't determine storage backend from config")

    if config['STORAGE_BACKEND'] == 'postgres':
        return PostgresQueryHelper()
    elif config['STORAGE_BACKEND'] == 'sql-server':
        return SqlServerQueryHelper()
    
    raise ValueError("Unsupported backend " + config['STORAGE_BACKEND'])


class PostgresQueryHelper:
    def date_field(self, field: str) -> str:
        return  f"to_json({field})#>>'{{}}' || 'Z'"

    def add_limit(self, query: str, limit: int) -> str:
        query = query.strip()
        if query[:7] != 'SELECT ':
            raise ValueError("Can't add limit to a query that does not start with SELECT")

        query += f"\nLIMIT {limit}"
        return query

class SqlServerQueryHelper:
    def date_field(self, field: str) -> str:
        return f"CONVERT(nvarchar(30), {field}, 126)"

    def add_limit(self, query: str, limit: int) -> str:
        query = query.strip()
        if query[:7] != 'SELECT ':
            raise ValueError("Can't add limit to a query that does not start with SELECT")
            
        query = f"SELECT TOP {limit} " + query[7:]
        return query


