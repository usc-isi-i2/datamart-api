# Manage the materialized views for fuzzy searching based on location

from typing import Tuple

from db.sql.utils import postgres_connection, query_to_dicts

# admin types supported by views, and their property type
ADMIN_TYPES = {
    'country': 'P17',
    #'admin1': 'P2006190001',
    #'admin2': 'P2006190002',
    #'admin3': 'P2006190003',
}

# A short script to create a view for one admin type - for variables whose main subject is the location
_VIEW_TEMPLATE = """
CREATE MATERIALIZED VIEW {view_name} AS
	SELECT
		   e_var_name.node2 AS variable_id,
		   e_dataset.node1 AS dataset_qnode,
		   e_{admin}.node2 AS {admin}_qnode
			FROM edges e_var
			JOIN edges e_var_property ON (e_var_property.node1=e_var.node1 AND e_var_property.label='P1687')
            JOIN edges e_var_name ON (e_var_name.node1=e_var.node1 AND e_var_name.label='P1813')
			JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
			JOIN edges e_main ON (e_var_property.node2=e_main.label)
			JOIN edges e_{admin} ON (e_{admin}.node1=e_main.node1 AND e_{admin}.label='{admin_pnode}')
	WHERE e_var.label='P31' AND e_var.node2='Q50701'
    UNION
	SELECT
		   e_var_name.node2 AS variable_id,
		   e_dataset.node1 AS dataset_qnode,
		   e_{admin}.node2 AS {admin}_qnode
			FROM edges e_var
			JOIN edges e_var_property ON (e_var_property.node1=e_var.node1 AND e_var_property.label='P1687')
            JOIN edges e_var_name ON (e_var_name.node1=e_var.node1 AND e_var_name.label='P1813')
			JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
			JOIN edges e_main ON (e_var_property.node2=e_main.label)
            JOIN edges e_location ON (e_main.node1=e_location.node1 AND e_location.label='P276')
			JOIN edges e_{admin} ON (e_{admin}.node1=e_location.node2 AND e_{admin}.label='{admin_pnode}')
	WHERE e_var.label='P31' AND e_var.node2='Q50701';
	
CREATE INDEX ix_{view_name} ON {view_name} (variable_id, dataset_qnode);
"""

def get_view_name(admin: str):
    return f"fuzzy_{admin}"

def _get_view_query(admin: str, admin_pnode: str):
    view_name = get_view_name(admin)

    template = _VIEW_TEMPLATE
    view_name = get_view_name(admin)
    template = template.replace('{view_name}', view_name)
    template = template.replace('{admin}', admin)
    template = template.replace('{admin_pnode}', admin_pnode)

    return template

def does_view_exists(conn, admin):
    view_name = get_view_name(admin)
    query = f"SELECT * FROM pg_matviews WHERE matviewname='{view_name}'"
    result = query_to_dicts(query, conn)
    return len(result)

def create_view(conn, admin, admin_pnode, debug=False):
    view_name = get_view_name(admin)
    query = _get_view_query(admin, admin_pnode)

    with conn.cursor() as cursor:
        if debug:
            print(query)
        cursor.execute(query)

def drop_view(conn, admin, debug=False):
    view_name = get_view_name(admin)
    query = f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"

    with conn.cursor() as cursor:
        if debug:
            print(query)
        cursor.execute(query)

def refresh_view(conn, admin):
    view_name = get_view_name(admin)
    query = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};"
    with conn.cursor() as cursor:
        if debug:
            print(query)
        conn.execute(query)

def refresh_all_views(config=None, debug=False):
    with postgres_connection(config) as conn:
        for admin in ADMIN_TYPES.keys():
            refresh_view(conn, location, admin, debug=debug)
