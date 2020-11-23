from db.sql.dal.general import get_dataset_id
from db.sql.dal.variables import get_variable_id
from db.sql.utils import query_to_dicts, postgres_connection
from api.util import DataInterval, TimePrecision

def query_dataset_metadata(dataset_name=None, include_dataset_qnode=False, debug=False):
    """ Returns the metadata of the dataset. If no name is provided, all datasets are returned """

    # Shortcut to helper, with only the parameters we need
    def join_edge(alias, label, satellite_type=None, left=False):
        return _join_edge_helper('e_dataset', alias, label, satellite_type=satellite_type, left=left)

    if dataset_name:
        dataset_id = get_dataset_id(dataset_name)  # Already calls sanitize
        if not dataset_id:
            return None

        filter = f"e_dataset.node1='{dataset_id}'"
    else:
        filter = '1=1'

    # query = f'''
    # SELECT e_dataset.node1 AS dataset_id,
    #        s_name.text AS name,
    #        s_description.text AS description,
    #        s_url.text AS url,
    #        s_short_name.text AS short_name

    #     FROM edges e_dataset
    # '''
    if include_dataset_qnode:
        qnode_select = 'e_dataset.node1 as dataset_qnode,'
    else:
        qnode_select = ''
    query = f'''
    SELECT {qnode_select}
            s_short_name.text AS dataset_id,
            s_name.text AS name,
            s_description.text AS description,
            s_url.text AS url,
            d_last_update.date_and_time AS last_update,
            d_last_update.precision AS last_update_precision


        FROM edges e_dataset
    '''
    # Mandatory First
    query += join_edge('name', 'P1476', 's')
    query += join_edge('description', 'description', 's')
    query += join_edge('url', 'P2699', 's')
    query += join_edge('short_name', 'P1813', 's')
    query += join_edge('last_update', 'P5017', 'd', left=True)


    query += f'''
    WHERE e_dataset.label='P31' AND e_dataset.node2='Q1172284' AND {filter}
    ''';

    if debug:
        print(query)
    return query_to_dicts(query)

def query_dataset_variables(dataset, debug=False):
    def join_edge(alias, label, satellite_type=None, qualifier=False, left=False):
        return _join_edge_helper('e_var', alias, label, satellite_type=satellite_type, qualifier=qualifier, left=left)

    dataset_id = get_dataset_id(dataset) # Already calls sanitize
    if not dataset_id:
        return None

    inner_query = f"""
    SELECT e_dataset.node1 AS internal_dataset_id, e_var.node1 AS internal_variable_id
        FROM edges e_var
        JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
    WHERE e_var.label='P31' AND e_var.node2='Q50701' AND e_dataset.node1='{dataset_id}'
    """

    return query_variables_metadata(inner_query, debug)

def query_variable_metadata(dataset, variable, debug=False):
    dataset_id = get_dataset_id(dataset)  # Already sanitizes
    if not dataset_id:
        return None

    variable_id = get_variable_id(dataset_id, variable) # Already sanitizes
    if not variable_id:
        return None

    inner_query = f"SELECT '{dataset_id}' AS internal_dataset_id, '{variable_id}' AS internal_variable_id"

    variables = query_variables_metadata(inner_query, debug)
    return variables[0]


def query_variables_metadata(variable_select: str, debug=False):
    def join_edge(alias, label, satellite_type=None, qualifier=False, left=False):
        return _join_edge_helper('e_var', alias, label, satellite_type=satellite_type, qualifier=qualifier, left=left)

    def fix_time_precisions(results):
        for row in results.values():
            if row['start_time_precision'] is not None:
                row['start_time_precision'] = TimePrecision.to_name(row['start_time_precision'])
            if row['end_time_precision'] is not None:
                row['end_time_precision'] = TimePrecision.to_name(row['end_time_precision'])

    def fix_intervals(results):
        for row in results.values():
            if row['data_interval'] is not None:
                row['data_interval'] = DataInterval.qnode_to_name(row['data_interval'])

    def run_query(select_clause, join_clause, order_by_clause=""):
        nonlocal from_clause, where_clause
        query = f"""
        {select_clause}
        {from_clause}
        {join_clause}
        {where_clause}
        {order_by_clause}
        """
        if debug:
            print(query)
        return query_to_dicts(query)

    def fetch_scalars():
#                e_short_name.node2 AS short_name,
        select = f"""
        SELECT  e_dataset.node1 AS internal_dataset_id, e_var.node1 AS internal_variable_id,
            s_name.text AS name,
            e_short_name.node2 AS variable_id,
            COALESCE(s_description.text, s_label.text) AS description,
            e_corresponds_to_property.node2 AS corresponds_to_property,
            to_json(d_start_time.date_and_time)#>>'{{}}' || 'Z' AS start_time,
            d_start_time.precision AS start_time_precision,
            to_json(d_end_time.date_and_time)#>>'{{}}' || 'Z' AS end_time,
            d_end_time.precision AS end_time_precision,
            e_data_interval.node2 AS data_interval,
            s_column_index.text AS column_index,
            q_count.number AS count
        """

        # Mandatory fields first
        join = ""
        join += join_edge('short_name', 'P1813')
        join += join_edge('label', 'label', 's')
        # Now optional fields that are supposed to be required
        join += join_edge('corresponds_to_property', 'P1687', left=True)
        join += join_edge('description', 'description', 's', left=True)
        join += join_edge('name', 'P1476', 's', left=True)
        # Now truely optional fields
        join += join_edge('start_time', 'P580', 'd', left=True)
        join += join_edge('end_time', 'P582', 'd', left=True)
        join += join_edge('data_interval', 'P6339', left=True)
        join += join_edge('column_index', 'P2006020001', 's', left=True)
        join += join_edge('count', 'P1114', 'q', left=True)

        return run_query(select, join)

    def fetch_list(entity, edge_label, results, qualifier=False, name_field='name'):
        select = f"""
        SELECT e_dataset.node1 AS internal_dataset_id, e_var.node1 AS internal_variable_id, e_{entity}.node2 AS identifier, s_{entity}_label.text AS name
        """

        if qualifier:
            select += f", e_{entity}_data_type.node2 AS wikidata_data_type"

        join = f"""
        JOIN edges e_{entity} ON (e_var.node1=e_{entity}.node1 AND e_{entity}.label='{edge_label}')
        LEFT JOIN edges e_{entity}_label
            JOIN strings s_{entity}_label ON (e_{entity}_label.id=s_{entity}_label.edge_id)
        ON (e_{entity}.node2=e_{entity}_label.node1 AND e_{entity}_label.label='label')
        """

        if qualifier:
            join += f"""
                LEFT JOIN edges e_{entity}_data_type
                ON (e_{entity}.node2= e_{entity}_data_type.node1 AND e_{entity}_data_type.label='wikidata_data_type')
            """
        order_by = "ORDER BY internal_dataset_id, internal_variable_id"

        list_rows = list(run_query(select, join, order_by))

        internal_variable_id = internal_dataset_id = current_result = current_list = None
        for row in list_rows:
            if row['internal_variable_id'] != internal_variable_id or row['internal_dataset_id'] != internal_dataset_id:
                internal_variable_id = row['internal_variable_id']
                internal_dataset_id = row['internal_dataset_id']
                current_result = results[(internal_dataset_id, internal_variable_id)]
                current_list = current_result[entity] = []

            if not qualifier:
                element = row[name_field]
            else:
                element = { 'name': row['name'], 'identifier': row['identifier'] }
                if row['wikidata_data_type']:  # Don't add anything if it's not stored explicitly in the database
                    element['data_type'] = row['wikidata_data_type']

            current_list.append(element)

    # We perform several similar queries - one to get all the scalar fields of a dataset, and
    # then one for each list field. Some parts of these queries are identical:
    from_clause = f"""
    FROM edges e_var
    JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
    JOIN ({variable_select}) e_inner_variable ON (e_dataset.node1=e_inner_variable.internal_dataset_id AND e_var.node1=e_inner_variable.internal_variable_id)
    """
    where_clause = f"""
    WHERE e_var.label='P31' AND e_var.node2='Q50701'
    """

    rows = fetch_scalars()
    results = {(row['internal_dataset_id'], row['internal_variable_id']): row for row in rows}
    fix_time_precisions(results)
    fetch_list('main_subject', 'P921', results)
    fetch_list('unit_of_measure', 'P1880', results)
    fetch_list('country', 'P17', results)
    fetch_list('location', 'P276', results)
    fetch_list('qualifier', 'P2006020002', results, True)
    fetch_list('tag', 'P2010050001', results, False, name_field='identifier')

    # Now clean the results - drop the internal fields
    variables = list(results.values())
    for variable in variables:
        del variable['internal_dataset_id']
        del variable['internal_variable_id']

    return variables

def _join_edge_helper( main_table, alias, label, satellite_type=None, qualifier=False, left=False):
    edge_table_alias = f'e_{alias}'

    if qualifier:
        main_table_ref = f'{main_table}.id'
    else:
        main_table_ref = f'{main_table}.node1'

    if satellite_type:
        satellites = dict(s='strings', sym='symbols', q='quantities', d='dates', c='coordinates')
        if satellite_type not in satellites:
            raise ValueError('Unsupported satellite type ' + satellite_type)
        satellite_table_name = satellites[satellite_type]
        satellite_table_alias = f'{satellite_type}_{alias}'

        satellite_join = f"JOIN {satellite_table_name} {satellite_table_alias} ON ({edge_table_alias}.id={satellite_table_alias}.edge_id)"
    else:
        satellite_join = ""

    sql = f"JOIN edges {edge_table_alias} {satellite_join} ON ({edge_table_alias}.node1={main_table_ref} AND {edge_table_alias}.label='{label}')"

    if left:
        sql = "LEFT " + sql;
    return '\t' + sql  + '\n';

def delete_variable_metadata(dataset_id, variable_qnodes, labels=None, debug=False):
    if not labels:
        labels_where = "1=1"
    else:
        labels_str =', '.join([f"'{label}'" for label in labels])
        labels_where = f"label in ({labels_str})"

    if not variable_qnodes:
        raise ValueError("At least one variable QNode should be supplied")
    variable_qnodes_str = ', '.join([f"'{qnode}'" for qnode in variable_qnodes])
    variable_qnodes_where = f'node1 in ({variable_qnodes_str})'

    with postgres_connection() as conn:
        with conn.cursor() as cursor:
            query = f"""DELETE FROM edges WHERE {variable_qnodes_where} AND {labels_where}"""
            if debug:
                print(query)
            cursor.execute(query)

def delete_dataset_metadata(dataset_qnode, debug=False):
    with postgres_connection() as conn:
        with conn.cursor() as cursor:
            query = f"DELETE FROM edges WHERE node1='{dataset_qnode}'"
            if debug:
                print(query)
            cursor.execute(query)

def delete_dataset_last_update(dataset_qnode, debug=False):
    with postgres_connection() as conn:
        with conn.cursor() as cursor:
            query = f"DELETE FROM edges WHERE node1='{dataset_qnode}' and label='P5017'"
            if debug:
                print(query)
            cursor.execute(query)
