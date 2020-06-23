from db.sql.dal.general import sanitize
from db.sql.utils import postgres_connection, query_to_dicts
from typing import Union

def get_variable_id(dataset_id, variable) -> Union[str, None]:
    dataset_id = sanitize(dataset_id)
    variable = sanitize(variable)

    print(f'variable_exists({dataset_id}, {variable})')
    variable_query = f'''
    select e_variable.node1 AS variable_id from edges e_variable
    where e_variable.node1 in
(
    select e_dataset.node2 from edges e_dataset
    where e_dataset.node1 = '{dataset_id}'
    and e_dataset.label = 'P2006020003'
)
and e_variable.label = 'P1813' and e_variable.node2 = '{variable}';
    '''
    variable_dicts = query_to_dicts(variable_query)
    if len(variable_dicts) > 0:
        return variable_dicts[0]['variable_id']
    return None


def query_variable(dataset, variable):
    dataset = sanitize(dataset)
    variable = sanitize(variable)

    variable_query = f'''
    SELECT e_var.node2 AS variable_id, s_var_label.text AS variable_name, e_property.node2 AS property_id, e_dataset.node1 AS dataset_id, e_dataset_label.node2 AS dataset_name
        FROM edges e_var
        JOIN edges e_var_label ON (e_var.node1=e_var_label.node1 AND e_var_label.label='label')
        JOIN strings s_var_label ON (e_var_label.id=s_var_label.edge_id)
        JOIN edges e_property ON (e_property.node1=e_var.node1 AND e_property.label='P1687')
        JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_property.node1)
        JOIN edges e_dataset_label ON (e_dataset_label.node1=e_dataset.node1 AND e_dataset_label.label='P1813')
    WHERE e_var.label='P1813' AND e_var.node2='{variable}' AND e_dataset_label.node2='{dataset}';
    '''

    variable_dicts = query_to_dicts(variable_query)
    if not len(variable_dicts):
        return None

    return {
        'dataset_id': variable_dicts[0]['dataset_id'],
        'dataset_name': variable_dicts[0]['dataset_name'],
        'variable_id': variable_dicts[0]['variable_id'],
        'property_id': variable_dicts[0]['property_id'],
        'variable_name': variable_dicts[0]['variable_name'],
    }

def query_qualifiers(variable_id, property_id):
    # Qualifier querying is not implemented yet in SQL
    return {}

def query_variable_data(dataset_id, property_id, places, qualifiers, limit, cols):
    # For now just return a limited set of values, since everything else is added from the metadata cache:
    # main_subject_id, time, value, value_unit
    if places:
        sanitized = [sanitize(place) for place in places]
        quoted_places = [f"'{place}'" for place in sanitized]
        commatized_places = ', '.join(quoted_places)
        places_clause = f'e_main.node1 IN ({commatized_places})'
    else:
        places_clause = '(1 = 1)'  # Until we have a main-subject id
    dataset_id = sanitize(dataset_id)
    property_id = sanitize(property_id)

    query = f"""
    SELECT e_main.node1 AS main_subject_id,
            s_main_label.text AS main_subject,
            q_main.number AS value,
            s_value_unit.text AS value_unit,
            to_json(d_value_date.date_and_time)#>>'{{}}' || 'Z' AS time,
            d_value_date.precision AS time_precision,
            'POINT(' || c_coordinate.longitude || ', ' || c_coordinate.latitude || ')' as coordinate,
            e_dataset.node2 AS dataset_id,
        e_stated.node2 AS stated_in_id,
        s_stated_label.text AS stated_in  -- May be null even if e_stated exists
    FROM edges AS e_main
        JOIN quantities AS q_main ON (e_main.id=q_main.edge_id)
        LEFT JOIN edges AS e_value_unit ON (e_value_unit.node1=q_main.unit AND e_value_unit.label='label')
        LEFT JOIN strings AS s_value_unit ON (e_value_unit.id=s_value_unit.edge_id)
        -- Allow missing main_subject labels
        LEFT JOIN edges AS e_main_label ON (e_main.node1=e_main_label.node1 AND e_main_label.label='label')
        LEFT JOIN strings AS s_main_label ON (e_main_label.id=s_main_label.edge_id)
        JOIN edges AS e_value_date ON (e_value_date.node1=e_main.id AND e_value_date.label='P585')
        JOIN dates AS d_value_date ON (e_value_date.id=d_value_date.edge_id)
        JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
        LEFT JOIN edges AS e_coordinate
            JOIN coordinates AS c_coordinate ON (e_coordinate.id=c_coordinate.edge_id)
            ON (e_coordinate.node1=e_main.node1 AND e_coordinate.label='P625')
        LEFT JOIN edges AS e_stated ON (e_stated.node1=e_main.id AND e_stated.label='P248')
        -- Allow the stated_in label to not exist in the database
        LEFT JOIN edges AS e_stated_label ON (e_stated_label.node1=e_stated.node2 AND e_stated_label.label='label')
            LEFT JOIN strings AS s_stated_label ON (s_stated_label.edge_id=e_stated_label.id)

    WHERE e_main.label='{property_id}' AND e_dataset.node2='{dataset_id}' AND {places_clause}
    ORDER BY main_subject_id, time
    """

    # Some remarks on that query:
    # to_json(d_value_date.date_and_time)... is a recommended way to convert dates to ISO format.
    #
    # Since coordinates are optional, we LEFT JOIN on *A JOIN* of e_coordinate and c_coordinate. The weird
    # syntax of T LEFT JOIN A JOIN B ON (...) ON (...) is the SQL way of explicity specifying which INNER
    # JOINS are LEFT JOINed.
    #
    # We use the || operator on fields from the LEFT JOIN, since x || NULL is NULL in SQL, so coordinate is
    # NULL in the result if there is no coordinate
    if limit > 0:
        query += f"\nLIMIT {limit}\n"
    print(query)

    return query_to_dicts(query)

def delete_variable(dataset_id, variable_id, property_id):
    with postgres_connection() as conn:
        with conn.cursor() as cursor:
            # Everything here is running under the same transaction

            # Delete properties
            query = f"""
            DELETE FROM edges WHERE node1 IN (
                    SELECT e_main.id
                        FROM edges AS e_main
                        JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
                    WHERE e_main.label='{property_id}' AND e_dataset.node2='{dataset_id}'
            );"""
            print(query)
            cursor.execute(query)

            # Now delete the main edges
            query = f"""
            DELETE FROM edges e_main WHERE id IN (
                    SELECT e_main.id
                        FROM edges AS e_main
                        JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
                    WHERE e_main.label='{property_id}' AND e_dataset.node2='{dataset_id}'
            );
            """
            print(query)
            cursor.execute(query)
