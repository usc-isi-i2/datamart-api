from db.sql.dal.general import sanitize
from db.sql.utils import postgres_connection, query_to_dicts
from typing import Union, Dict, List, Tuple, Any, Set
from abc import ABC, abstractmethod, abstractproperty


class Qualifier:
    name: str
    label: str
    data_type: str
    is_region: bool
    is_optional: bool
    join_clause: str
    fields: Dict[str, str]  # Field name to select_clause_field

    DATA_TYPES = ['date_and_time', 'string', 'symbol', 'quantity', 'coordinate', 'location']

    def __init__(self, name, label, wikidata_data_type=None):
        if name == 'point in time':  # Override hardly for now
            name = 'time'
        self.name = name
        self.label = label
        self.is_region = False  # For now qualifiers are not regions
        self.is_optional = name != 'time'
        self.data_type = self._get_data_type(wikidata_data_type)

        if self.data_type == 'location' and (not self.name or self.label == 'P131'):  # P131 qualifiers are always 'location'
            self.name = 'location'

        self._init_sql()

    LOCATION_PROPS = {'P17': 'country', 'P2006190001': 'admin1', 'P2006190002': 'admin2', 'P2006190003': 'admin3',
                      'P131': 'location'}

    WIKIDATA_TYPE_MAP = {
        'GlobeCoordinate': 'location',
        'Quantity': 'quantity',
        'Time': 'date_and_time',
        'String': 'string',
        'MonolingualText': 'string',
        'ExternalIdentifier': 'symbol',
        'WikibaseItem': 'symbol',
        'WikibaseProperty': 'symbol',
        'Url': 'symbol',
    }
    def _get_data_type(self, wikidata_data_type):
        # First, if the wikidata data type is specified, use it
        if wikidata_data_type:
            if wikidata_data_type not in self.WIKIDATA_TYPE_MAP:
                raise ValueError(f"Unexpected Wikidata Datatype {wikidata_data_type}")
            return self.WIKIDATA_TYPE_MAP[wikidata_data_type]
            
        # The heuristic is simple - we know the types of a few well known qualifiers. All the others
        # are strings
        if self.label == "P585":  # point in time
            return 'date_and_time'
        if self.label == 'P248':  # stated in
            return 'symbol'
        if self.label in self.LOCATION_PROPS.keys():  # Various locations
            return 'location'

        # Everything else is a string
        return 'string'

    @property
    def main_column(self):
        return self.name

    def _init_sql(self):
        main_name = self.main_column
        underscored_main_name = sanitize(main_name.replace(' ', '_'))
        main_table = 'e_' + underscored_main_name
        if self.is_optional:
            join_clause = 'LEFT '
        else:
            join_clause = ''
        join_clause += f"JOIN edges {main_table}"
        join_on_clause = f"ON (e_main.id={main_table}.node1 AND {main_table}.label='{self.label}')"
        if self.data_type == 'date_and_time':
            satellite_table = 'd_' + main_name
            satellite_join = f"JOIN dates {satellite_table} ON ({main_table}.id={satellite_table}.edge_id)"
            self.fields = {
                main_name: f"to_json({satellite_table}.date_and_time)#>>'{{}}' || 'Z'",
                main_name + "_precision": f"{satellite_table}.precision",
            }
        elif self.data_type == 'quantity':
            satellite_table = 'q_' + underscored_main_name
            unit_table = 'e_' + underscored_main_name + '_unit_label'
            unit_string_table = 's' + unit_table[1:]
            satellite_join = f"""
                JOIN quantities {satellite_table}
                    LEFT JOIN edges {unit_table}
                        LEFT JOIN strings {unit_string_table} ON ({unit_table}.id={unit_string_table}.edge_id)
                    ON ({satellite_table}.unit={unit_table}.node1 AND {unit_table}.label='label')
                ON ({main_table}.id={satellite_table}.edge_id)
            """
            self.fields = {
                main_name: f"{satellite_table}.number",
                main_name + "_unit_id": f"{satellite_table}.unit",
                main_name + "_unit": f"{unit_string_table}.text"
            }
        elif self.data_type == 'symbol' or self.data_type == 'location':  # Locations are just symbols at this point
            label_table = 'e_' + underscored_main_name + '_label'
            label_string_table = 's' + label_table[1:]
            satellite_join = f"""
                JOIN edges {label_table}
                    JOIN strings {label_string_table} ON ({label_table}.id={label_string_table}.edge_id)
                ON ({label_table}.node1={main_table}.node2 AND {label_table}.label='label')"""
            self.fields = {
                main_name: f"{label_string_table}.text",
                main_name + "_id": f"{main_table}.node2"
            }
        elif self.data_type == 'string':
            satellite_table = 's_' + underscored_main_name
            satellite_join = f"""
                JOIN strings {satellite_table} ON ({main_table}.id={satellite_table}.edge_id)
            """
            self.fields = {
                main_name: f"{satellite_table}.text",
            }
        elif self.data_type == 'coordinate':
            satellite_table = 'c_' + underscored_main_name
            satellite_join = f"""
                JOIN coordinates {satellite_table} ON ({main_table}.id={satellite_table}.edge_id)
            """
            self.fields = {
                main_name: f"""'POINT(' || {satellite_table}.longitude || ' ' || {satellite_table}.latitude || ')'"""
            }
        else:
            raise ValueError('Qualifiers of type ' + self.data_type + ' are not supported yet')

        self.join_clause = f"""
            {join_clause}
            {satellite_join}
            {join_on_clause}
        """


def get_variable_id(dataset_id, variable, debug=False) -> Union[str, None]:
    dataset_id = sanitize(dataset_id)
    variable = sanitize(variable)

    if debug:
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
    SELECT e_var.node1 AS variable_qnode, e_var.node2 AS variable_id, s_var_label.text AS variable_name, e_property.node2 AS property_id, e_dataset.node1 AS dataset_id, e_dataset_label.node2 AS dataset_name
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

    return variable_dicts[0]


def query_qualifiers(dataset_id, variable_qnode):
    dataset_id = sanitize(dataset_id)
    variable_qnode = sanitize(variable_qnode)
    query = f"""
    SELECT e_qualifier.node2 AS label, s_qualifier_label.text AS name, e_data_type.node2 AS wikidata_data_type
        FROM edges e_var
        JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
        JOIN edges e_qualifier ON (e_var.node1=e_qualifier.node1 AND e_qualifier.label='P2006020002')
        LEFT JOIN edges e_qualifier_label  -- Location qualifiers have no name
            JOIN strings s_qualifier_label ON (e_qualifier_label.id=s_qualifier_label.edge_id)
        ON (e_qualifier.node2=e_qualifier_label.node1 AND e_qualifier_label.label='label')
        LEFT JOIN edges e_data_type
            ON (e_qualifier.node2=e_data_type.node1 AND e_data_type.label='wikidata_data_type')

    WHERE e_var.label='P31' AND e_var.node2='Q50701' AND e_dataset.node1='{dataset_id}'  AND e_var.node1='{variable_qnode}'
    """
    qualifiers = query_to_dicts(query)
    return [Qualifier(**q) for q in qualifiers]

def query_tags(dataset_id, variable_qnode):
    dataset_id = sanitize(dataset_id)
    variable_qnode = sanitize(variable_qnode)

    query = f"""
    SELECT e_tag.node2 AS tag
        FROM edges e_var
        JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
        JOIN edges e_tag ON (e_var.node1=e_tag.node1 AND e_tag.label='P2010050001')
    WHERE e_var.label='P31' AND e_var.node2='Q50701' AND e_dataset.node1='{dataset_id}'  AND e_var.node1='{variable_qnode}'
    """
    result = query_to_dicts(query)
    tags = [r['tag'] for r in result]

    return tags
    


def preprocess_places(places: Dict[str, List[str]], region_field) -> Tuple[str, str]:
    joins: List[str] = []
    wheres: List[str] = []

    admin_edges = {
        'country': 'P17',
        'admin1': 'P2006190001',
        'admin2': 'P2006190002',
        'admin3': 'P2006190003',
    }

    for (type, ids) in places.items():
        if not ids:
            continue

        label = admin_edges[type]
        joins.append(f"LEFT JOIN edges e_{type} ON ({region_field}=e_{type}.node1 AND e_{type}.label='{label}')")

        quoted_ids = [f"'{id}'" for id in ids]
        ids_string = ', '.join(quoted_ids)
        wheres.append(f"e_{type}.node2 IN ({ids_string})")

    if not joins or not wheres:
        return ('', '1=1')
    join = '\n'.join(joins)
    where = ' OR '.join(wheres)

    return join, where


def preprocess_qualifiers(qualifiers: List[Qualifier], cols: List[str]) -> Tuple[str, str]:
    col_set = set(cols)
    fields = []
    joins = []
    for qualifier in qualifiers:
        qualifier_field_set = set(qualifier.fields.keys())
        used_fields = qualifier_field_set & col_set  # | qualifier.required_fields
        if not used_fields:
            continue

        joins.append(qualifier.join_clause)
        for field in used_fields:
            fields.append(qualifier.fields[field] + " AS \"" + field + "\"")

    return ',\n\t\t'.join(fields), '\n'.join(joins)


def query_variable_data(dataset_id, property_id, places: Dict[str, List[str]], qualifiers, limit, cols, debug=False) -> \
List[Dict[str, Any]]:
    dataset_id = sanitize(dataset_id)
    property_id = sanitize(property_id)

    location_qualifiers = [q for q in qualifiers if q.data_type == 'location']
    if len(location_qualifiers) == 0:
        location_node = 'e_main.node1'
    elif len(location_qualifiers) == 1:
        location_node = 'e_location.node2'
    else:
        raise ValueError("There are more than one location qualifiers for variable")

    places_join, places_where = preprocess_places(places, location_node)
    qualifier_fields, qualifier_joins = preprocess_qualifiers(qualifiers, cols)

    query = f"""
    SELECT  e_main.node1 AS main_subject_id,
            s_main_label.text AS main_subject,
            e_dataset.node2 AS dataset_id,
            q_main.number AS value,
            s_value_unit.text AS value_unit,
            {qualifier_fields}
    FROM edges AS e_main
        JOIN quantities AS q_main ON (e_main.id=q_main.edge_id)
        JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
        {qualifier_joins}
        {places_join}
        LEFT JOIN edges AS e_value_unit
            LEFT JOIN strings AS s_value_unit ON (e_value_unit.id=s_value_unit.edge_id)
        ON (e_value_unit.node1=q_main.unit AND e_value_unit.label='label')
        LEFT JOIN edges AS e_main_label
            JOIN strings AS s_main_label ON (e_main_label.id=s_main_label.edge_id)
        ON (e_main.node1=e_main_label.node1 AND e_main_label.label='label')

    WHERE e_main.label='{property_id}' AND e_dataset.node2='{dataset_id}' AND ({places_where})
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
    if debug:
        print(query)

    return query_to_dicts(query)


def delete_variable(dataset_id, variable_id, property_id, debug=False):
    with postgres_connection() as conn:
        with conn.cursor() as cursor:
            # Everything here is running under the same transaction
            # We need to delete all edges for this variable, as well as all edges connected to them.
            # 
            # To do so, we first create a temporary table with the IDs of all the edges that need to be deleted,
            # and then use it to delete the actual edges
            #
            # Step 1. create the temporary table
            query = f"""SELECT e_main.id INTO TEMPORARY TABLE to_be_deleted
                            FROM edges AS e_main
                            JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
                        WHERE e_main.label='{property_id}' AND e_dataset.node2='{dataset_id}';
            """
            if (debug):
                print(query)
            cursor.execute(query)

            # Step 2. Delete all propery edges
            query = f"""
            DELETE FROM edges WHERE node1 IN (SELECT id FROM to_be_deleted);
            """;
            if debug:
                print(query)
            cursor.execute(query)

            # Step 3. Now delete the main edges
            query = f"""
            DELETE FROM edges WHERE id IN (SELECT id FROM to_be_deleted);
            """
            if debug:
                print(query)
            cursor.execute(query)

            # We do not delete the temporary table, it is deleted automatically when the session is closed

def variable_data_exists(dataset_id, property_ids, debug=False):
    # Check whether there is some data for any of the property_ids
    property_ids_str = ', '.join([f"'{property_id}'" for property_id in property_ids])
    query = f"""
            SELECT e_main.id
                FROM edges AS e_main
                JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
            WHERE e_main.label IN ({property_ids_str}) AND e_dataset.node2='{dataset_id}'
            LIMIT 1
    """
    if debug:
        print(query)

    result = query_to_dicts(query)
    return len(result) > 0
