# This file contains a class that implements a lot of SQL queries.
# The idea behind this class is to replace it with an equivalent implementation that performs SPARQL queries.
#
# We need to reorganize this in the future, all queries shouldn't be in the same place

from db.sql.utils import query_to_dicts
from api.util import TimePrecision, DataInterval

class SQLProvider:
    def get_dataset_id(self, dataset):
        dataset_query = f'''
        SELECT e_dataset.node1 AS dataset_id
        	FROM edges e_dataset
        WHERE e_dataset.label='P1813' AND e_dataset.node2='{dataset}';
        '''
        dataset_dicts = query_to_dicts(dataset_query)
        if len(dataset_dicts) > 0:
            return dataset_dicts[0]['dataset_id']
        return None

    def get_variable_id(self, dataset_id, variable) -> str:
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

    def get_label(self, qnode, default=None, lang='en'):
        label_query = f'''
        SELECT node1 as qnode, text as label
        FROM edges e
        INNER JOIN strings s on e.id = s.edge_id
        WHERE e.node1 = '{qnode}' and e.label = 'label' and s.language='{lang}';
        '''
        label = query_to_dicts(label_query)
        if len(label) > 0:
            return label[0]['label']
        return default

    def next_variable_value(self, dataset_id, prefix) -> int:
        query = f'''
        select max(substring(e_variable.node2 from '{prefix}#"[0-9]+#"' for '#')::INTEGER)  from edges e_variable
        where e_variable.node1 in
	(
		select e_dataset.node2 from edges e_dataset
		where e_dataset.node1 = '{dataset_id}'
		and e_dataset.label = 'P2006020003'
	)
	and e_variable.label = 'P1813' and e_variable.node2 similar to '{prefix}[0-9]+';
        '''
        result = query_to_dicts(query)
        if len(result) > 0 and result[0]['max'] is not None:
            number = result[0]['max'] + 1
        else:
            number = 0
        return number

    def node_exists(self, node1):
        query = f'''
        select e.node1 as node1 from edges e
        where e.node1 = '{node1}'
        '''
        result_dicts = query_to_dicts(query)
        return len(result_dicts) > 0

    def query_variable(self, dataset, variable):
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

    def query_qualifiers(self, variable_id, property_id):
        # Qualifier querying is not implemented yet in SQL
        return {}

    def query_data(self, dataset_id, property_id, places, qualifiers, limit, cols):
        # For now just return a limited set of values, since everything else is added from the metadata cache:
        # main_subject_id, time, value, value_unit
        if places:
            quoted_places = [f"'{place}'" for place in places]
            commatized_places = ', '.join(quoted_places)
            places_clause = f'e_main.node1 IN ({commatized_places})'
        else:
            places_clause = '(1 = 1)'  # Until we have a main-subject id

        query = f"""
        SELECT e_main.node1 AS main_subject_id,
               s_main_label.text AS main_subject,
	       e_main.node1 AS country_id,    -- Patch for now, until we handle proper regions
	       s_main_label.text AS country,  -- Still the patch
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
            JOIN edges AS e_value_unit ON (e_value_unit.node1=q_main.unit AND e_value_unit.label='label')
            JOIN strings AS s_value_unit ON (e_value_unit.id=s_value_unit.edge_id)
			JOIN edges AS e_main_label ON (e_main.node1=e_main_label.node1 AND e_main_label.label='label')
			JOIN strings AS s_main_label ON (e_main_label.id=s_main_label.edge_id)
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

    def query_country_qnodes(self, countries):
        # Translates countries to Q-nodes. Returns a dictionary of each input country and its QNode (None if not found)
        # We look for countries in a case-insensitive fashion.
        if not countries:
            return {}

        lower_countries = [country.lower() for country in countries]
        quoted_countries = [f"'{country}'" for country in lower_countries]
        countries_in = ', '.join(quoted_countries)

        query = f'''
            SELECT e_country.node1 as qnode, s_country_label.text AS country
            	FROM edges e_country
	            JOIN edges e_country_label ON (e_country_label.node1=e_country.node1 AND e_country_label.label='label')
	            JOIN strings s_country_label ON (e_country_label.id=s_country_label.edge_id)
            WHERE e_country.label='P31' AND e_country.node2='Q6256' AND LOWER(s_country_label.text) IN ({countries_in})
        ''';
        print(query)
        rows = query_to_dicts(query)

        result_dict = { row['country']: row['qnode'] for row in rows }

        # The result dictionary contains all the countries we have found, we need to add those we did not find
        found_countries = set([country.lower() for country in result_dict.keys()])
        for country in countries:
            if country.lower() not in found_countries:
                result_dict[country] = None

        print(result_dict)
        return result_dict

    def query_dataset_metadata(self, dataset_name=None, include_dataset_qnode=False):
        """ Returns the metadata of the dataset. If no name is provided, all datasets are returned """

        # Shortcut to helper, with only the parameters we need
        def join_edge(alias, label, satellite_type=None, left=False):
            return self.join_edge('e_dataset', alias, label, satellite_type, left)

        if dataset_name:
            dataset_id = self.get_dataset_id(dataset_name)
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
               s_url.text AS url


            FROM edges e_dataset
        '''
        # Mandatory First
        query += join_edge('name', 'P1476', 's')
        query += join_edge('description', 'description', 's')
        query += join_edge('url', 'P2699', 's')
        query += join_edge('short_name', 'P1813', 's')


        query += f'''
        WHERE e_dataset.label='P31' AND e_dataset.node2='Q1172284' AND {filter}
        ''';

        print(query)
        return query_to_dicts(query)

    def query_dataset_variables(self, dataset):
        def join_edge(alias, label, satellite_type=None, qualifier=False, left=False):
            return self.join_edge('e_var', alias, label, satellite_type=satellite_type, qualifier=qualifier, left=left)

        dataset_id = self.get_dataset_id(dataset)
        if not dataset_id:
            return None

        # query = f"""
        # SELECT '{dataset}' AS dataset_short_name,
        #        s_name.text AS name,
        #        e_short_name.node2 AS short_name

        # FROM edges e_var
        # JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
        # """
        query = f"""
        SELECT '{dataset}' AS dataset_id,
               s_name.text AS name,
               e_short_name.node2 AS variable_id

        FROM edges e_var
        JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
        """

        # Mandatory fields first
        query += join_edge('short_name', 'P1813')
        # Now optional fields that are supposed to be required
        query += join_edge('name', 'P1476', 's', left=True)

        query += f"""
        WHERE e_var.label='P31' AND e_var.node2='Q50701' AND e_dataset.node1='{dataset_id}'
        """

        print(query)
        return query_to_dicts(query)

    def query_variable_metadata(self, dataset, variable):
        def join_edge(alias, label, satellite_type=None, qualifier=False, left=False):
            return self.join_edge('e_var', alias, label, satellite_type=satellite_type, qualifier=qualifier, left=left)

        def fix_time_precisions(row):
            if row['start_time_precision'] is not None:
                row['start_time_precision'] = TimePrecision.to_name(row['start_time_precision'])
            if row['end_time_precision'] is not None:
                row['end_time_precision'] = TimePrecision.to_name(row['end_time_precision'])

        def fix_interval(row):
            if row['data_interval'] is not None:
                row['data_interval'] = DataInterval.qnode_to_name(row['data_interval'])

        def run_query(select_clause, join_clause):
            nonlocal from_clause, where_clause
            query = f"""
            {select_clause}
            {from_clause}
            {join_clause}
            {where_clause}
            """
            print(query)
            return query_to_dicts(query)

        def fetch_scalars():
#                e_short_name.node2 AS short_name,
            select = f"""
            SELECT s_name.text AS name,
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

        def fetch_list(entity, edge_label, return_ids=False):
            select = f"""
            SELECT e_{entity}.node2 AS identifier, s_{entity}_label.text AS name
            """

            join = f"""
            JOIN edges e_{entity} ON (e_var.node1=e_{entity}.node1 AND e_{entity}.label='{edge_label}')
            LEFT JOIN edges e_{entity}_label
                JOIN strings s_{entity}_label ON (e_{entity}_label.id=s_{entity}_label.edge_id)
            ON (e_{entity}.node2=e_{entity}_label.node1 AND e_{entity}_label.label='label')
            """

            rows = run_query(select, join)

            if return_ids:
                return rows

            # We need to return just the entities, in a simple list
            entity_list = [row['name'] for row in rows]
            return entity_list

            # We need to turn this into a list of the label field. We could in

        def fetch_stated_as_list(entity, edge_label, return_ids=False):
            select = f"""
            SELECT e_{entity}.node2 AS identifier, e_qualifier_stated_as.node2 AS name
            """

            join = f"""
            JOIN edges e_{entity} ON (e_var.node1=e_{entity}.node1 AND e_{entity}.label='{edge_label}')
            LEFT JOIN edges e_{entity}_stated_as ON (e_{entity}_stated_as.node1 = e_{entity}.id)
            """

            rows = run_query(select, join)

            if return_ids:
                return rows

            # We need to return just the entities, in a simple list
            entity_list = [row['name'] for row in rows]
            return entity_list

            # We need to turn this into a list of the label field. We could in

        dataset_id = self.get_dataset_id(dataset)
        if not dataset_id:
            return None

        variable_id = self.get_variable_id(dataset_id, variable)
        if not variable_id:
            return None

        # We perform several similar queries - one to get all the scalar fields of a dataset, and
        # then one for each list field. Some parts of these queries are identical:
        from_clause = f"""
        FROM edges e_var
        JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
        """
        where_clause = f"""
        WHERE e_var.label='P31' AND e_var.node2='Q50701' AND e_dataset.node1='{dataset_id}'  AND e_var.node1='{variable_id}'
        """

        result = fetch_scalars()[0]
        fix_time_precisions(result)
        result['main_subject'] = fetch_list('main_subject', 'P921')
        result['unit_of_measure'] = fetch_list('unit_of_measure', 'P1880')
        result['country'] = fetch_list('country', 'P17')
        result['location'] = fetch_list('location', 'P276')
        # result['qualifier'] = fetch_stated_as_list('qualifier', 'P2006020002', True)
        result['qualifier'] = fetch_list('qualifier', 'P2006020002', True)
        return result

    def join_edge(self,  main_table, alias, label, satellite_type=None, qualifier=False, left=False):
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

    def fuzzy_query_variables(self, question):
        if not question:
            return []

        # Use Postgres's full text search capabilities
        sql = f"""
        SELECT fuzzy.variable_id, fuzzy.dataset_qnode, fuzzy.name,  ts_rank(variable_text, plainto_tsquery('{question}')) AS rank FROM
            (SELECT e_var_name.node2 AS variable_id,
                    -- e_dataset_name.node2 AS dataset_id,
                    e_dataset.node1 AS dataset_qnode,
                    to_tsvector(CONCAT(s_description.text, ' ', s_name.text, ' ', s_label.text)) AS variable_text,
                    CONCAT(s_name.text, ' ', s_label.text) as name
                FROM edges e_var
                JOIN edges e_var_name ON (e_var_name.node1=e_var.node1 AND e_var_name.label='P1813')
                JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
                        -- JOIN edges e_dataset_name ON (e_dataset_name.node1=e_dataset.node1 AND e_dataset_name.label='P1813')
                LEFT JOIN edges e_description JOIN strings s_description ON (e_description.id=s_description.edge_id) ON (e_var.node1=e_description.node1 AND e_description.label='description')
                LEFT JOIN edges e_name JOIN strings s_name ON (e_name.id=s_name.edge_id) ON (e_var.node1=e_name.node1 AND e_name.label='P1813')
                LEFT JOIN edges e_label JOIN strings s_label ON (e_label.id=s_label.edge_id) ON (e_var.node1=e_label.node1 AND e_label.label='label')

            WHERE e_var.label='P31' AND e_var.node2='Q50701') AS fuzzy
        WHERE variable_text @@ plainto_tsquery('{question}')
        ORDER BY rank DESC
        LIMIT 10
        """
        print(sql)
        results = query_to_dicts(sql)

        return results
