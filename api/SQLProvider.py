# This file contains a class that implements a lot of SQL queries.
# The idea behind this class is to replace it with an equivalent implementation that performs SPARQL queries.
#
# We need to reorganize this in the future, all queries shouldn't be in the same place

from db.sql.utils import query_to_dicts

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
			LEFT JOIN edges AS e_stated_label
				JOIN strings AS s_stated_label ON (s_stated_label.edge_id=e_stated_label.id)
			ON (e_stated_label.node1=e_stated.id AND e_stated_label.label='label')

        WHERE e_main.label='{property_id}' AND e_dataset.node2 IN ('{dataset_id}', 'Q{dataset_id}') AND {places_clause}
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
        rows = query_to_dicts(query)

        result_dict = { row['country']: row['qnode'] for row in rows }

        # The result dictionary contains all the countries we have found, we need to add those we did not find
        found_countries = set([country.lower() for country in result_dict.keys()])
        for country in countries:
            if country.lower() not in found_countries:
                result_dict[country] = None

        return result_dict
