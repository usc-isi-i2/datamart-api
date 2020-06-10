
import os
import random
import typing

from enum import Enum

from flask import request, make_response, current_app
from flask_restful import Resource

import pandas as pd
from .util import Labels, Location, TimePrecision

from db.sql.utils import query_to_dicts
from flask.blueprints import Blueprint

DROP_QUALIFIERS = [
    'pq:P585', 'P585' # time
    'pq:P1640',  # curator
    'pq:Pdataset', 'P2006020004' # dataset
]

# Load labels and location
labels = None
location = None

# region.csv is an alternate cleaner version admin hiearchy, compared
# with the admin hierarchy in the Location class. Sub-administractive
# regions are required to have path to super-adminstractive regions
# all the way up to country.
region_df = None
def _init_regions():
    global region_df
    if region_df:
        return
    region_df = pd.read_csv(os.path.join(current_app.config['METADATA_DIR'], 'region.csv'), dtype=str)
    region_df = region_df.fillna('')
    for column in ['country', 'admin1', 'admin2', 'admin3']:
        region_df.loc[:, column] = region_df.loc[:, column].map(lambda s: s.lower())

class ColumnStatus(Enum):
    REQUIRED = 0
    DEFAULT = 1
    OPTIONAL = 2

COMMON_COLUMN = {
    'dataset_id': ColumnStatus.DEFAULT,
    'variable_id': ColumnStatus.DEFAULT,
    'variable': ColumnStatus.REQUIRED,
    'category': ColumnStatus.OPTIONAL,
    'main_subject': ColumnStatus.REQUIRED,
    'main_subject_id': ColumnStatus.DEFAULT,
    'value': ColumnStatus.REQUIRED,
    'value_unit': ColumnStatus.DEFAULT,
    'time': ColumnStatus.REQUIRED,
    'time_precision': ColumnStatus.DEFAULT,
    'country': ColumnStatus.DEFAULT,
    'country_id': ColumnStatus.OPTIONAL,
    'admin1': ColumnStatus.DEFAULT,
    'admin1_id': ColumnStatus.OPTIONAL,
    'admin2': ColumnStatus.DEFAULT,
    'admin2_id': ColumnStatus.OPTIONAL,
    'admin3': ColumnStatus.DEFAULT,
    'admin3_id': ColumnStatus.OPTIONAL,
    'place': ColumnStatus.DEFAULT,
    'place_id': ColumnStatus.OPTIONAL,
    'coordinate': ColumnStatus.DEFAULT,
    'shape': ColumnStatus.OPTIONAL
}

class GeographyLevel(Enum):
    COUNTRY = 0
    ADMIN1 = 1
    ADMIN2 = 2
    ADMIN3 = 3
    OTHER = 4

class VariableResource(Resource):
    def init_resources(self):
        global labels, location
        
        if not labels and not location:
            _init_regions()
            Labels.initialize(current_app.config)
            labels = Labels()
            Location.initialize(current_app.config)
            location = Location()

    def get(self, dataset=None, variable=None):
        self.init_resources()
        if dataset == 'Qwikidata':
            dataset = 'Wikidata'
        if dataset not in ['Wikidata', 'UAZ']:
            content = {
                'Error': f'path not found: /datasets/{dataset}',
                'Usage': 'Use path /datasets/Wikidata/variables/{variable}'
            }
            return content, 404
        if variable is None:
            content = {
                'Error': f'path not found: /datasets/{dataset}/variables/{variable}',
                'Usage': f'Use path /datasets/{dataset}/variables/{{PNode}}',
                'Example': f'Use path /datasets/{dataset}/variables/VP1200149'
            }
            return content, 404

        include_cols = []
        exclude_cols = []
        main_subjects = []
        limit = -1
        if request.args.get('include') is not None:
            include_cols = request.args.get('include').split(',')
        if request.args.get('exclude') is not None:
            exclude_cols = request.args.get('exclude').split(',')
        if request.args.get('limit') is not None:
            try:
                limit = int(request.args.get('limit'))
            except:
                pass

        # Add main subject by exact English label
        for keyword in ['country', 'admin1', 'admin2', 'admin3']:
            if request.args.get(keyword) is not None:
                admins = [x.lower() for x in request.args.get(keyword).split(',')]
                index = region_df.loc[:, keyword].isin(admins)
                print(f'Add {keyword}:', region_df.loc[index, keyword + '_id'].unique())
                main_subjects += [x for x in region_df.loc[index, keyword + '_id'].unique()]

        # Add main subject by qnode
        for keyword in ['main_subject_id', 'country_id', 'admin1_id', 'admin2_id', 'admin3_id']:
            if request.args.get(keyword) is not None:
                qnodes = request.args.get(keyword).split(',')
                print(f'Add {keyword}:', qnodes)
                main_subjects += qnodes

        # Add administrative locations using the name of parent administrative location
        for keyword, admin_col, lower_admin_col in zip(
                ['in_country', 'in_admin1', 'in_admin2'],
                ['country', 'admin1', 'admin2'],
                ['admin1_id', 'admin2_id', 'admin3_id']):
            if request.args.get(keyword) is not None:
                admins = [x.lower() for x in request.args.get(keyword).split(',')]
                index = region_df.loc[:, admin_col].isin(admins)
                print(f'Add {keyword}({request.args.get(keyword)}):',
                      region_df.loc[index, lower_admin_col].unique())
                main_subjects += qnodes

        # Add administrative locations using the qnode of parent administrative location
        for keyword, admin_col, lower_admin_col in zip(
                ['in_country_id', 'in_admin1_id', 'in_admin2_id'],
                ['country_id', 'admin1_id', 'admin2_id'],
                ['admin1_id', 'admin2_id', 'admin3_id']):
            if request.args.get(keyword) is not None:
                admin_ids = request.args.get(keyword).split(',')
                index = region_df.loc[:, admin_col].isin(admin_ids)
                print(f'Add {keyword}({request.args.get(keyword)}):',
                      region_df.loc[index, lower_admin_col].unique())
                main_subjects += [x for x in region_df.loc[index, lower_admin_col].unique()]

        print((dataset, variable, include_cols, exclude_cols, limit, main_subjects))
        # return self.get_using_cache(dataset, variable, include_cols, exclude_cols, limit, main_subjects=main_subjects)

        return self.get_direct(dataset, variable, include_cols, exclude_cols, limit, main_subjects=main_subjects)

    def fix_time_precision(self, precision):
        try:
            return TimePrecision.to_name(int(precision))
        except ValueError:
            return 'N/A'

    def get_direct(self, dataset, variable, include_cols, exclude_cols, limit, main_subjects=[]):
        provider = SQLProvider()
        result = provider.query_variable(dataset, variable)
        if not result:
            content = {
                'Error': f'Could not find dataset {dataset} variable {variable}'
            }
            return content, 404
        admin_level = 1
        qualifiers = provider.query_qualifiers(result['variable_id'], result['property_id'])
        qualifiers = {key: value for key, value in qualifiers.items() if key not in DROP_QUALIFIERS}
        select_cols = self.get_columns(admin_level, include_cols, exclude_cols, qualifiers)
        print(select_cols)

        # Needed for place columns
        if 'main_subject_id' in select_cols:
            temp_cols = select_cols
        else:
            temp_cols = ['main_subject_id'] + select_cols

        results = provider.query_data(result['dataset_id'], result['property_id'], main_subjects, qualifiers, limit, temp_cols)
    
        result_df = pd.DataFrame(results, columns=temp_cols)

        if 'dataset_id' in result_df.columns:
            result_df['dataset_id'] = dataset
        if 'variable_id' in result_df.columns:
            result_df['variable_id'] = variable
        result_df.loc[:, 'variable'] = result['variable_name']
        result_df['time_precision'] = result_df['time_precision'].map(self.fix_time_precision)      
        # result_df.loc[:, 'time_precision'] = self.get_time_precision([10])

        for main_subject_id in result_df.loc[:, 'main_subject_id'].unique():
            place = location.lookup_admin_hierarchy(admin_level, main_subject_id)
            index = result_df.loc[:, 'main_subject_id'] == main_subject_id
            if main_subject_id in labels:
                result_df.loc[index, 'main_subject'] = labels.get(main_subject_id, '')
            for col, val in place.items():
                if col in select_cols:
                    result_df.loc[index, col] = val

        print(result_df.head())
        if 'main_subject_id' not in select_cols:
            result_df = result_df.drop(columns=['main_subject_id'])

        csv = result_df.to_csv(index=False)
        output = make_response(csv)
        output.headers['Content-Disposition'] = f'attachment; filename={variable}.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    def get_columns(self, admin_level, include_cols, exclude_cols, qualifiers) -> typing.List[str]:
        result = []
        for col, status in COMMON_COLUMN.items():
            if status == ColumnStatus.REQUIRED or col in include_cols:
                result.append(col)
                continue
            if col in exclude_cols:
                continue
            if status == ColumnStatus.DEFAULT:
                if col.startswith('admin'):
                    level = int(col[5])
                    if level <= admin_level:
                        result.append(col)
                else:
                    result.append(col)
        for pq_node, col in qualifiers.items():
            if col not in exclude_cols:
                result.append(col)
            col_id = f'{col}_id'
            if col_id in include_cols:
                result.append(col_id)
        return result


class SQLProvider:
    def does_dataset_exists(self, dataset):
        dataset_query = f'''
        SELECT e_dataset.node2 AS dataset_id
        	FROM edges e_dataset
        WHERE e_dataset.label='P1813' AND e_dataset.node2='{dataset}';
        '''
        dataset_dicts = query_to_dicts(dataset_query)
        return len(dataset_dicts) > 0
        
    def query_variable(self, dataset, variable):
        dataset_query = f'''
        SELECT e_dataset.node2 AS dataset_id
        	FROM edges e_dataset
        WHERE e_dataset.label='P1813' AND e_dataset.node2='{dataset}';
        '''
        dataset_dicts = query_to_dicts(dataset_query)
        if not len(dataset_dicts):
            return None

        variable_query = f'''
        SELECT e_var.node2 AS variable_id, s_var_label.text AS variable_name, e_property.node2 AS property_id
        	FROM edges e_var
	        JOIN edges e_var_label ON (e_var.node1=e_var_label.node1 AND e_var_label.label='label')
	        JOIN strings s_var_label ON (e_var_label.id=s_var_label.edge_id)
	        JOIN edges e_property ON (e_property.node1=e_var.node1 AND e_property.label='P1687')
        WHERE e_var.label='P1813' AND e_var.node2='{variable}';
        '''

        variable_dicts = query_to_dicts(variable_query)
        if not len(variable_dicts):
            return None

        return {
            'dataset_id': dataset_dicts[0]['dataset_id'],
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
               q_main.number AS value,
               s_value_unit.text AS value_unit,
               to_json(d_value_date.date_and_time)#>>'{{}}' || 'Z' AS time,
               d_value_date.precision AS time_precision,
        	   'POINT(' || c_coordinate.longitude || ', ' || c_coordinate.latitude || ')' as coordinate,
               e_dataset.node2 AS dataset_id
        FROM edges AS e_main   -- Main edge
            JOIN quantities AS q_main ON (e_main.id=q_main.edge_id)
            JOIN edges AS e_value_unit ON (e_value_unit.node1=q_main.unit AND e_value_unit.label='label')
            JOIN strings AS s_value_unit ON (e_value_unit.id=s_value_unit.edge_id)
            JOIN edges AS e_value_date ON (e_value_date.node1=e_main.id AND e_value_date.label='P585')
            JOIN dates AS d_value_date ON (e_value_date.id=d_value_date.edge_id)
        	JOIN edges AS e_dataset ON (e_dataset.node1=e_main.id AND e_dataset.label='P2006020004')
            LEFT JOIN edges AS e_coordinate
                JOIN coordinates AS c_coordinate ON (e_coordinate.id=c_coordinate.edge_id)
                ON (e_coordinate.node1=e_main.node1 AND e_coordinate.label='P625')

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
