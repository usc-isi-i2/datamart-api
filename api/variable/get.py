
import os
import typing

from enum import Enum

from flask import request, make_response, current_app

import pandas as pd
from api.util import TimePrecision

from db.sql.utils import query_to_dicts
from flask.blueprints import Blueprint
from api.SQLProvider import SQLProvider

DROP_QUALIFIERS = [
    'pq:P585', 'P585' # time
    'pq:P1640',  # curator
    'pq:Pdataset', 'P2006020004' # dataset
]

class ColumnStatus(Enum):
    REQUIRED = 0
    DEFAULT = 1
    OPTIONAL = 2

COMMON_COLUMN = {
    'dataset_short_name': ColumnStatus.DEFAULT,
    'variable_short_name': ColumnStatus.DEFAULT,
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

class VariableGetter:
    def get(self, dataset, variable):

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

        provider = SQLProvider()

        # Add main subject by exact English label
        # For now assume only country:
        keyword = 'country'
        if request.args.get(keyword) is not None:
            admins = [x.lower() for x in request.args.get(keyword).split(',')]
            admin_dict = provider.query_country_qnodes(admins)
            # Find and report unknown countries
            unknown = [country for country, qnode in admin_dict.items() if qnode is None]
            if unknown:
                return { 'Error': 'Unknown countries: ' + ', '. join(unknown) }, 404
            qnodes = [qnode for qnode in admin_dict.values() if qnode]
            main_subjects += qnodes

        # Add main subject by qnode
        for keyword in ['main_subject_id', 'country_id', 'admin1_id', 'admin2_id', 'admin3_id']:
            if request.args.get(keyword) is not None:
                qnodes = request.args.get(keyword).split(',')
                print(f'Add {keyword}:', qnodes)
                main_subjects += qnodes

        # Not needed for Causex release
        # # Add administrative locations using the name of parent administrative location
        # for keyword, admin_col, lower_admin_col in zip(
        #         ['in_country', 'in_admin1', 'in_admin2'],
        #         ['country', 'admin1', 'admin2'],
        #         ['admin1_id', 'admin2_id', 'admin3_id']):
        #     if request.args.get(keyword) is not None:
        #         admins = [x.lower() for x in request.args.get(keyword).split(',')]
        #         index = region_df.loc[:, admin_col].isin(admins)
        #         print(f'Add {keyword}({request.args.get(keyword)}):',
        #               region_df.loc[index, lower_admin_col].unique())
        #         main_subjects += qnodes

        # # Add administrative locations using the qnode of parent administrative location
        # for keyword, admin_col, lower_admin_col in zip(
        #         ['in_country_id', 'in_admin1_id', 'in_admin2_id'],
        #         ['country_id', 'admin1_id', 'admin2_id'],
        #         ['admin1_id', 'admin2_id', 'admin3_id']):
        #     if request.args.get(keyword) is not None:
        #         admin_ids = request.args.get(keyword).split(',')
        #         index = region_df.loc[:, admin_col].isin(admin_ids)
        #         print(f'Add {keyword}({request.args.get(keyword)}):',
        #               region_df.loc[index, lower_admin_col].unique())
        #         main_subjects += [x for x in region_df.loc[index, lower_admin_col].unique()]

        print((dataset, variable, include_cols, exclude_cols, limit, main_subjects))
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

        if 'dataset_short_name' in result_df.columns:
            result_df['dataset_short_name'] = dataset
        if 'variable_short_name' in result_df.columns:
            result_df['variable_short_name'] = variable
        result_df.loc[:, 'variable'] = result['variable_name']
        result_df['time_precision'] = result_df['time_precision'].map(self.fix_time_precision)
        # result_df.loc[:, 'time_precision'] = self.get_time_precision([10])


        # Ke-Thia - this seems unnecessary. The query already returns main_subject, main_subject_id, country, country_id
        # The query_country_qnodes function converts country names to qnodes, and should be used when filtering by countries
        # based on the URL
        # main_subject_ids = result_df.loc[:, 'main_subject_id'].unique()
        # countries = provider.query_country_qnodes(main_subject_ids)
        # for main_subject_id in result_df.loc[:, 'main_subject_id'].unique():
        #     # For now, assume main subject is always country
        #     # place = location.lookup_admin_hierarchy(admin_level, main_subject_id)
        #     place = {}
        #     if main_subject_id in countries:
        #         place['country'] = countries[main_subject_id]

        #     index = result_df.loc[:, 'main_subject_id'] == main_subject_id
        #     result_df.loc[index, 'main_subject'] = provider.get_label(main_subject_id, '')
        #     for col, val in place.items():
        #         if col in select_cols:
        #             result_df.loc[index, col] = val

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
