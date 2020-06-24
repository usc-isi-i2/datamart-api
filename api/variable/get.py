
import os
from typing import List, Any, Dict

from enum import Enum

from flask import request, make_response, current_app

import pandas as pd
from api.util import TimePrecision

from db.sql.utils import query_to_dicts
from flask.blueprints import Blueprint
from db.sql import dal
from db.sql.dal import Region

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
    'place': ColumnStatus.OPTIONAL,
    'place_id': ColumnStatus.OPTIONAL,
    'coordinate': ColumnStatus.DEFAULT,
    'shape': ColumnStatus.OPTIONAL,
    'stated_in': ColumnStatus.DEFAULT,
    'stated_in_id': ColumnStatus.DEFAULT
}

class GeographyLevel(Enum):
    COUNTRY = 0
    ADMIN1 = 1
    ADMIN2 = 2
    ADMIN3 = 3
    OTHER = 4

class UnknownSubjectError(Exception):
    def __init__(self, unknown_names, unknown_ids):
        super().__init__()
        self.unknown_names = unknown_names
        self.unknown_ids = unknown_ids

    def get_error_dict(self):
        errors: List[str] = []
        if self.unknown_names:
            errors.append('Unknown regions ' + ', '.join(self.unknown_names))
        if self.unknown_ids:
            errors.append('Uknown region ids ' + ', '.join(self.unknown_ids))
        return { 'Error': errors }

class VariableGetter:
    def get(self, dataset, variable):

        include_cols = request.args.getlist('include') or []
        exclude_cols = request.args.getlist('exclude') or []
        main_subjects = []
        limit = -1
        if request.args.get('limit') is not None:
            try:
                limit = int(request.args.get('limit'))
            except:
                pass

        try:
            regions = self.get_subject_regions()
        except UnknownSubjectError as ex:
            return ex.get_error_dict(), 404

        print((dataset, variable, include_cols, exclude_cols, limit, main_subjects))
        return self.get_direct(dataset, variable, include_cols, exclude_cols, limit, regions=regions)

    def get_subject_regions(self) -> Dict[str, Region]:
        main_subjects: List[str] = []
        unknown_names: List[str] = []
        unknown_ids: List[str] = []

        args = { 
            'country': lambda names, ids: dal.query_countries(countries=names, country_ids=ids), 
            'admin1': lambda names, ids: dal.query_admin1s(admin1s=names, admin1_ids=ids), 
            'admin2': lambda names, ids: dal.query_admin2s(admin2s=names, admin2_ids=ids),
            'admin3': lambda names, ids: dal.query_admin3s(admin3s=names, admin3_ids=ids),
        }
        

        all_regions: Dict[str, Region] = {}

        for (arg, query) in args.items():
            arg_id = f'{arg}_id'

            region_names = request.args.getlist(arg)
            region_ids = request.args.getlist(arg_id)
            if not region_names and not region_ids:
                continue


            regions = query(region_names, region_ids)

            # Find unknown regions and ids
            found_names = set([region[arg].lower() for region in regions])
            found_ids = set([region[arg_id] for region in regions])
            for name in region_names:
                if name.lower() not in found_names:
                    unknown_names.append(name)

            for id in region_ids:
                if id not in found_ids:
                    unknown_ids.append(id)

            for region in regions:
                all_regions[region[arg_id]] = region

        if unknown_names or unknown_ids:
            raise UnknownSubjectError(unknown_names, unknown_ids)

        return all_regions
        
    def get_result_regions(self, df) -> Dict[str, Region]:
        # Get all the regions that have rows in the dataframe
        region_ids = list(df['main_subject_id'].unique())
        regions = dal.query_admins(admin_ids=region_ids)
        return { region.admin_id: region for region in regions }

    def fix_time_precision(self, precision):
        try:
            return TimePrecision.to_name(int(precision))
        except ValueError:
            return 'N/A'

    def get_direct(self, dataset, variable, include_cols, exclude_cols, limit, regions: Dict[str, Region]={}):
        result = dal.query_variable(dataset, variable)
        if not result:
            content = {
                'Error': f'Could not find dataset {dataset} variable {variable}'
            }
            return content, 404

        # Output just the country column
        admin_level = 0

        qualifiers = dal.query_qualifiers(result['variable_id'], result['property_id'])
        qualifiers = {key: value for key, value in qualifiers.items() if key not in DROP_QUALIFIERS}
        select_cols = self.get_columns(admin_level, include_cols, exclude_cols, qualifiers)
        print(select_cols)

        # Needed for place columns
        if 'main_subject_id' in select_cols:
            temp_cols = select_cols
        else:
            temp_cols = ['main_subject_id'] + select_cols

        main_subjects = regions.keys()
        results = dal.query_variable_data(result['dataset_id'], result['property_id'], main_subjects, qualifiers, limit, temp_cols)

        result_df = pd.DataFrame(results, columns=temp_cols)

        if 'dataset_id' in result_df.columns:
            result_df['dataset_id'] = dataset
        if 'variable_id' in result_df.columns:
            result_df['variable_id'] = variable
        result_df.loc[:, 'variable'] = result['variable_name']
        result_df['time_precision'] = result_df['time_precision'].map(self.fix_time_precision)

        self.add_region_columns(result_df, select_cols, regions)
        print(result_df.head())
        if 'main_subject_id' not in select_cols:
            result_df = result_df.drop(columns=['main_subject_id'])

        csv = result_df.to_csv(index=False)
        output = make_response(csv)
        output.headers['Content-Disposition'] = f'attachment; filename={variable}.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    def get_columns(self, admin_level, include_cols, exclude_cols, qualifiers) -> List[str]:
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

    def add_region_columns(self, df, select_cols: List[str], regions: Dict[str, Region]):
        if not regions:
            regions = self.get_result_regions(df)

        region_columns = ['country', 'country_id', 'admin1', 'admin1_id', 'admin2', 'admin2_id', 'admin3', 'admin3_id']
        for col in region_columns:
            if col in select_cols:
                df[col] = df['main_subject_id'].map(lambda msid: regions[msid][col])
