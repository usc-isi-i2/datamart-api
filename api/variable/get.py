import pandas as pd
from enum import Enum
from db.sql import dal
from db.sql.dal import Region
from typing import List, Dict, Set
from api.util import TimePrecision
from flask import request, make_response
from api.region_utils import get_query_region_ids, region_cache, UnknownSubjectError

DROP_QUALIFIERS = [
    'pq:P585', 'P585'  # time
               'pq:P1640',  # curator
    'pq:Pdataset', 'P2006020004'  # dataset
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
    'region_coordinate': ColumnStatus.DEFAULT,
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
            regions = get_query_region_ids(request.args)
        except UnknownSubjectError as ex:
            return ex.get_error_dict(), 404

        # print((dataset, variable, include_cols, exclude_cols, limit, regions))
        return self.get_direct(dataset, variable, include_cols, exclude_cols, limit, regions)

    def get_result_regions(self, df_location) -> Dict[str, Region]:
        # Get all the regions that have rows in the dataframe
        region_ids = [id for id in df_location.unique() if id is not None]
        regions = region_cache.get_regions(region_ids=region_ids)
        return regions

    def fix_time_precision(self, precision):
        try:
            precision_number = int(float(precision))  # precision is a string that can be '11.0'
            return TimePrecision.to_name(precision_number)
        except ValueError:
            return 'N/A'

    def get_direct(self, dataset, variable, include_cols, exclude_cols, limit, regions: Dict[str, List[str]] = {},
                   return_df=False):
        result = dal.query_variable(dataset, variable)
        if not result:
            content = {
                'Error': f'Could not find dataset {dataset} variable {variable}'
            }
            return content, 404

        qualifiers = dal.query_qualifiers(result['dataset_id'], result['variable_qnode'])
        qualifier_names = set([q.name for q in qualifiers])
        if 'time' not in qualifier_names:
            if return_df:
                return None
            return '', 204

        location_qualifier = 'location' in [q.name for q in qualifiers]
        # qualifiers = {key: value for key, value in qualifiers.items() if key not in DROP_QUALIFIERS}
        select_cols = self.get_columns(include_cols, exclude_cols, qualifiers)

        # Needed for place columns
        if 'main_subject_id' in select_cols:
            temp_cols = select_cols
        else:
            temp_cols = ['main_subject_id'] + select_cols

        if location_qualifier and 'location_id' not in temp_cols:
            temp_cols = ['location_id'] + temp_cols

        results = dal.query_variable_data(result['dataset_id'], result['property_id'], regions, qualifiers, limit,
                                          temp_cols)

        result_df = pd.DataFrame(results, columns=temp_cols).fillna('')

        if 'dataset_id' in result_df.columns:
            result_df['dataset_id'] = dataset
        if 'variable_id' in result_df.columns:
            result_df['variable_id'] = variable
        result_df.loc[:, 'variable'] = result['variable_name']
        result_df['time_precision'] = result_df['time_precision'].map(self.fix_time_precision)

        self.add_region_columns(result_df, select_cols)

        if 'main_subject_id' not in select_cols:
            result_df = result_df.drop(columns=['main_subject_id'])

        if return_df:
            return result_df

        result_df.replace('N/A', '', inplace=True)
        csv = result_df.to_csv(index=False)
        output = make_response(csv)
        output.headers['Content-Disposition'] = f'attachment; filename={variable}.csv'
        output.headers['Content-type'] = 'text/csv'
        return output

    def get_columns(self, include_cols, exclude_cols, qualifiers) -> List[str]:
        result = []
        for col, status in COMMON_COLUMN.items():
            if status == ColumnStatus.REQUIRED or col in include_cols:
                result.append(col)
                continue
            if col in exclude_cols:
                continue
            if status == ColumnStatus.DEFAULT:
                result.append(col)
        # Ignore qualifier fields for now
        # for pq_node, col in qualifiers.items():
        #    if col not in exclude_cols:
        #        result.append(col)
        #    col_id = f'{col}_id'
        #    if col_id in include_cols:
        #        result.append(col_id)

        # Now go over the qualifiers, and add the main column of each qualifier by default
        for qualifier in qualifiers:
            for field in qualifier.fields.keys():
                if (field == qualifier.main_column and field not in exclude_cols) or field in include_cols:
                    if field not in result:
                        result.append(field)

        return result

    def add_region_columns(self, df, select_cols: List[str]):
        if 'location_id' in df:
            location_df = df['location_id']
            location_in_qualifier = True
        else:
            location_df = df['main_subject_id']
            location_in_qualifier = False

        regions = self.get_result_regions(location_df)

        # if not location_in_qualifier:
        #    df['main_subject'] = location_df.map(lambda msid: regions[msid].admin if msid in regions else 'N/A')

        # Add the other columns
        region_columns = ['country', 'country_id', 'admin1', 'admin1_id', 'admin2', 'admin2_id', 'admin3', 'admin3_id',
                          'region_coordinate']
        for col in region_columns:
            if col in select_cols:
                df[col] = location_df.map(lambda msid: regions[msid][col] if msid in regions else 'N/A')
