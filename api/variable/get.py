import pandas as pd
from enum import Enum
from db.sql import dal
from db.sql.dal import Region
from typing import List, Dict, Set
from api.util import TimePrecision
from flask import request, make_response
from api.region_cache import region_cache

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
    def __init__(self, *errors):
        super().__init__()
        self.errors = errors

    def get_error_dict(self):
        return {'Error': self.errors}


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
            regions = self.get_query_region_ids()
        except UnknownSubjectError as ex:
            return ex.get_error_dict(), 404

        # print((dataset, variable, include_cols, exclude_cols, limit, regions))
        return self.get_direct(dataset, variable, include_cols, exclude_cols, limit, regions)

    def get_query_region_ids(self) -> Dict[str, List[str]]:
        # Returns all ids pf regions specifed in the URL in a dictionary based on region_type:
        # { country_id: [all countries in the query],
        #   admin1_id: [all admin1s in the query],
        #   admin2_id: [all admin2s in the query],
        #   admin3_id: [all admin3s in the query] }
        # Raises an exception if non-existing regions are specified (by name or by ID)

        # Consolidate all names and ids into two lists
        args = {
            'country': Region.COUNTRY,
            'admin1': Region.ADMIN1,
            'admin2': Region.ADMIN2,
            'admin3': Region.ADMIN3,
        }
        arg_names = []
        arg_ids = []
        for arg in args.keys():
            arg_id = f'{arg}_id'
            arg_names += request.args.getlist(arg)
            arg_ids += request.args.getlist(arg_id)

        # Query those regions
        found_regions_by_id = region_cache.get_regions(region_names=arg_names, region_ids=arg_ids)
        found_regions_by_name: Dict[
            str, Set[Region]] = {}  # Organize by name for easy lookup, there can be numerous regions per name
        for region in found_regions_by_id.values():
            name = region.admin.lower()
            if not name in found_regions_by_name:
                found_regions_by_name[name] = {region}
            else:
                found_regions_by_name[name].add(region)

        # Now go over the queried regions and make sure we have everything we asked for
        errors = []
        result_regions: Dict[str, List[str]] = {}
        for arg, arg_type in args.items():
            result_regions[arg] = []

            arg_names = [name for name in request.args.getlist(arg)]
            for name in arg_names:
                found = False
                for candidate in found_regions_by_name.get(name.lower(), set()):
                    if candidate.region_type == arg_type:
                        result_regions[arg].append(candidate.admin_id)
                        found = True
                if not found:
                    errors.append(f'No {arg} {name}')

            arg_id = f'{arg}_id'
            arg_ids = request.args.getlist(arg_id) or []
            for arg_id in arg_ids:
                c = found_regions_by_id.get(arg_id)
                if c and c.region_type == arg_type:
                    result_regions[arg].append(c.admin_id)
                else:
                    errors.append(f'No {arg}_id {arg_id}')

        if errors:
            raise UnknownSubjectError(errors)

        return result_regions

    def get_result_regions(self, df_location) -> Dict[str, Region]:
        # Get all the regions that have rows in the dataframe
        region_ids = [id for id in df_location.unique() if id is not None]
        regions = region_cache.get_regions(region_ids=region_ids)
        return regions

    def fix_time_precision(self, precision):
        try:
            return TimePrecision.to_name(int(precision))
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
            content = {
                'Error': f'Variable {variable} in dataset {dataset} does not have a time qualifier associated with it'
            }
            return content, 422
            
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

        result_df = pd.DataFrame(results, columns=temp_cols)

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
                          'coordinate']
        for col in region_columns:
            if col in select_cols:
                df[col] = location_df.map(lambda msid: regions[msid][col] if msid in regions else 'N/A')
