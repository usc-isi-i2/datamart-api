import re
import json
import time
import hashlib
import pandas as pd
from db.sql import dal
from flask import request
from api.util import TimePrecision
from db.sql.utils import query_to_dicts
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from .country_wikifier import DatamartCountryWikifier


class CanonicalData(object):
    def __init__(self):
        self.qnode_regex = {}
        self.all_ids_dict = {}
        self.tp = TimePrecision()
        self.required_fields = [
            'main_subject',
            'value',
            'time',
            'time_precision',
            'country'
        ]
        self.vd = VariableDeleter()

    @staticmethod
    def format_sql_string(values, value_type='str'):
        str = ''
        if value_type == 'str':
            for value in values:
                str += '\'{}\','.format(value)
        elif value_type == 'int':
            for value in values:
                str += '{},'.format(value)
        return str[:-1] if str else None

    def create_new_qnodes(self, dataset_id, values, q_type):
        # q_type can be 'Unit' or 'Source' for now
        # create qnodes for units with this scheme {dataset_id}Unit-{d}
        if q_type == 'Unit':
            _query = "SELECT max(e.node1) as qnode_max FROM edges e WHERE e.label = 'P31' and e.node2 = 'Q47574' and" \
                     " e.node1 like '{}{}-%'".format(dataset_id, q_type)
        else:
            _query = "SELECT max(e.node1) as qnode_max FROM edges e WHERE e.node1 like '{}{}-%'".format(dataset_id,
                                                                                                        q_type)

        _result = query_to_dicts(_query)[0]
        qnode_max = _result['qnode_max']

        regex_key = '({}{}-)(\d*)'.format(dataset_id, q_type)
        if regex_key not in self.qnode_regex:
            self.qnode_regex[regex_key] = re.compile(regex_key)
        _regex = self.qnode_regex[regex_key]
        if not qnode_max:
            seed = 0
        else:
            u, v = _regex.match(qnode_max).groups()
            seed = int(v) + 1
        _dict = {}
        for value in values:
            _dict[value] = '{}{}-{}'.format(dataset_id, q_type, seed)
            seed += 1
        return _dict

    def create_triple(self, node1, label, node2):
        id_key = '{}-{}'.format(node1, label)
        if id_key not in self.all_ids_dict:
            self.all_ids_dict[id_key] = 0
        else:
            self.all_ids_dict[id_key] += 1
        id_index = self.all_ids_dict[id_key]
        return {
            'node1': node1,
            'label': label,
            'node2': node2,
            'id': 'Q{}'.format(
                hashlib.sha256(bytes('{}{}{}{}'.format(node1, label, node2, id_index), encoding='utf-8')).hexdigest())
        }

    def create_kgtk_measurements(self, row, dataset_id, variable_id):
        kgtk_measurement_temp = list()
        main_subject = row['main_subject_id'].strip()
        if main_subject:

            if 'value_unit_id' in row:
                main_triple = self.create_triple(main_subject, variable_id,
                                                 '{}{}'.format(row['value'], row['value_unit_id']))
            else:
                main_triple = self.create_triple(main_subject, variable_id, '{}'.format(row['value']))
            kgtk_measurement_temp.append(main_triple)

            main_triple_id = main_triple['id']
            kgtk_measurement_temp.append(self.create_triple(main_triple_id, 'P2006020004', dataset_id))

            kgtk_measurement_temp.append(
                self.create_triple(main_triple_id, 'P585',
                                   '{}/{}'.format('{}{}'.format('^', row['time']),
                                                  self.tp.to_int(row['time_precision'].lower()))))
            if 'country_id' in row:
                country_id = row['country_id'].strip()
                if country_id:
                    kgtk_measurement_temp.append(self.create_triple(main_triple_id, 'P17', country_id))

            if 'source_id' in row:
                source_id = row['source_id'].strip()
                if source_id:
                    kgtk_measurement_temp.append(self.create_triple(main_triple_id, 'P248', source_id))

        return kgtk_measurement_temp

    def validate_headers(self, df):
        d_columns = list(df.columns)
        validator_log = []
        valid_file = True
        for c in self.required_fields:
            if c not in d_columns:
                validator_log.append(self.error_row(f'Missing required column: \'{c}\'', 1, c,
                                                    f'The uploaded file is missing a required column: {c}. '
                                                    f'Please add the missing column and upload again.'))
                valid_file = False
        return validator_log, valid_file

    def validate_input_file(self, df, dataset_id, variable_id):

        validator_log = []
        valid_file = True

        d_columns = list(df.columns)

        for i, row in df.iterrows():
            valid_value = self.validate_number(row['value'])
            if not valid_value:
                validator_log.append(self.error_row(f"Value Error: \'{row['value']}\'",
                                                    i + 2,
                                                    'value',
                                                    f"\'{row['value']}\' is not a valid number"))
                valid_file = False

            try:
                precision = self.tp.to_int(row['time_precision'])
            except ValueError:
                validator_log.append(
                    self.error_row(f"Illegal precision value: \'{row['time_precision']}\'", i + 2, 'time_precision',
                                   f"Legal precision values are: \'{','.join(list(self.tp.name_int_map))}\'"
                                   ))
                valid_file = False

            if row['main_subject_id'] is None:
                validator_log.append(
                    self.error_row(f"Could not wikify: \'{row['main_subject']}\'", i + 2, 'main_subject',
                                   f"Could not find a Wikidata Qnode for the main subject: "
                                   f"\'{row['main_subject']}.\' "
                                   f"Please check for spelling mistakes in the country name."))
                valid_file = False

            if row['country_id'] is None:
                validator_log.append(self.error_row(f"Could not wikify: \'{row['country']}\'", i + 2, 'country',
                                                    f"Could not find a Wikidata Qnode for the country:  "
                                                    f"\'{row['country']}\'. "
                                                    f"Please check for spelling mistakes in the country name."))
                valid_file = False

            if 'dataset_id' in d_columns:
                if row['dataset_id'].strip() != dataset_id:
                    validator_log.append(self.error_row(f"Dataset ID in the file: \'{row['dataset_id']}\' "
                                                        f"is not same as Dataset ID in the url : \'{dataset_id}\'",
                                                        i + 2, 'dataset_id',
                                                        "Dataset IDs in the input file should match the "
                                                        "Dataset Id in the API url"))
                    valid_file = False

            if 'variable_id' in d_columns:
                if row['variable_id'].strip() != variable_id:
                    validator_log.append(self.error_row(f"Variable ID in the file: \'{row['variable_id']}\' "
                                                        f"is not same as Variable ID in the url : \'{variable_id}\'",
                                                        i + 2, 'variable_id',
                                                        "Variable IDs in the input file should match the "
                                                        "Variable Id in the API url"))
                    valid_file = False

            valid_time = self.validate_time(row['time'])
            if not valid_time:
                validator_log.append(self.error_row(f"Invalid datetime format: \'{row['time']}\'", i + 2, 'time',
                                                    f"Invalid format to specify time. Valid format: \'%Y-%m-%dT%H:%M:%SZ\'"
                                                    f" "
                                                    f"Explanation: "
                                                    f" "
                                                    f"%Y - Year with century as a decimal number (2010, 2020 etc)."
                                                    f" "
                                                    f"%m - Month as a zero-padded decimal number(01, 02,..,12)."
                                                    f" "
                                                    f"%d - Day of the month as a zero-padded decimal number. (01,02,..,31)."
                                                    f" "
                                                    f"%H - Hour (24-hour clock) as a zero-padded decimal number. (00, 01,..,23)."
                                                    f" "
                                                    f"%M - Minute as a zero-padded decimal number.(00, 01,...,59)."
                                                    f" "
                                                    f"%S - Second as a zero-padded decimal number.(00, 01,...,59)."
                                                    f" "
                                                    f"A valid date: \'2020-02-27T13:45:44Z\'"))
                valid_file = False
        return validator_log, valid_file

    @staticmethod
    def validate_time(time_str):
        # 2021-01-01T00:00:00Z
        try:
            time.strptime(time_str, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            return False
        return True

    @staticmethod
    def validate_number(num):
        try:
            float(num)
        except ValueError:
            return False
        return True

    @staticmethod
    def error_row(error, row, column, description):
        return {
            'Error': error,
            'Line Number': row,
            'Column': column,
            'Description': description
        }

    def canonical_data(self, dataset, variable, is_request_put=True):
        wikify = request.args.get('wikify', 'false').lower() == 'true'
        print(wikify)
        # check if the dataset exists
        dataset_id = dal.get_dataset_id(dataset)

        if not dataset_id:
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        # check if variable exists for the dataset
        # P2006020003 - Variable Measured
        # P1687 - Corresponds to property

        variable_query = f"""Select e.node1, e.node2 from edges e where e.node1 in (
                                select e_variable.node1 from edges e_variable
                                        where e_variable.node1 in
                                    (
                                        select e_dataset.node2 from edges e_dataset
                                        where e_dataset.node1 = '{dataset_id}'
                                        and e_dataset.label = 'P2006020003'
                                    )
                                    and e_variable.label = 'P1813' and e_variable.node2 = '{variable}'
                                )
                                and e.label = 'P1687'
                                    """
        print(variable_query)
        variable_result = query_to_dicts(variable_query)
        if len(variable_result) == 0:
            return {'error': 'Variable: {} not found for the dataset: {}'.format(variable, dataset)}, 404

        variable_pnode = variable_result[0]['node2']

        kgtk_format_list = list()

        # dataset and variable has been found, wikify and upload the data
        df = pd.read_csv(request.files['file'], dtype=object)
        column_map = {}
        _d_columns = list(df.columns)
        for c in _d_columns:
            column_map[c] = c.lower().strip()

        df = df.rename(columns=column_map)

        d_columns = list(df.columns)

        # validate file headers first
        validator_header_log, valid_file = self.validate_headers(df)
        if not valid_file:
            return validator_header_log, 400

        country_wikifier = DatamartCountryWikifier()

        if 'main_subject_id' not in d_columns or wikify:
            main_subjects = list(df['main_subject'].unique())
            main_subjects_wikified = country_wikifier.wikify(main_subjects)
            df['main_subject_id'] = df['main_subject'].map(lambda x: main_subjects_wikified[x])

        if 'country_id' not in d_columns or wikify:
            countries = list(df['country'].unique())
            countries_wikified = country_wikifier.wikify(countries)
            df['country_id'] = df['country'].map(lambda x: countries_wikified[x])

        # validate file contents
        validator_file_log, valid_file = self.validate_input_file(df, dataset, variable)
        if not valid_file:
            return validator_file_log, 400

        if 'value_unit' in d_columns and ('value_unit_id' not in d_columns or wikify):
            units = list(df['value_unit'].unique())

            units_query = "SELECT e.node1, e.node2 FROM edges e WHERE e.node2 in ({}) and e.label = 'label'".format(
                self.format_sql_string(units))

            units_results = query_to_dicts(units_query)

            unit_qnode_dict = {}

            for ur in units_results:
                unit_qnode_dict[ur['node2']] = ur['node1']

            no_qnode_units = list()
            no_qnode_units.extend([u for u in units if u not in unit_qnode_dict])

            no_unit_qnode_dict = self.create_new_qnodes(dataset_id, no_qnode_units, 'Unit')

            df['value_unit_id'] = df['value_unit'].map(
                lambda x: unit_qnode_dict[x] if x in unit_qnode_dict else no_unit_qnode_dict[x])

            # create rows for new created variables Unit Qnodes and Source Qnodes
            for k in no_unit_qnode_dict:
                _q = no_unit_qnode_dict[k]
                kgtk_format_list.append(self.create_triple(_q, 'label', json.dumps(k)))
                kgtk_format_list.append(self.create_triple(_q, 'P31', 'Q47574'))  # unit of measurement

        if 'source' in d_columns and ('source_id' not in d_columns or wikify):
            sources = list(df['source'].unique())

            sources_query = "SELECT  e.node1, e.node2 FROM edges e WHERE e.label = 'label' and e.node2 in  ({})".format(
                self.format_sql_string(sources))

            sources_results = query_to_dicts(sources_query)

            source_qnode_dict = {}
            for sr in sources_results:
                source_qnode_dict[sr['node2']] = sr['node1']

            no_qnode_sources = list()
            no_qnode_sources.extend([s for s in sources if s not in source_qnode_dict])

            no_source_qnode_dict = self.create_new_qnodes(dataset_id, no_qnode_sources, 'Source')

            df['source_id'] = df['source'].map(
                lambda x: source_qnode_dict[x] if x in source_qnode_dict else no_source_qnode_dict[x])
            for k in no_source_qnode_dict:
                kgtk_format_list.append(self.create_triple(no_source_qnode_dict[k], 'label', json.dumps(k)))

        for i, row in df.iterrows():
            kgtk_format_list.extend(self.create_kgtk_measurements(row, dataset_id, variable_pnode))

        if is_request_put:
            # this is a PUT request, delete all data for this variable and upload the current data
            self.vd.delete(dataset, variable)

        df_kgtk = pd.DataFrame(kgtk_format_list)
        import_kgtk_dataframe(df_kgtk)

        return '{} rows imported!'.format(len(df)), 201  # original file
