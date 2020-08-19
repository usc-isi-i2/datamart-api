import pandas as pd
from db.sql import dal
from flask import request
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from api.metadata.main import VariableMetadataResource
import csv
import tempfile
import subprocess
import os


class IngestT2WMLOutput(object):
    def __init__(self):
        self.vd = VariableDeleter()
        self.vmr = VariableMetadataResource()

    def process(self, dataset, is_request_put=True):

        # check if the dataset exists
        dataset_qnode = dal.get_dataset_id(dataset)

        if not dataset_qnode:
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        t2wml_file_name = request.files['kgtk_output'].filename
        item_defs_file_name = request.files['item_definitions'].filename
        if not t2wml_file_name.endswith('.tsv'):
            return {"error": "Please upload a TSV file (T2WML output)"}, 400

        if not item_defs_file_name.endswith('.tsv'):
            return {"error": "Please upload a TSV file (T2WML output)"}, 400

        t2wml_output_df = pd.read_csv(request.files['kgtk_output'], dtype=object, sep='\t',
                                      quoting=csv.QUOTE_NONE).fillna('')
        item_defs_df = pd.read_csv(request.files['item_definitions'], dtype=object, sep='\t',
                                   quoting=csv.QUOTE_NONE).fillna('')

        df = self.convert_t2wml_files(t2wml_output_df, item_defs_df)
        variable_ids = self.identify_variables(df)

        if is_request_put:
            # delete the variable canonical data and metadata before inserting into databse again!!
            for v in variable_ids:
                print(self.vd.delete(dataset, v))
                print(self.vmr.delete(dataset, v))

        # All good ingest the tsv file into database.
        import_kgtk_dataframe(df, implode_file_first=True)

        variables_metadata = []
        for v in variable_ids:
            variables_metadata.append(self.vmr.get(dataset, variable=v)[0])

        return variables_metadata, 201

    def identify_variables(self, df):
        variables = {}
        for i, row in df.iterrows():
            if row['label'].strip() == 'P1813':
                if row['node1'] not in variables:
                    variables[row['node1']] = {}
                variables[row['node1']]['id'] = row['node2']

            if row['label'] == 'P31' and row['node2'] == 'Q50701':
                if row['node1'] not in variables:
                    variables[row['node1']] = {}
                variables[row['node1']]['is_variable'] = True
        variable_ids = [variables[x]['id'] for x in variables if
                        variables[x].get('is_variable', False) and 'id' in variables[x]]
        return variable_ids

    def convert_t2wml_files(self, t2wml_output_df, item_defs_df):
        temp_dir = tempfile.mkdtemp()
        tsv_path = os.path.join(temp_dir, f'kgtk.tsv')
        exploded_tsv_path = os.path.join(temp_dir, f'kgtk-exploded.tsv')
        imploded_tsv_path = os.path.join(temp_dir, f'kgtk-imploded.tsv')
        exploded_tsv_path_with_ids = os.path.join(temp_dir, f'kgtk-exploded-ids.tsv')

        subprocess.run(
            ['kgtk', 'implode', tsv_path, '-o', imploded_tsv_path, '--allow-lax-qnodes', 'true', '--without',
             'si_units', 'language_suffix'])
        if not os.path.isfile(imploded_tsv_path):
            raise ValueError("Couldn't create imploded TSV file")

        subprocess.run(['kgtk', 'explode', imploded_tsv_path, '-o', exploded_tsv_path, '--allow-lax-qnodes', 'true',
                            '--overwrite', 'true'])
        if not os.path.isfile(exploded_tsv_path):
                raise ValueError("Couldn't create exploded TSV file")

        subprocess.run(['kgtk', 'add-id', '-i', exploded_tsv_path, '-o', exploded_tsv_path_with_ids,
                            '--overwrite-id', 'true', '--id-style', 'node1-label-node2-num'])
        print(tsv_path)
        print(imploded_tsv_path)
        print(exploded_tsv_path)
        print(exploded_tsv_path_with_ids)

        return None
