import pandas as pd
from db.sql import dal
from flask import request
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from api.metadata.main import VariableMetadataResource


class IngestT2WMLOutput(object):
    def __init__(self):
        self.vd = VariableDeleter()
        self.vmr = VariableMetadataResource()

    def process(self, dataset, is_request_put=True):

        # check if the dataset exists
        dataset_qnode = dal.get_dataset_id(dataset)

        if not dataset_qnode:
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        file_name = request.files['file'].filename
        if not file_name.endswith('.tsv'):
            return {"error": "Please upload a TSV file (T2WML output)"}, 400

        df = pd.read_csv(request.files['file'], dtype=object, sep='\t').fillna('')

        variable_ids = self.identify_variables(df)

        if is_request_put:
            # delete the variable canonical data and metadata before inserting into databse again!!
            for v in variable_ids:
                print(self.vd.delete(dataset, v))
                print(self.vmr.delete(dataset, v))

        # All good ingest the tsv file into database.
        import_kgtk_dataframe(df, is_file_exploded=True)

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
