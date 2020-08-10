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

        # TODO delete variable metadata and data here, first need to identify variables though
        # CODE GOES Here
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
        return []
