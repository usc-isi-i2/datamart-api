import json
import sys
import pandas as pd
from db.sql import dal
from flask import request
import tempfile
import tarfile
from flask import send_from_directory
from annotation.main import T2WMLAnnotation
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from api.metadata.main import DatasetMetadataResource, VariableMetadataResource
from api.metadata.metadata import DatasetMetadata
from api.metadata.update import DatasetMetadataUpdater
from annotation.validation.validate_annotation import ValidateAnnotation
from time import time
import traceback

class TsvData(object):
    def __init__(self):
        self.vmr = VariableMetadataResource()
        self.vd = VariableDeleter()
        self.dmr = DatasetMetadataResource()

    def process(self, dataset, is_request_put=False):
        l = time()

        # check if the dataset exists
        s = time()
        dataset_qnode = dal.get_dataset_id(dataset)
        print(f'time take to get dataset: {time() - s} seconds')

        if not create_if_not_exist and not dataset_qnode:
            print(f'Dataset not defined: {dataset}')
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        file_name = request.files['file'].filename

        if not file_name.endswith('.tsv'):
            return {"Error": "Please upload an kgtk file "
                             "(file name ending with .tsv)"}, 400

        # Parse the dataframe from request
        df = pd.read_csv(request.files['file'], dtype=object, delimiter='\t').fillna('')
        variable_ids = df.query(f"label == 'P1813' & node2 != '{dataset}'")["node2"].tolist()
        variable_ids = [v.replace('"', '') for v in variable_ids]

        if is_request_put:
            # delete the variable canonical data and metadata before inserting into databse again!!
            for v in variable_ids:
                print(self.vd.delete(dataset, v))
                print(self.vmr.delete(dataset, v))
            # Also delete the metadata for this dataset
            print(self.dmr.delete(dataset))

        # import to database
        s = time()
        print('number of rows to be imported: {}'.format(len(df)))
        try:
            import_kgtk_dataframe(df, is_file_exploded=True)
        except Exception as e:
            # Not sure what's going on here, so print for debugging purposes
            print("Can't import exploded kgtk file")
            traceback.print_exc(file=sys.stdout)
            raise e
        print(f'time take to import kgtk file into database: {time() - s} seconds')

        variables_metadata = []
        for v in variable_ids:
            variables_metadata.append(self.vmr.get(dataset, variable=v)[0])
        print(f'total time taken: {time() - l}')

        return variables_metadata, 201
