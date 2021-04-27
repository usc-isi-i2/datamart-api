import json
import sys, os
import pandas as pd
from db.sql import dal
from flask import request
import tempfile
import shutil
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

        temp_tar_dir = tempfile.mkdtemp()

        if 'file' in request.files:

            file_name = request.files['file'].filename
            if not file_name.endswith('.tsv'):
                return {"Error": "Please upload an kgtk file "
                                 "(file name ending with .tsv)"}, 400

            with open(f"{temp_tar_dir}/{file_name}", 'wb') as fd:
                fd.write(request.files['file'].read())
            files = [file_name]

        elif 'tar' in request.files:

            if not request.files['tar'].filename.endswith('tar.gz'):
                return {"Error": "Please upload a compressed file"}, 400
            # Copy
            with open(f"{temp_tar_dir}/{request.files['tar'].filename}", 'wb') as fd:
                fd.write(request.files['tar'].read())
            # Extract
            with tarfile.open(f"{temp_tar_dir}/{request.files['tar'].filename}", 'r') as tar:
                tar.extractall(temp_tar_dir)
            files = [f for f in os.listdir(temp_tar_dir) if f.endswith('.tsv')]

        else:
            return {"Error" : "No file is found"}, 400

        # Parse the dataframe from request
        # df = pd.read_csv(request.files['file'], dtype=object, delimiter='\t').fillna('')
        # variable_ids = df.query(f"label == 'P1813' & node2 != '{dataset}'")["node2"].tolist()
        # variable_ids = [v.replace('"', '') for v in variable_ids]

        if is_request_put:

            variable_ids = []
            var_metadata, code = self.vmr.get(dataset)
            if code == 200:
                # get the variable names
                for v in var_metadata:
                    print(v)
                    variable_ids.append(v['variable_id'])

                # delete the variable canonical data and metadata before inserting into databse again!!
                for v in variable_ids:
                    print(self.vd.delete(dataset, v))
                    print(self.vmr.delete(dataset, v))
                # Also delete the metadata for this dataset
                print(self.dmr.delete(dataset))

        # import to database
        s = time()
        for i, fname in enumerate(files):

            print(f'importing file #{i+1}: {fname}')
            df = pd.read_csv(f'{temp_tar_dir}/{fname}', dtype=object, delimiter='\t').fillna('')
            print('number of rows to be imported: {}'.format(len(df)))

            try:
                import_kgtk_dataframe(df, is_file_exploded=True)
            except Exception as e:
                # Not sure what's going on here, so print for debugging purposes
                print("Can't import exploded kgtk file")
                traceback.print_exc(file=sys.stdout)
                raise e
        print('All files have been imported')
        print(f'time take to import kgtk file into database: {time() - s} seconds')

        # Clean up
        shutil.rmtree(temp_tar_dir)

        variables_metadata = self.vmr.get(dataset)[0]

        return variables_metadata, 201
