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

class DatasetData(object):
    def __init__(self):
        self.dmr = DatasetMetadataResource()

    def process(self, dataset, is_request_put=False):
        l = time()

        # check if the dataset exists
        s = time()
        dataset_qnode = dal.get_dataset_id(dataset)
        if not dataset_qnode:
            return {"Error": f"Dataset {dataset} is not defined."}, 400

        print(f'time take to get dataset: {time() - s} seconds')

        temp_tar_dir = tempfile.mkdtemp()

        # Import data
        if 'file' in request.files:
            # Submit a single tsv file
            file_name = request.files['file'].filename
            if not file_name.endswith('.tsv'):
                return {"Error": "Please upload an kgtk file "
                                 "(file name ending with .tsv)"}, 400

            with open(f"{temp_tar_dir}/{file_name}", 'wb') as fd:
                fd.write(request.files['file'].read())
            files = [file_name]

        elif 'tar' in request.files:

            # Submit a single tar file
            if not request.files['tar'].filename.endswith('tar.gz'):
                return {"Error": "Please upload a compressed file"}, 400

            # Copy
            with open(f"{temp_tar_dir}/{request.files['tar'].filename}", 'wb') as fd:
                fd.write(request.files['tar'].read())
            # Extract
            with tarfile.open(f"{temp_tar_dir}/{request.files['tar'].filename}", 'r') as tar:
                
                import os
                
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tar, temp_tar_dir)
            files = [f for f in os.listdir(temp_tar_dir) if f.endswith('.tsv')]

        else:
            return {"Error" : "No file is found"}, 400

        # import dataframe to database
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

        return None, 201
