import json
import sys
import pandas as pd
from pandas import DataFrame
from db.sql import dal
from flask import request
import tempfile
import tarfile
import csv
import shutil
import subprocess
from flask import send_from_directory
from annotation.main import T2WMLAnnotation
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from api.metadata.main import DatasetMetadataResource, VariableMetadataResource
from api.metadata.metadata import DatasetMetadata
from api.metadata.update import DatasetMetadataUpdater
from annotation.validation.validate_annotation import ValidateAnnotation
from time import time
from datetime import datetime
from typing import Dict, List, Any, Union, NoReturn, Optional, Tuple
import traceback


class AnnotatedData(object):
    def __init__(self):
        self.ta = T2WMLAnnotation()
        self.va = ValidateAnnotation()
        self.vmr = VariableMetadataResource()
        self.vd = VariableDeleter()

    def process(self, dataset, is_request_put=False):
        l = time()
        validate = request.args.get('validate', 'true').lower() == 'true'
        files_only = request.args.get('files_only', 'false').lower() == 'true'
        create_if_not_exist = request.args.get('create_if_not_exist', 'false').lower() == 'true'
        return_tsv = request.args.get('tsv', 'false').lower() == 'true'

        # check if the dataset exists
        s = time()
        dataset_qnode = dal.get_dataset_id(dataset)
        print(f'time take to get dataset: {time() - s} seconds')

        if not create_if_not_exist and not dataset_qnode:
            print(f'Dataset not defined: {dataset}')
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        file_name = request.files['file'].filename
        t2wml_yaml, metadata_edges = None, None
        if 't2wml_yaml' in request.files:
            request.files['t2wml_yaml'].seek(0)
            t2wml_yaml = str(request.files['t2wml_yaml'].read(), 'utf-8')

        if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
            return {"Error": "Please upload an annotated excel file or a csv file "
                             "(file name ending with .xlsx or .csv)"}, 400

        if file_name.endswith('.xlsx'):
            df = pd.read_excel(request.files['file'], dtype=object, header=None).fillna('')
        elif file_name.endswith('.csv'):
            df = pd.read_csv(request.files['file'], dtype=object, header=None).fillna('')

        if dataset_qnode:
            # only update metadata if we are going to insert data, if the request is only to return files, skip
            if not files_only:
                # update dataset metadata last_updated field
                DatasetMetadataUpdater().update(dataset)
        else:
            try:
                dataset_dict = {
                    'dataset_id': df.iloc[0, 1],
                    'name': df.iloc[0, 2],
                    'description': df.iloc[0, 3],
                    'url': df.iloc[0, 4]
                }
            except Exception as e:
                return {'Error': 'Failed to create dataset: ' + str(e)}, 400

            missing = []
            for key, value in dataset_dict.items():
                if not value:
                    missing.append(key)

            if len(missing) > 0:
                print(f'Dataset metadata missing fields: {missing}')
                return {'Error': f'Dataset metadata missing fields: {missing}'}, 404

            metadata = DatasetMetadata()
            metadata.from_dict(dataset_dict)
            dataset_qnode, metadata_edges = DatasetMetadataUpdater().create_dataset(metadata)

        s = time()
        validation_report, valid_annotated_file, rename_columns = self.va.validate(dataset, df=df)
        print(f'time take to validate annotated file: {time() - s} seconds')
        if validate:
            if not valid_annotated_file:
                return json.loads(validation_report), 400

        if files_only:
            t2wml_yaml, combined_item_def_df, consolidated_wikifier_df = self.ta.process(dataset_qnode, df,
                                                                                         rename_columns,
                                                                                         extra_files=True,
                                                                                         t2wml_yaml=t2wml_yaml)

            temp_tar_dir = tempfile.mkdtemp()
            open(f'{temp_tar_dir}/t2wml.yaml', 'w').write(t2wml_yaml)
            combined_item_def_df.to_csv(f'{temp_tar_dir}/item_definitions_all.tsv', sep='\t', index=False)
            consolidated_wikifier_df.to_csv(f'{temp_tar_dir}/consolidated_wikifier.csv', index=False)

            with tarfile.open(f'{temp_tar_dir}/t2wml_annotation_files.tar.gz', "w:gz") as tar:
                tar.add(temp_tar_dir, arcname='.')

            try:
                return send_from_directory(temp_tar_dir, 't2wml_annotation_files.tar.gz')
            finally:
                shutil.rmtree(temp_tar_dir)

        elif return_tsv:

            variable_ids, kgtk_exploded_df = self.generate_kgtk_dataset(dataset, dataset_qnode, df, rename_columns, t2wml_yaml, is_request_put)

            self.import_to_database(kgtk_exploded_df)

            temp_tar_dir = tempfile.mkdtemp()

            # Generate dataset kgtk file
            dataset_path = f'/{temp_tar_dir}/{dataset}-dataset-exploded.tsv'
            kgtk_exploded_df.to_csv(dataset_path, index=None, sep='\t', quoting=csv.QUOTE_NONE, quotechar='')

            # Generate dataset metadata kgtk file and explode it
            metadata_path = f'/{temp_tar_dir}/{dataset}-metadata-exploded.tsv'
            if metadata_edges is None:
                metadata_edges = self.generate_kgtk_dataset_metadata(dataset)
            metadata_df = pd.DataFrame(metadata_edges)
            metadata_df.to_csv(metadata_path, index=None, sep='\t', quoting=csv.QUOTE_NONE, quotechar='')
            subprocess.run(['kgtk', 'explode', "-i", metadata_path, '-o', metadata_path, '--allow-lax-qnodes'])

            # Concatenate
            output_path = f'/{temp_tar_dir}/{dataset}-exploded.tsv'
            subprocess.run(['kgtk', 'cat', metadata_path, dataset_path,
                            '--allow-lax-qnodes', 'True', '-o', output_path])
            subprocess.run(['kgtk', 'validate', '--allow-lax-qnodes', 'True', output_path])

            # Compress and return
            compressed_file_path = f'/{temp_tar_dir}/{dataset}-compressed.tar.gz'
            with tarfile.open(compressed_file_path, "w:gz") as tar:
                tar.add(output_path, arcname=f'./{dataset}-exploded.tsv')

            try:
                return send_from_directory(temp_tar_dir, f'{dataset}-compressed.tar.gz')
            finally:
                shutil.rmtree(temp_tar_dir)

        else:

            variable_ids, kgtk_exploded_df = self.generate_kgtk_dataset(dataset, dataset_qnode, df, rename_columns, t2wml_yaml, is_request_put)

            self.import_to_database(kgtk_exploded_df)

            variables_metadata = self.generate_variable_metadata(dataset, variable_ids)

            print(f'total time taken: {time() - l}')
            return variables_metadata, 201

    def import_to_database(self, kgtk_exploded_df: DataFrame) -> NoReturn:

        s = time()
        print('number of rows to be imported: {}'.format(len(kgtk_exploded_df)))
        try:
            import_kgtk_dataframe(kgtk_exploded_df, is_file_exploded=True)
        except Exception as e:
            # Not sure what's going on here, so print for debugging purposes
            print("Can't import exploded kgtk file")
            traceback.print_exc(file=sys.stdout)
            raise e
        print(f'time take to import kgtk file into database: {time() - s} seconds')

    def generate_dataset_metadata(self, dataset: str) -> Dict[str, Union[str, datetime]]:

        dataset_metadata = dal.query_dataset_metadata(dataset, include_dataset_qnode=True)[0]
        # Convert last_update from type<datetime.datetime> to type<str>
        dataset_metadata['last_update'] = dataset_metadata['last_update'].isoformat().split('.')[0]

        return dataset_metadata

    def generate_variable_metadata(self, dataset: str, variable_ids: List) -> List[Dict]:

        variables_metadata = []
        for v in variable_ids:
            variables_metadata.append(self.vmr.get(dataset, variable=v)[0])

        return variables_metadata

    def generate_kgtk_dataset_metadata(self, dataset: str) -> List:

        # Retrieve the metadata from database
        dataset_metadata = self.generate_dataset_metadata(dataset)
        dataset_qnode = dataset_metadata.pop('dataset_qnode')

        return DatasetMetadata().from_dict(dataset_metadata).to_kgtk_edges(dataset_qnode)

    def generate_kgtk_dataset(self, dataset: str, dataset_qnode: str, df: DataFrame,
                                rename_columns: List, t2wml_yaml: Optional[str] = None,
                                is_request_put: bool = False) -> Tuple[List[str], DataFrame]:

        s = time()
        variable_ids, kgtk_exploded_df = self.ta.process(dataset_qnode, df, rename_columns, t2wml_yaml=t2wml_yaml)
        print(f'time take to create kgtk files: {time() - s} seconds')
        variable_ids = [v.replace('"', '') for v in variable_ids]
        if is_request_put:
            # delete the variable canonical data and metadata before inserting into databse again!!
            for v in variable_ids:
                print(self.vd.delete(dataset, v))
                print(self.vmr.delete(dataset, v))

        return variable_ids, kgtk_exploded_df
