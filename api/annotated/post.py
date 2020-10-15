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
from annotation.validation.validate_annotation import ValidateAnnotation
from time import time
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

        # check if the dataset exists
        s = time()
        dataset_qnode = dal.get_dataset_id(dataset)
        print(f'time take to get dataset: {time() - s} seconds')

        if not create_if_not_exist and not dataset_qnode:
            print(f'Dataset not defined: {dataset}')
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        file_name = request.files['file'].filename

        if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
            return {"Error": "Please upload an annotated excel file or a csv file "
                             "(file name ending with .xlsx or .csv)"}, 400

        if file_name.endswith('.xlsx'):
            df = pd.read_excel(request.files['file'], dtype=object, header=None).fillna('')
        elif file_name.endswith('.csv'):
            df = pd.read_csv(request.files['file'], dtype=object, header=None).fillna('')

        if create_if_not_exist and not dataset_qnode:
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
            dataset_qnode, _ = DatasetMetadataResource.create_dataset(metadata)

        s = time()
        validation_report, valid_annotated_file, rename_columns = self.va.validate(dataset, df=df)
        print(f'time take to validate annotated file: {time() - s} seconds')
        if validate:
            if not valid_annotated_file:
                return json.loads(validation_report), 400

        if files_only:
            t2wml_yaml, combined_item_def_df, consolidated_wikifier_df = self.ta.process(dataset_qnode, df,
                                                                                         rename_columns,
                                                                                         extra_files=True)

            temp_tar_dir = tempfile.mkdtemp()
            open(f'{temp_tar_dir}/t2wml.yaml', 'w').write(t2wml_yaml)
            combined_item_def_df.to_csv(f'{temp_tar_dir}/item_definitions_all.tsv', sep='\t', index=False)
            consolidated_wikifier_df.to_csv(f'{temp_tar_dir}/consolidated_wikifier.csv', index=False)

            with tarfile.open(f'{temp_tar_dir}/t2wml_annotation_files.tar.gz', "w:gz") as tar:
                tar.add(temp_tar_dir, arcname='.')
            return send_from_directory(temp_tar_dir, 't2wml_annotation_files.tar.gz')

        else:
            s = time()
            variable_ids, kgtk_exploded_df = self.ta.process(dataset_qnode, df, rename_columns)
            print(f'time take to create kgtk files: {time() - s} seconds')

            if is_request_put:
                # delete the variable canonical data and metadata before inserting into databse again!!
                for v in variable_ids:
                    print(self.vd.delete(dataset, v))
                    print(self.vmr.delete(dataset, v))

            # import to database
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

            variables_metadata = []
            for v in variable_ids:
                variables_metadata.append(self.vmr.get(dataset, variable=v)[0])
            print(f'total time taken: {time() - l}')
            return variables_metadata, 201
