import json
import pandas as pd
from db.sql import dal
from flask import request
from annotation.main import T2WMLAnnotation
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from api.metadata.main import VariableMetadataResource
from annotation.validation.validate_annotation import ValidateAnnotation


class AnnotatedData(object):
    def __init__(self):
        self.ta = T2WMLAnnotation()
        self.va = ValidateAnnotation()
        self.vmr = VariableMetadataResource()
        self.vd = VariableDeleter()

    def process(self, dataset, is_request_put=False):
        validate = request.args.get('validate', 'true').lower() == 'true'

        # check if the dataset exists
        dataset_qnode = dal.get_dataset_id(dataset)

        if not dataset_qnode:
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        file_name = request.files['file'].filename

        if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
            return {"Error": "Please upload an annotated excel file or a csv file "
                             "(file name ending with .xlsx or .csv)"}, 400

        if file_name.endswith('.xlsx'):
            df = pd.read_excel(request.files['file'], dtype=object, header=None).fillna('')
        elif file_name.endswith('.csv'):
            df = pd.read_csv(request.files['file'], dtype=object, header=None).fillna('')

        validation_report, valid_annotated_file, rename_columns = self.va.validate(dataset, df=df)
        if validate:
            if not valid_annotated_file:
                return json.loads(validation_report), 400

        variable_ids, kgtk_exploded_df = self.ta.process(dataset_qnode, df, rename_columns)

        if is_request_put:
            # delete the variable canonical data and metadata before inserting into databse again!!
            for v in variable_ids:
                print(self.vd.delete(dataset, v))
                print(self.vmr.delete(dataset, v))

        # import to database
        import_kgtk_dataframe(kgtk_exploded_df, is_file_exploded=True)

        variables_metadata = []
        for v in variable_ids:
            variables_metadata.append(self.vmr.get(dataset, variable=v)[0])

        return variables_metadata, 201
