import json
import pandas as pd
from db.sql import dal
from flask import request
from annotation.validation.validate_annotation import VaidateAnnotation
from annotation.generation.generate_t2wml import ToT2WML
from annotation.generation.generate_kgtk import GenerateKgtk
from api.metadata.main import VariableMetadataResource
from db.sql.kgtk import import_kgtk_dataframe


class AnnotatedData(object):
    def __init__(self):
        self.va = VaidateAnnotation()
        self.vmr = VariableMetadataResource()

    def process(self, dataset):
        # check if the dataset exists
        dataset_id = dal.get_dataset_id(dataset)

        if not dataset_id:
            return {'Error': 'Dataset not found: {}'.format(dataset)}, 404

        df = pd.read_csv(request.files['file'], dtype=object, header=None).fillna('')
        validation_report, valid_annotated_file = self.va.validate(df=df)
        if not valid_annotated_file:
            return json.loads(validation_report), 400

        # get the t2wml yaml file
        # TODO finish this section
        to_t2wml = ToT2WML(df)
        t2wml_yaml_dict = to_t2wml.get_dict()

        # generate kgtk exploded file
        # TODO finish this section
        gk = GenerateKgtk(df, t2wml_yaml_dict)
        kgtk_exploded_df = gk.generate_edges_df()

        # import to database
        import_kgtk_dataframe(kgtk_exploded_df)

        variables_metadata = []
        variable_ids = gk.get_variables()
        for v in variable_ids:
            variables_metadata.append(self.vmr.get(dataset, variable=v)[0])

        return variables_metadata, 201
