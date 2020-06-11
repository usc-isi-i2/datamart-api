import csv

from pprint import pprint

import pandas as pd

from flask import request, make_response
from flask_restful import Resource

from api.SQLProvider import SQLProvider
from api.metadata.metadata import DatasetMetadata, VariableMetadata
from db.sql.kgtk import import_kgtk_dataframe

provider = SQLProvider()

class DatasetMetadataResource(Resource):
    def post(self):
        if not request.json:
            content = {
                'Error': 'JSON content body is empty'
            }
            return content, 400
        # print('Post dataset: ', request.json)
        metadata = DatasetMetadata()
        status, code = metadata.from_request(request.json)
        if not code == 200:
            return status, code

        if provider.get_dataset_id(metadata.shortName):
            content = {
                'Error': f'Dataset identifier {metadata.shortName} has already been used'
            }
            return content, 409

        dataset_id = f'Q{metadata.shortName}'
        count = 0
        while provider.node_exists(dataset_id):
            count += 1
            dataset_id = f'Q{metadata.shortName}{count}'

        metadata.datasetID = metadata.shortName
        metadata._dataset_id = dataset_id

        # pprint(metadata.to_dict())
        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_id))
        # pprint(edges)

        import_kgtk_dataframe(edges)

        content = metadata.to_dict()

        if 'tsv' in request.args:
            tsv = edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False)
            output = make_response(tsv)
            output.headers['Content-Disposition'] = f'attachment; filename={metadata.shortName}.tsv'
            output.headers['Content-type'] = 'text/tsv'
            return output


        return content, 200

    def get(self, dataset=None):
        provider = SQLProvider()
        results = provider.query_dataset_metadata(dataset)
        if results is None:
            return { 'error': "No such dataset" }, 404

        return results, 200


class VariableMetadataResource(Resource):
    def post(self, dataset):
        if not request.json:
            content = {
                'Error': 'JSON content body is empty'
            }
            return content, 400
        # print('Post variable: ', request.json)

        metadata = VariableMetadata()
        status, code = metadata.from_request(request.json)
        if not code == 200:
            return status, code

        dataset_id = provider.get_dataset_id(dataset)
        if not dataset_id:
            status = {
                'Error': f'Cannot find dataset {dataset}'
            }
            return  status, 404
        metadata.datasetID = dataset

        if metadata.shortName and provider.get_variable_id(dataset_id, metadata.shortName) is not None:
            status = {
                'Error': f'Variable {metadata.shortName} has already been defined in dataset {dataset}'
            }
            return status, 409

        # Create qnode for variable
        if not metadata.shortName:
            prefix = f'V{metadata.datasetID}-'
            number = provider.next_variable_value(dataset_id, prefix)
            metadata.shortName = f'{prefix}{number}'
        metadata.variableID = metadata.shortName
        variable_id = f'Q{metadata.datasetID}-{metadata.variableID}'
        metadata._variable_id = variable_id

        # pprint(metadata.to_dict())
        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_id, variable_id))
        # pprint(edges)

        if 'test' not in request.args:
            import_kgtk_dataframe(edges)

        content = metadata.to_dict()

        if 'tsv' in request.args:
            tsv = edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False)
            output = make_response(tsv)
            output.headers['Content-Disposition'] = f'attachment; filename={metadata.datasetID}-{metadata.variableID}.tsv'
            output.headers['Content-type'] = 'text/tsv'
            return output

        return content, 200
