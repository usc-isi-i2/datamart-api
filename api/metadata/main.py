import csv
import pandas as pd
from db.sql import dal
from flask_restful import Resource
from flask import request, make_response
from db.sql.kgtk import import_kgtk_dataframe
from api.variable.delete import VariableDeleter
from api.metadata.metadata import DatasetMetadata, VariableMetadata


class VariableMetadataResource(Resource):
    def post(self, dataset, variable=None):
        if not request.json:
            content = {
                'Error': 'JSON content body is empty'
            }
            return content, 400
        # print('Post variable: ', request.json)

        if variable:
            content = {
                'Error': 'Please do not supply a variable when POSTing'
            }
            return content, 400

        metadata: VariableMetadata = VariableMetadata()
        status, code = metadata.from_request(request.json)
        if not code == 200:
            return status, code

        dataset_id = dal.get_dataset_id(dataset)
        if not dataset_id:
            status = {
                'Error': f'Cannot find dataset {dataset}'
            }
            return status, 404
        metadata.dataset_id = dataset

        if metadata.variable_id and dal.get_variable_id(dataset_id, metadata.variable_id) is not None:
            status = {
                'Error': f'Variable {metadata.variable_id} has already been defined in dataset {dataset}'
            }
            return status, 409

        # Create qnode for variable
        if not metadata.variable_id:
            prefix = f'V{metadata.dataset_id}-'
            number = dal.next_variable_value(dataset_id, prefix)
            metadata.variable_id = f'{prefix}{number}'
        variable_id = f'Q{metadata.dataset_id}-{metadata.variable_id}'
        variable_pnode = f'P{metadata.dataset_id}-{metadata.variable_id}'
        metadata._variable_id = variable_id
        metadata.corresponds_to_property = variable_pnode

        # pprint(metadata.to_dict())
        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_id, variable_id))
        # pprint(edges)

        if 'test' not in request.args:
            import_kgtk_dataframe(edges)

        content = metadata.to_dict()

        if 'tsv' in request.args:
            tsv = edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False)
            output = make_response(tsv)
            output.headers[
                'Content-Disposition'] = f'attachment; filename={metadata.dataset_id}-{metadata.variable_id}.tsv'
            output.headers['Content-type'] = 'text/tsv'
            return output

        return content, 201

    def get(self, dataset, variable=None):
        if variable is None:
            results = dal.query_dataset_variables(dataset, False)
            if results is None:
                return {'Error': f"No dataset {dataset}"}, 404
            results = [VariableMetadata().from_dict(x).to_dict() for x in results]
        else:
            results = dal.query_variable_metadata(dataset, variable)
            if results is None:
                return {'Error': f"No variable {variable} in dataset {dataset}"}, 404
            results['dataset_id'] = dataset
            results = VariableMetadata().from_dict(results).to_dict()

        return results, 200

    def delete(self, dataset, variable=None):
        try:
            limit = int(request.args.get('limit', '20'))
        except ValueError:
            return {'Error': 'Invalid limit'}, 400

        if variable is None:
            variables = request.args.getlist('variable')
            if not variables:
                results = dal.query_dataset_variables(dataset, False)
                variables = [result['variable_id'] for result in results]
        else:
            variables = [variable]

        variables = variables[:limit]
        dataset_id = None
        property_ids = []
        qnodes = []
        for variable in variables:
            result = dal.query_variable(dataset, variable)
            if not result:
                content = {
                    'Error': f'Could not find dataset {dataset} variable {variable}'
                }
                return content, 404
            dataset_id = result['dataset_id']
            property_ids.append(result['property_id'])
            qnodes.append(result['variable_qnode'])

        if dal.variable_data_exists(dataset_id, property_ids):
            return {'Error': f"Please delete all variable data before deleting metadata"}, 409

        dal.delete_variable_metadata(dataset_id, qnodes)
        return {'Message': f'Successfully deleted {str(variables)} in the dataset: {dataset}.'}, 200


class DatasetMetadataResource(Resource):
    vd = VariableDeleter()
    vmr = VariableMetadataResource()

    def post(self, dataset=None):
        if not request.json:
            content = {
                'Error': 'JSON content body is empty'
            }
            return content, 400

        if dataset:
            content = {
                'Error': 'Please do not supply a dataset-id when POSTing'
            }
            return content, 400

        # print('Post dataset: ', request.json)
        metadata = DatasetMetadata()
        status, code = metadata.from_request(request.json)
        if not code == 200:
            return status, code

        if dal.get_dataset_id(metadata.dataset_id):
            content = {
                'Error': f'Dataset identifier {metadata.dataset_id} has already been used'
            }
            return content, 409

        # Create qnode
        dataset_id = f'Q{metadata.dataset_id}'
        count = 0
        while dal.node_exists(dataset_id):
            count += 1
            dataset_id = f'Q{metadata.dataset_id}{count}'
        metadata._dataset_id = dataset_id

        # pprint(metadata.to_dict())
        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_id))
        # pprint(edges)

        if 'test' not in request.args:
            import_kgtk_dataframe(edges)

        content = metadata.to_dict()

        if 'tsv' in request.args:
            tsv = edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False)
            output = make_response(tsv)
            output.headers['Content-Disposition'] = f'attachment; filename={metadata.dataset_id}.tsv'
            output.headers['Content-type'] = 'text/tsv'
            return output

        return content, 201

    def get(self, dataset=None):
        results = dal.query_dataset_metadata(dataset)
        if results is None:
            return {'Error': f"No such dataset {dataset}"}, 404

        # validate
        results = [DatasetMetadata().from_dict(x).to_dict() for x in results]

        return results, 200

    def delete(self, dataset=None):
        if dataset is None:
            return {'Error': 'Please provide a dataset'}, 400

        dataset_metadata = dal.query_dataset_metadata(dataset, include_dataset_qnode=True)
        if not dataset_metadata:
            return {'Error': f'No such dataset {dataset}'}, 404

        forced = request.args.get('force', 'false').lower() == 'true'

        variables = dal.query_dataset_variables(dataset)
        if variables:
            if forced:
                variable_ids = [x['variable_id'] for x in variables]
                for v in variable_ids:
                    print(self.vd.delete(dataset, v))
                    print(self.vmr.delete(dataset, v))
            else:
                return {'Error': f'Dataset {dataset} is not empty'}, 409

        dal.delete_dataset_metadata(dataset_metadata[0]['dataset_qnode'])
        return {'Message': f'Dataset {dataset} deleted'}, 200


class FuzzySearchResource(Resource):
    def get(self):
        queries = request.args.getlist('keyword')
        if not queries:
            return {'Error': 'A variable query must be provided: keyword'}, 400

        # We're using Postgres's full text search capabilities for now
        results = dal.fuzzy_query_variables(queries, False)

        # Due to performance issues we will solve later, adding a JOIN to get the dataset short name makes the query
        # very inefficient, so results only have dataset_ids. We will now add the short_names
        dataset_results = dal.query_dataset_metadata(include_dataset_qnode=True)
        datasets = {row['dataset_qnode']: row['dataset_id'] for row in dataset_results}
        for row in results:
            row['dataset_id'] = datasets[row['dataset_qnode']]
            del row['dataset_qnode']

        return results
