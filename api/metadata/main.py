import csv
import pandas as pd
from flask_restful import Resource
from flask import request, make_response

from api.util import get_edges_from_request
from api.variable.delete import VariableDeleter
from api.metadata.metadata import DatasetMetadata, VariableMetadata
from api.metadata.update import DatasetMetadataUpdater
from api.region_utils import get_query_region_ids, UnknownSubjectError
from db.sql import dal
from db.sql.kgtk import import_kgtk_dataframe, unquote


class VariableMetadataResource(Resource):
    def put(self, dataset, variable=None):
        '''Update metadata'''
        if variable is None:
            content = {
                'Error': 'Variable id is required.'
            }
            return content, 400

        dataset_qnode = dal.get_dataset_id(dataset)
        if not dataset_qnode:
            status = {
                'Error': f'Cannot find dataset {dataset}'
            }
            return status, 404

        # Get current variable metadata
        metadata_dict = dal.query_variable_metadata(dataset, variable, debug=False)
        if metadata_dict is None:
            return {'Error': f"No variable {variable} in dataset {dataset}"}, 404
        variable_qnode = dal.get_variable_id(dataset_qnode, variable)

        metadata_dict['dataset_id'] = dataset

        if request.json:
            return self.put_json(dataset, dataset_qnode, variable, metadata_dict, variable_qnode)
        return self.put_edges(dataset, dataset_qnode, variable, metadata_dict, variable_qnode)

    def put_edges(self, dataset, dataset_qnode, variable, metadata_dict, variable_qnode):
        try:
            edges = get_edges_from_request()
        except ValueError as e:
            return e.args[0], 400

        # Must provide complete set of variable metadata edges

        metadata = VariableMetadata()
        status, code = metadata.validate_edges(edges, dataset_qnode, dataset)
        if not code == 200:
            return status, code

        variable_edges = edges[edges.loc[:, 'label'] != 'P2006020003']
        var_qnodes = variable_edges['node1'].unique()
        if not var_qnodes[0] == variable_qnode or len(var_qnodes) > 1:
            status = {
                'Error': f'Edges must be about variable {variable} ({variable_qnode})'
            }
            return status, 400


        dal.delete_variable_metadata(dataset_qnode, [variable_qnode])

        # import variable metadatga
        import_kgtk_dataframe(edges)

        DatasetMetadataUpdater().update(dataset)

        result = dal.query_variable_metadata(dataset, variable)
        result['dataset_id'] = dataset
        result = VariableMetadata().from_dict(result).to_dict()

        return result, 200

    def put_json(self, dataset, dataset_qnode, variable, metadata_dict, variable_qnode):
        # Delete existing fields that are to be updated
        request_dict = request.json
        for field_name in request_dict.keys():
            if field_name not in VariableMetadata.fields():
                return {'Error': f'Not valid field name: {field_name}'}, 404
        labels = [VariableMetadata.get_property(field_name) for field_name in request_dict.keys()]

        # If name changes, then so should its label.
        # Note: variable metadata uses 'P1476', but CSV data uses 'label'
        if 'P1476' in labels:
            labels.append('label')

        dal.delete_variable_metadata(dataset_qnode, [variable_qnode], labels=labels, debug=False)

        # Update and validate
        metadata_dict.update(request_dict)
        metadata = VariableMetadata().from_dict(metadata_dict)

        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_qnode, variable_qnode))
        edges = edges[edges['label'].isin(labels)]

        import_kgtk_dataframe(edges)

        DatasetMetadataUpdater().update(dataset)

        results = dal.query_variable_metadata(dataset, variable)
        results['dataset_id'] = dataset
        results = VariableMetadata().from_dict(results).to_dict()

        return results, 200

    def post(self, dataset, variable=None):
        if variable:
            content = {
                'Error': 'Please do not supply a variable when POSTing'
            }
            return content, 400

        dataset_qnode = dal.get_dataset_id(dataset)  # qnode
        if not dataset_qnode:
            status = {
                'Error': f'Cannot find dataset {dataset}'
            }
            return status, 404

        if request.json:
            return self.post_json(dataset, dataset_qnode)
        return self.post_edges(dataset, dataset_qnode)

    def post_edges(self, dataset, dataset_qnode):
        try:
            edges = get_edges_from_request()
        except ValueError as e:
            return e.args[0], 400

        metadata = VariableMetadata()
        status, code = metadata.validate_edges(edges, dataset_qnode, dataset)
        if not code == 200:
            return status, code

        p1813_edges = edges[edges.loc[:, 'label'] == 'P1813']

        already_defined = []
        for edge in p1813_edges.itertuples(index=False):
            variable_id = unquote(edge.node2)
            if dal.get_variable_id(dataset_qnode, variable_id) is not None:
                already_defined.append(variable_id)
        if already_defined:
            status = {
                'Error': f'Variable has already been defined in dataset {dataset}: {already_defined}'
            }
            return status, 409

        # Need to check variable qnode is not used

        # import variable metadatga
        import_kgtk_dataframe(edges)

        results = []
        for i, edge in p1813_edges.iterrows():
            variable = unquote(edge['node2'])
            result = dal.query_variable_metadata(dataset, variable)
            result['dataset_id'] = dataset
            result = VariableMetadata().from_dict(result).to_dict()
            results.append(result)

        if len(results) == 1:
            # Return dict to be compatible with existing post_json
            return results[0], 201
        else:
            return results, 201


    def post_json(self, dataset, dataset_id):
        metadata: VariableMetadata = VariableMetadata()
        status, code = metadata.from_request(request.json)
        if not code == 200:
            return status, code

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

        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_id, variable_id))

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
            results = dal.query_dataset_variables(dataset, True)
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
        if variable is None:
            variables = request.args.getlist('variable')
            if not variables:
                results = dal.query_dataset_variables(dataset, False)
                variables = [result['variable_id'] for result in results]
        else:
            variables = [variable]

        dataset_id = None
        property_ids = []
        qnodes = []
        for variable in variables:
            result = dal.query_variable(dataset, variable)
            if not result:
                content = {
                    'Error': f'Could not find dataset: {dataset} variable: {variable}'
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

    def put(self, dataset=None) :
        if not dataset:
            content = {
                'Error': 'Please supply a dataset-id when PUTing'
            }
            return content, 400

        dataset_qnode = dal.get_dataset_id(dataset)
        if not dataset_qnode:
            content = {
                'Error': f'Dataset is not defined {dataset}'
            }
            return content, 404

        if request.json:
            return self.put_json(dataset_qnode, dataset, request.json)
        return self.put_edges(dataset_qnode, dataset)

    def put_edges(self, dataset_qnode:str, dataset:str):
        try:
            edges = get_edges_from_request()
        except ValueError as e:
            return e.args[0], 400

        metadata = DatasetMetadata()
        status, code = metadata.validate_edges(edges, dataset_qnode, dataset)
        if not code == 200:
            return status, code

        import_kgtk_dataframe(edges)

        DatasetMetadataUpdater().update(dataset)

        result = dal.query_dataset_metadata(dataset)[0]

        # validate, just in case
        result = DatasetMetadata().from_dict(result).to_dict()

        return result, 200

    def put_json(self, dataset_qnode:str, dataset:str, request_json:dict):
        request_dict = request_json

        invalid_metadata = False
        error_report = []
        for key in request_dict:
            if request_dict[key].strip() == "":
                error_report.append(
                    {'error': f'Metadata field: {key}, cannot be blank'}
                )
                invalid_metadata = True

        if invalid_metadata:
            return error_report, 400

        metadata = DatasetMetadata()
        status, code = metadata.from_request(request_dict, check_required_fields=False)
        if not code == 200:
            return status, code

        labels = [DatasetMetadata.get_property(name) for name in request_dict]

        # If name changes, then so should its label
        if 'P1476' in labels:
            labels.append('label')

        # Get current dataset metadat
        metadata_dict = dal.query_dataset_metadata(dataset, debug=False)[0]

        # delete old edges
        dal.delete_dataset_metadata(dataset_qnode, labels=labels, debug=False)

        metadata_dict.update(request_dict)
        metadata = DatasetMetadata().from_dict(metadata_dict)

        # keep just the changed edges
        edges = pd.DataFrame(metadata.to_kgtk_edges(dataset_qnode))
        edges = edges[edges['label'].isin(labels)]

        import_kgtk_dataframe(edges)

        DatasetMetadataUpdater().update(dataset)

        result = dal.query_dataset_metadata(dataset)[0]

        # validate, just in case
        result = DatasetMetadata().from_dict(result).to_dict()

        return result, 200

    def post(self, dataset=None):
        if dataset:
            content = {
                'Error': 'Please do not supply a dataset-id when POSTing'
            }
            return content, 400

        if request.json:
            return self.post_json()
        return self.post_edges()

    def post_edges(self):
        try:
            edges = get_edges_from_request()
        except ValueError as e:
            return e.args[0], 400

        if edges.shape[0] == 0:
            content = {
                'Error': 'Empty edge file'
            }
            return content, 400

        metadata = DatasetMetadata()
        status, code = metadata.validate_edges(edges)
        if not code == 200:
            return status, code

        dataset_qnode = edges.iloc[0]['node1']
        p1813_edge = edges[edges.loc[:, 'label'] == 'P1813']
        dataset = unquote(p1813_edge.iloc[0]['node2'])

        if dal.get_dataset_id(dataset):
            content = {
                'Error': f'Dataset identifier {dataset} has already been used'
            }
            return content, 409

        if dal.qnode_exists(dataset_qnode):
            content = {
                'Error': f'Dataset qnode {dataset_qnode} has already been used'
            }
            return content, 409

        import_kgtk_dataframe(edges)

        DatasetMetadataUpdater().update(dataset)

        result = dal.query_dataset_metadata(dataset)[0]

        # validate, just in case
        result = DatasetMetadata().from_dict(result).to_dict()

        return result, 201


    def post_json(self):
        request_metadata = request.json

        invalid_metadata = False
        error_report = []
        for key in request_metadata:
            if request_metadata[key].strip() == "":
                error_report.append(
                    {'error': f'Metadata field: {key}, cannot be blank'}
                )
                invalid_metadata = True

        if invalid_metadata:
            return error_report, 400

        metadata = DatasetMetadata()
        status, code = metadata.from_request(request_metadata)
        if not code == 200:
            return status, code

        if dal.get_dataset_id(metadata.dataset_id):
            content = {
                'Error': f'Dataset identifier {metadata.dataset_id} has already been used'
            }
            return content, 409

        _, edges = DatasetMetadataUpdater().create_dataset(metadata, create='test' not in request.args)

        content = metadata.to_dict()

        # Used for debugging
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

        try:
            regions = get_query_region_ids(request.args)
        except UnknownSubjectError as ex:
            return ex.get_error_dict(), 404

        # if regions.get('admin1') or regions.get('admin2') or regions.get('admin3'):
        #    return {'Error': 'Filtering on admin1, admin2 or admin3 levels is not supported'}, 400

        try:
            limit = int(request.args.get('limit', 100))
            if limit < 1:
                limit = 100
        except:
            limit = 100

        tags = request.args.getlist('tag')

        # We're using Postgres's full text search capabilities for now
        results = dal.fuzzy_query_variables(queries, regions, tags, limit, True)

        # Due to performance issues we will solve later, adding a JOIN to get the dataset short name makes the query
        # very inefficient, so results only have dataset_ids. We will now add the short_names
        dataset_results = dal.query_dataset_metadata(include_dataset_qnode=True)
        datasets = {row['dataset_qnode']: row['dataset_id'] for row in dataset_results}
        for row in results:
            row['dataset_id'] = datasets[row['dataset_qnode']]
            del row['dataset_qnode']

        return results


"""
Query based on materialized views:

SELECT fuzzy.variable_id, fuzzy.variable_qnode, fuzzy.variable_property, fuzzy.dataset_qnode, fuzzy.name,  ts_rank(variable_text, (plainto_tsquery('worker'))) AS rank FROM
        (
SELECT e_var_name.node1 AS variable_qnode,
		        e_var_name.node2 AS variable_id,
		 		e_var_property.node2 AS variable_property,
                -- e_dataset_name.node2 AS dataset_id,
                e_dataset.node1 AS dataset_qnode,
                to_tsvector(CONCAT(s_description.text, ' ', s_name.text, ' ', s_label.text)) AS variable_text,
                CONCAT(s_name.text, ' ', s_label.text) as name
            FROM edges e_var
            JOIN edges e_var_name ON (e_var_name.node1=e_var.node1 AND e_var_name.label='P1813')
		    JOIN edges e_var_property ON (e_var_property.node1=e_var.node1 AND e_var_property.label='P1687')
            JOIN edges e_dataset ON (e_dataset.label='P2006020003' AND e_dataset.node2=e_var.node1)
                    -- JOIN edges e_dataset_name ON (e_dataset_name.node1=e_dataset.node1 AND e_dataset_name.label='P1813')
            LEFT JOIN edges e_description JOIN strings s_description ON (e_description.id=s_description.edge_id) ON (e_var.node1=e_description.node1 AND e_description.label='description')
            LEFT JOIN edges e_name JOIN strings s_name ON (e_name.id=s_name.edge_id) ON (e_var.node1=e_name.node1 AND e_name.label='P1813')
            LEFT JOIN edges e_label JOIN strings s_label ON (e_label.id=s_label.edge_id) ON (e_var.node1=e_label.node1 AND e_label.label='label')
	WHERE e_var.label='P31' AND e_var.node2='Q50701'  AND (
	EXISTS (SELECT 1 FROM fuzzy_country_main fcm WHERE fcm.variable_property=e_var_property.node2 AND fcm.dataset_qnode=e_dataset.node1 AND fcm.country_qnode='Q115')
	OR 	EXISTS (SELECT 1 FROM fuzzy_country_qualifier fcq WHERE fcq.variable_property=e_var_property.node2 AND fcq.dataset_qnode=e_dataset.node1 AND fcq.country_qnode='Q115')
)
) AS fuzzy
    WHERE variable_text @@ (plainto_tsquery('worker'))
    ORDER BY rank DESC
	LIMIT 10

"""
