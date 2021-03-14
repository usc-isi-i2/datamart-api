import csv
import sys
import traceback

import pandas as pd

from flask import make_response
from flask_restful import request, Resource

from api.util import get_edges_from_request
from db.sql.dal.entity import is_entity_used, check_existing_entities, delete_entity, query_entity
from db.sql.kgtk import import_kgtk_dataframe

def is_same_as_existing(entity, edges):
    existing_def = query_entity(entity)
    existing_def = existing_def.sort_values(['label']).reset_index(drop=True)
    new_def = edges[edges['node1']==entity].sort_values(['label']).reset_index(drop=True)
    same = (existing_def == new_def).all().all()
    return same


class EntityResource(Resource):
    label_white_list = ['label', 'description', 'alias', 'P31']

    def get_edges(self) -> pd.DataFrame:
        if request.files is None:
            content = {
                'Error': 'Missing TSV edge file'
            }
            return content, 400

        valid_column_names = ['id', 'node1', 'label', 'node2']
        for key, file_storage in request.files.items():
            edges = pd.read_csv(file_storage, sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')
            # Get just the first file
            break

        if not set(edges.columns) == set(valid_column_names):
            content = {
                'Error': f'Invalid TSV columns: {edges.columns}. Expecting: {valid_column_names}'
            }
            return content, 400

        edges = edges.loc[:, valid_column_names]
        return edges

    def get(self, entity=None):
        '''Get entity'''

        entity = request.args.get('name', None)
        label = request.args.get('label', None)

        if entity is None and label is None:
            edges = query_entity()
            entity = 'entities'
        elif entity is None: # has label
            edges = query_entity(entity_label=label)
        elif entity.startswith('Q'): # prioritize Q node
            edges = query_entity(entity)
        else:
            content = {
                'Error': f'Entity name must start with the letter "Q": {entity}'
            }
            return content, 400

        if edges.shape[0] == 0:
            return None, 204

        tsv =  edges.to_csv(index=False, sep="\t", quoting=csv.QUOTE_NONE)
        output = make_response(tsv)
        if not entity is None:
            output.headers['Content-Disposition'] = f'attachment; filename={entity}.tsv'
        else:
            output.headers['Content-Disposition'] = f'attachment; filename={label}.tsv'
        output.headers['Content-type'] = 'text/tab-separated-values'
        return output

    def post(self, entity=None):
        '''Post one or more entities'''

        if entity is not None:
            content = {
                'Error': f'Cannot post to an entity: {entity}'
            }
            return content, 400

        edges = self.get_edges()

        illegal_labels = [x for x in edges.loc[:,'label'].unique() if x not in self.label_white_list]
        if illegal_labels:
            content = {
                'Error': f'Labels not in white list: {",".join(illegal_labels)}'
            }
            return content, 400

        entities = edges.loc[:,'node1'].unique()

        existing_entities = check_existing_entities(entities)

        # Remove the current definition before updating the new definition
        for ent in existing_entities:
            delete_entity(ent)

        print('new')
        print(edges.to_csv(index=False, quoting=csv.QUOTE_NONE))


        try:
            import_kgtk_dataframe(edges, is_file_exploded=False)
        except Exception as e:
            print("Failed to import exploded kgtk file", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            content = {
                'Error': 'Failed to import imploded kgtk file'
            }
            return content, 400



        return None, 201

    def put(self, entity=None):
        '''Create/redefine entity'''

        if entity is None:
            content = {
                'Error': 'Must specify entity name'
            }
            return content, 400

        if not entity.startswith('Q'):
            content = {
                'Error': f'Entity name must start with the letter "Q": {entity}'
            }
            return content, 400

        try:
            edges = get_edges_from_request()
        except ValueError as e:
            return e.args[0], 400

        print(edges)

        illegal_labels = [x for x in edges.loc[:,'label'].unique() if x not in self.label_white_list]
        if illegal_labels:
            content = {
                'Error': f'Labels not in white list: {",".join(illegal_labels)}'
            }
            return content, 400

        entities = list(edges['node1'].drop_duplicates())
        if len(entities) > 1:
            content = {
                'Error': f'Edge file contains more than one entity: {",".join(entities)}'
            }
            return content, 400

        if entity not in entities:
            content = {
                'Error': f'Property in edge file {entities[0]} does not match property in URL {entity}'
            }
            return content, 400

        existing_entities = check_existing_entities([entity])
        if entity in existing_entities:
            delete_entity(entity)

        try:
            import_kgtk_dataframe(edges, is_file_exploded=False)
        except Exception as e:
            print("Failed to import exploded kgtk file", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            content = {
                'Error': 'Failed to import imploded kgtk file'
            }
            return content, 400

        if entity in existing_entities:
            return None, 200
        else:
            return None, 201

    def delete(self, entity=None):
        '''Delete given entity'''
        if entity is None:
            content = {
                'Error': 'Must specify entity name'
            }
            return content, 400

        if not entity.startswith('Q'):
            content = {
                'Error': f'Entity name must start with the letter "Q": {entity}'
            }
            return content, 400

        if is_entity_used(entity):
            content = {
                'Error': f'Cannot delete entity that is in use: {entity}'
            }
            return content, 400


        delete_entity(entity)
        return None, 200
