import csv
import sys
import traceback

import pandas as pd

from flask import make_response
from flask_restful import request, Resource

from db.sql.dal.property import is_property_used, check_existing_properties, delete_property, query_property
from db.sql.kgtk import import_kgtk_dataframe

def is_same_as_existing(property, edges):
    existing_def = query_property(property)
    existing_def = existing_def.sort_values(['label']).reset_index(drop=True)
    new_def = edges[edges['node1']==property].sort_values(['label']).reset_index(drop=True)
    same = (existing_def == new_def).all().all()
    return same


class PropertyResource(Resource):
    label_white_list = ['label', 'description', 'data_type', 'alias', 'P31']

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

    def get(self, property=None):
        '''Get property'''
        if property is None:
            edges = query_property()
            property = 'properties'
        elif property.startswith('P'):
            edges = query_property(property)
        else:
            content = {
                'Error': f'Property name must start with the letter "P": {property}'
            }
            return content, 400

        if edges.shape[0] == 0:
            return None, 204

        tsv =  edges.to_csv(index=False, sep="\t", quoting=csv.QUOTE_NONE)
        output = make_response(tsv)
        output.headers['Content-Disposition'] = f'attachment; filename={property}.tsv'
        output.headers['Content-type'] = 'text/tab-separated-values'
        return output

    def post(self, property=None):
        '''Post one or more properties'''

        if property is not None:
            content = {
                'Error': f'Cannot post to a property: {property}'
            }
            return content, 400

        edges = self.get_edges()
        # print(edges.to_csv(index=False, quoting=csv.QUOTE_NONE))

        illegal_labels = [x for x in edges.loc[:,'label'].unique() if x not in self.label_white_list]
        if illegal_labels:
            content = {
                'Error': f'Labels not in white list: {",".join(illegal_labels)}'
            }
            return content, 400

        properties = edges.loc[:,'node1'].unique()

        existing_properties = check_existing_properties(properties)

        redefinitions = []
        for prop in existing_properties:
            same = is_same_as_existing(prop, edges)
            if not same:
                redefinitions.append(prop)

        # create only new properties
        edges = edges[~edges.loc[:, 'node1'].isin(existing_properties)]

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

        if redefinitions:
            content = {
                'Error': f'Cannot redefine existing properties: {",".join(redefinitions)}'
            }
            return content, 409

        return None, 201

    def put(self, property=None):
        '''Create/redefine property'''

        if property is None:
            content = {
                'Error': 'Must specify property name'
            }
            return content, 400

        if not property.startswith('P'):
            content = {
                'Error': f'Property name must start with the letter "P": {property}'
            }
            return content, 400

        edges = self.get_edges()

        illegal_labels = [x for x in edges.loc[:,'label'].unique() if x not in self.label_white_list]
        if illegal_labels:
            content = {
                'Error': f'Labels not in white list: {",".join(illegal_labels)}'
            }
            return content, 400

        properties = list(edges['node1'].drop_duplicates())
        if len(properties) > 1:
            content = {
                'Error': f'Edge file contains more than one property: {",".join(properties)}'
            }
            return content, 400

        if property not in properties:
            content = {
                'Error': f'Property in edge file {properties[0]} does not match property in URL {property}'
            }
            return content, 400

        existing_properties = check_existing_properties([property])
        if property in existing_properties:
            if not is_same_as_existing(property, edges) and is_property_used(property):
                content = {
                    'Error': 'cannot redefine property that is in use: {property}'
                }
                return content, 400
            delete_property(property)

        try:
            import_kgtk_dataframe(edges, is_file_exploded=False)
        except Exception as e:
            print("Failed to import exploded kgtk file", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            content = {
                'Error': 'Failed to import imploded kgtk file'
            }
            return content, 400

        if property in existing_properties:
            return None, 200
        else:
            return None, 201


    def delete(self, property=None):
        '''Delete given property'''
        if property is None:
            content = {
                'Error': 'Must specify property name'
            }
            return content, 400

        if not property.startswith('P'):
            content = {
                'Error': f'Property name must start with the letter "P": {property}'
            }
            return content, 400

        if is_property_used(property):
            content = {
                'Error': f'Cannot delete property that is in use: {property}'
            }
            return content, 400


        delete_property(property)
        return None, 200
