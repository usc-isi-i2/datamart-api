import csv
import unittest
import pandas as pd
from io import StringIO
from requests import post, put
from test.utility import create_variable, create_dataset, delete_variable, delete_dataset, create_variables_with_edges, variable_edges


class TestCreateVariable(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'

    def test_create_variable(self):
        delete_variable(self.url)
        delete_dataset(self.url)
        expected_response = {
            "name": "unit test variable",
            "variable_id": "unittestvariable",
            "dataset_id": "unittestdataset",
            "corresponds_to_property": "Punittestdataset-unittestvariable"
        }

        dataset_id = create_dataset(self.url).json()['dataset_id']
        response = create_variable(self.url, dataset_id)

        self.assertEqual(response.status_code, 201)
        self.assertEqual(expected_response, response.json())
        delete_variable(self.url)
        delete_dataset(self.url)

    # TODO: Need to update. Edge file upload changed.
    '''
    def test_create_variable_with_edge_file(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        expected_response = [
            {'name': 'Variable-Qvariable-unittestdataset-001',
             'variable_id': 'variable-unittestdataset-001',
             'dataset_id': 'unittestdataset',
             'description': 'Variable Qvariable-unittestdataset-001 for dataset Qunittestdataset',
             'corresponds_to_property': 'P1687',
             'qualifier': [
                 {'name': 'stated in', 'identifier': 'P248'},
                 {'name': 'point in time', 'identifier': 'P585'}]},
            {'name': 'Variable-Qvariable-unittestdataset-002',
             'variable_id': 'variable-unittestdataset-002',
             'dataset_id': 'unittestdataset',
             'description': 'Variable Qvariable-unittestdataset-002 for dataset Qunittestdataset',
             'corresponds_to_property': 'P1687',
             'qualifier': [
                 {'name': 'stated in', 'identifier': 'P248'},
                 {'name': 'point in time', 'identifier': 'P585'}]}]

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        response = create_variables_with_edges(self.url, dataset_id)

        self.assertEqual(response.status_code, 201, response.json())
        self.assertEqual(expected_response, response.json())

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)
    '''

    def test_create_variable_with_edge_file_missing_P31_fail(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        edges = create_variables_with_edges(self.url, dataset_id, return_edges=True)
        edges = edges[edges.loc[:, 'label']!='P31']
        post_url = f'{self.url}/metadata/datasets/{dataset_id}/variables'
        response = post(post_url , files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 400)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

    def test_create_variable_with_edge_file_missing_label_fail(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        edges = create_variables_with_edges(self.url, dataset_id, return_edges=True)
        edges = edges[edges.loc[:, 'label']!='label']
        post_url = f'{self.url}/metadata/datasets/{dataset_id}/variables'
        response = post(post_url , files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 400)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

    def test_create_variable_with_edge_file_missing_P1476_fail(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        edges = create_variables_with_edges(self.url, dataset_id, return_edges=True)
        edges = edges[edges.loc[:, 'label']!='P1476']
        post_url = f'{self.url}/metadata/datasets/{dataset_id}/variables'
        response = post(post_url , files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 400)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

    def test_create_variable_with_edge_file_missing_P2006020002_fail(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        edges = create_variables_with_edges(self.url, dataset_id, return_edges=True)
        edges = edges[edges.loc[:, 'label']!='P2006020002']
        post_url = f'{self.url}/metadata/datasets/{dataset_id}/variables'
        response = post(post_url , files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 400)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

    def test_create_variable_with_edge_file_missing_P2006020003_fail(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        edges = create_variables_with_edges(self.url, dataset_id, return_edges=True)
        edges = edges[edges.loc[:, 'label']!='P2006020003']
        post_url = f'{self.url}/metadata/datasets/{dataset_id}/variables'
        response = post(post_url , files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 400)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

    def test_create_variable_with_edge_file_missing_P2006020004_fail(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        response = create_dataset(self.url)
        dataset_id = response.json()['dataset_id']
        edges = create_variables_with_edges(self.url, dataset_id, return_edges=True)
        edges = edges[edges.loc[:, 'label']!='P2006020004']
        post_url = f'{self.url}/metadata/datasets/{dataset_id}/variables'
        response = post(post_url , files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 400)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

    def test_create_variable_with_desc_tag(self):
        delete_variable(self.url)
        delete_dataset(self.url)
        expected_response = {
            "name": "unit test variable",
            "variable_id": "unittestvariable",
            "dataset_id": "unittestdataset",
            "description": "A test variable",
            "corresponds_to_property": "Punittestdataset-unittestvariable",
            "tag": [
                "tag1",
                "tag2:True"
            ]
        }

        dataset_id = create_dataset(self.url).json()['dataset_id']
        response = create_variable(self.url, dataset_id, description='A test variable', tag=['tag1', 'tag2:True'])

        self.assertEqual(response.status_code, 201)
        self.assertEqual(expected_response, response.json())
        delete_variable(self.url)
        delete_dataset(self.url)

    def test_create_variable_edges(self):
        delete_variable(self.url)
        delete_dataset(self.url)

        expected_labels = [
            'P31',
            'label',
            'P1476',
            'P2006020003',
            'P2006020004',
            'P1813',
            'P1687'
        ]

        dataset_id = create_dataset(self.url).json()['dataset_id']

        response = create_variable(self.url, dataset_id, return_edges=True)
        df = pd.read_csv(StringIO(response.text), sep='\t')

        for i, row in df.iterrows():
            self.assertTrue(row['label'] in expected_labels)
            if row['label'] == 'P31':
                self.assertEqual(row['node2'], 'Q50701')

            if row['label'] == 'label':
                self.assertEqual(row['node2'], 'unit test variable')

            if row['label'] == 'P1476':
                self.assertEqual(row['node2'], 'unit test variable')

            if row['label'] == 'P1813':
                self.assertEqual(row['node2'], 'unittestvariable')

            if row['label'] == 'P2006020003':
                self.assertEqual(row['node2'], 'Qunittestdataset-unittestvariable')

            if row['label'] == 'P2006020004':
                self.assertEqual(row['node2'], 'Qunittestdataset')

            if row['label'] == 'P1687':
                self.assertEqual(row['node2'], 'Punittestdataset-unittestvariable')

        delete_variable(self.url)
        delete_dataset(self.url)

    def test_create_variable_with_desc_tag_edges(self):
        delete_variable(self.url)
        delete_dataset(self.url)

        dataset_id = create_dataset(self.url).json()['dataset_id']
        response = create_variable(self.url, dataset_id, description='A test variable', tag=['tag1', 'tag2:True'], return_edges=True)
        df = pd.read_csv(StringIO(response.text), sep='\t')

        self.assertTrue((df['label'] == 'description').sum() == 1)
        self.assertTrue(df[df['label'] == 'description']['node2'].iloc[0] == 'A test variable')

        self.assertTrue((df['label'] == 'P2010050001').sum() == 2)
        self.assertTrue(set(df[df['label'] == 'P2010050001']['node2']) == {'tag1', 'tag2:True'})

        delete_variable(self.url)
        delete_dataset(self.url)

    # TODO: Need to update. Edge file upload changed.
    '''
    def test_update_variable_with_edge_file(self):
        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)

        dataset_id = create_dataset(self.url).json()['dataset_id']

        response = create_variables_with_edges(self.url, dataset_id)
        self.assertEqual(response.status_code, 201)

        dataset_qnode = 'Q' + dataset_id
        variable_id = 'variable-unittestdataset-001'

        new_description = 'New description for variable'
        edges = variable_edges(variable_id, dataset_qnode)
        edges.loc[edges[edges['label']=='description'].index, 'node2'] = f'"{new_description}"'

        put_url = f'{self.url}/metadata/datasets/{dataset_id}/variables/{variable_id}'
        response = put(put_url, files={'file': StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['description'], new_description)

        delete_variable(self.url)
        delete_variable(self.url, variable_id='variable-unittestdataset-001')
        delete_variable(self.url, variable_id='variable-unittestdataset-002')
        delete_dataset(self.url)
    '''
