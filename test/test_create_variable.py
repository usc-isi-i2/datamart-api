import unittest
import pandas as pd
from io import StringIO
from test.utility import create_variable, create_dataset, delete_variable, delete_dataset


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
