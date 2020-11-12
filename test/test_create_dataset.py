import unittest
from requests import post, delete, get
import pandas as pd
from io import StringIO


class TestCreateDataset(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'

    def test_create_dataset_1(self):
        metadata = {
            "name": "failure dataset",
            "dataset_id": "setup_for_failure",
            "description": "",
            "url": ""
        }
        expected_response = [{'error': 'Metadata field: description, cannot be blank'},
                             {'error': 'Metadata field: url, cannot be blank'}]

        response = post(f'{self.url}/metadata/datasets', json=metadata)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(expected_response, response.json())

    def test_create_dataset_2(self):
        metadata = {
            "name": "",
            "dataset_id": "",
            "description": "",
            "url": ""
        }
        expected_response = [{'error': 'Metadata field: name, cannot be blank'},
                             {'error': 'Metadata field: dataset_id, cannot be blank'},
                             {'error': 'Metadata field: description, cannot be blank'},
                             {'error': 'Metadata field: url, cannot be blank'}]

        response = post(f'{self.url}/metadata/datasets', json=metadata)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(expected_response, response.json())

    def test_create_dataset_3(self):
        metadata = {
            "name": "UnitTestDataset",
            "dataset_id": "unittestdataset",
            "description": "will be deleted in this unit test",
            "url": "http://unittest101.org"
        }
        expected_response = {'name': 'UnitTestDataset', 'description': 'will be deleted in this unit test',
                             'url': 'http://unittest101.org', 'dataset_id': 'unittestdataset'}

        response = post(f'{self.url}/metadata/datasets', json=metadata)

        self.assertEqual(response.status_code, 201)
        self.assertEqual(expected_response, response.json())

        response = get(f'{self.url}/metadata/datasets').json()
        d_metadata = None
        for x in response:
            if x['dataset_id'] == metadata['dataset_id']:
                d_metadata = x

        self.assertEqual(metadata, d_metadata)

        # delete the dataset for future runs
        delete(f'{self.url}/metadata/datasets/unittestdataset')

    def test_create_dataset_edges(self):
        delete(f'{self.url}/metadata/datasets/unittestdataset')
        metadata = {
            "name": "Unit Test Dataset",
            "dataset_id": "unittestdataset",
            "description": "will be deleted in this unit test",
            "url": "http://unittest101.org"
        }

        r = post(f'{self.url}/metadata/datasets?tsv=true', json=metadata)
        expected_labels = [
            'P31',
            'label',
            'P1476',
            'description',
            'P2699',
            'P1813',
            'P5017'
        ]

        df = pd.read_csv(StringIO(r.text), sep='\t')
        self.assertTrue(len(df) == 7)
        for i, row in df.iterrows():
            self.assertEqual(row['node1'], 'Qunittestdataset')

            self.assertTrue(row['label'] in expected_labels)

            if row['label'] == 'P31':
                self.assertEqual(row['node2'], 'Q1172284')

            if row['label'] == 'label':
                self.assertEqual(row['node2'], 'Unit Test Dataset')

            if row['label'] == 'P1476':
                self.assertEqual(row['node2'], 'Unit Test Dataset')

            if row['label'] == 'description':
                self.assertEqual(row['node2'], 'will be deleted in this unit test')

            if row['label'] == 'P2699':
                self.assertEqual(row['node2'], 'http://unittest101.org')

            if row['label'] == 'P1813':
                self.assertEqual(row['node2'], 'unittestdataset')

        # delete the dataset for future runs
        delete(f'{self.url}/metadata/datasets/unittestdataset')
