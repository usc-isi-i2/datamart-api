import unittest
from requests import get
import pandas as pd
from io import StringIO
from test.utility import create_dataset, create_dataset_with_edges, delete_dataset


class TestCreateDataset(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'
        self.expected_response = {'name': 'Unit Test Dataset',
                                  'description': 'will be deleted in this unit test',
                                  'url': 'http://unittest101.org',
                                  'dataset_id': 'unittestdataset'}

    def test_create_dataset_blank_description_url(self):

        expected_response = [{'error': 'Metadata field: description, cannot be blank'},
                             {'error': 'Metadata field: url, cannot be blank'}]

        response = create_dataset(self.url, description='', url='')

        self.assertEqual(response.status_code, 400)
        self.assertEqual(expected_response, response.json())

    def test_create_dataset_blank_all_fields(self):

        expected_response = [{'error': 'Metadata field: name, cannot be blank'},
                             {'error': 'Metadata field: dataset_id, cannot be blank'},
                             {'error': 'Metadata field: description, cannot be blank'},
                             {'error': 'Metadata field: url, cannot be blank'}]

        response = create_dataset(self.url, name='', description='', url='', dataset_id='')

        self.assertEqual(response.status_code, 400)
        self.assertEqual(expected_response, response.json())

    def test_create_dataset_3(self):
        delete_dataset(self.url)

        response = create_dataset(self.url)

        self.assertEqual(response.status_code, 201)

        response = get(f'{self.url}/metadata/datasets').json()

        d_metadata = None
        for x in response:
            if x['dataset_id'] == self.expected_response['dataset_id']:
                d_metadata = x
        self.assertTrue('name' in d_metadata)
        self.assertTrue('description' in d_metadata)
        self.assertTrue('dataset_id' in d_metadata)
        self.assertTrue('url' in d_metadata)
        self.assertTrue('last_update_precision' in d_metadata)
        self.assertTrue('last_update' in d_metadata)
        self.assertEqual(self.expected_response['dataset_id'], d_metadata['dataset_id'])
        self.assertEqual(self.expected_response['name'], d_metadata['name'])
        self.assertEqual(self.expected_response['url'], d_metadata['url'])
        self.assertEqual(self.expected_response['description'], d_metadata['description'])

        # delete the dataset for future runs
        delete_dataset(self.url)

    def test_create_dataset_edges(self):
        delete_dataset(self.url)

        r = create_dataset(self.url, return_edges=True)
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

            # test the actual response from API as well
            response = get(f'{self.url}/metadata/datasets').json()

            d_metadata = None
            for x in response:
                if x['dataset_id'] == self.expected_response['dataset_id']:
                    d_metadata = x
            self.assertTrue('name' in d_metadata)
            self.assertTrue('description' in d_metadata)
            self.assertTrue('dataset_id' in d_metadata)
            self.assertTrue('url' in d_metadata)
            self.assertTrue('last_update_precision' in d_metadata)
            self.assertTrue('last_update' in d_metadata)
            self.assertEqual(self.expected_response['dataset_id'], d_metadata['dataset_id'])
            self.assertEqual(self.expected_response['name'], d_metadata['name'])
            self.assertEqual(self.expected_response['url'], d_metadata['url'])
            self.assertEqual(self.expected_response['description'], d_metadata['description'])

        # delete the dataset for future runs
        delete_dataset(self.url)

    def test_create_dataset_with_edges(self):

        delete_dataset(self.url)

        response = create_dataset_with_edges(self.url)

        self.assertEqual(response.status_code, 201, response.text)

        response = get(f'{self.url}/metadata/datasets').json()

        d_metadata = None
        for x in response:
            if x['dataset_id'] == self.expected_response['dataset_id']:
                d_metadata = x
        self.assertTrue('name' in d_metadata)
        self.assertTrue('description' in d_metadata)
        self.assertTrue('dataset_id' in d_metadata)
        self.assertTrue('url' in d_metadata)
        self.assertTrue('last_update_precision' in d_metadata)
        self.assertTrue('last_update' in d_metadata)
        self.assertEqual(self.expected_response['dataset_id'], d_metadata['dataset_id'])
        self.assertEqual(self.expected_response['name'], d_metadata['name'])
        self.assertEqual(self.expected_response['url'], d_metadata['url'])
        self.assertEqual(self.expected_response['description'], d_metadata['description'])

        # delete the dataset for future runs
        delete_dataset(self.url)

    def test_create_dataset_with_edges_fail_1(self):

        delete_dataset(self.url)

        response = create_dataset_with_edges(self.url, delete_labels=['P31'])

        self.assertEqual(response.status_code, 400)

        result = response.json()
        self.assertTrue('Error' in result)
        self.assertTrue('Missing required properties' in result['Error'])
        self.assertTrue('P31' in result['Missing_Required_Properties'])


    def test_create_dataset_with_edges_fail_2(self):

        delete_dataset(self.url)

        response = create_dataset_with_edges(self.url, extra_edges=[('P131', 'Q115')])

        self.assertEqual(response.status_code, 400)

        result = response.json()
        self.assertTrue('Error' in result)
        self.assertTrue('Properties not recognized' in result['Error'])
        self.assertTrue('P131' in result['Properties_Not_Recognized'])
