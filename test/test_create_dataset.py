import unittest
from requests import post, delete, get


class TestCreateDataset(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:5000'

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
