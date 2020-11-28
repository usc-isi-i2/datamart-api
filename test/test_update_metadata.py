import datetime
import time
import unittest
import pandas as pd
from io import StringIO
from test.utility import create_variable, create_dataset, delete_variable, delete_dataset, get_dataset, update_variable_metadata

class TestUpdateMetadata(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'

    def test_update_variable_description(self):
        delete_variable(self.url)
        delete_dataset(self.url)

        dataset_id = create_dataset(self.url).json()['dataset_id']
        _ = create_variable(self.url, dataset_id, description='A test variable', tag=['tag1', 'tag2:True'])

        create_json = get_dataset(self.url).json()[0]
        self.assertTrue('last_update' in create_json)
        create_time = datetime.datetime.fromisoformat(create_json['last_update'])

        time.sleep(1)

        response = update_variable_metadata(self.url, description='A new description')
        self.assertTrue(response.status_code == 201)
        metadata = response.json()

        self.assertTrue(metadata['description']=='A new description')
        self.assertTrue(metadata['name']=='unit test variable')
        self.assertTrue(set(metadata['tag'])==set(['tag1', 'tag2:True']))

        update_json = get_dataset(self.url).json()[0]
        self.assertTrue('last_update' in update_json)
        update_time = datetime.datetime.fromisoformat(update_json['last_update'])

        self.assertTrue(create_time < update_time)


        delete_variable(self.url)
        delete_dataset(self.url)

    def test_update_variable_tag(self):
        delete_variable(self.url)
        delete_dataset(self.url)

        dataset_id = create_dataset(self.url).json()['dataset_id']
        _ = create_variable(self.url, dataset_id, description='A test variable', tag=['tag1', 'tag2:True'])

        create_json = get_dataset(self.url).json()[0]
        self.assertTrue('last_update' in create_json)
        create_time = datetime.datetime.fromisoformat(create_json['last_update'])

        response = update_variable_metadata(self.url, tag=['tag2:False', 'tag3'])
        self.assertTrue(response.status_code == 201)
        metadata = response.json()

        self.assertTrue('tag2:False' in metadata['tag'])
        self.assertTrue('tag3' in metadata['tag'])
        self.assertTrue('tag1' not in metadata['tag'])
        self.assertTrue(metadata['name']=='unit test variable')
        self.assertTrue(metadata['description']=='A test variable')

        delete_variable(self.url)
        delete_dataset(self.url)
