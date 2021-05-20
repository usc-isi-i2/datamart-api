import datetime
import time
import unittest
import pandas as pd

from io import StringIO
from operator import itemgetter
from pathlib import Path

from test.utility import create_variable, create_dataset, delete_variable, delete_variable_data, delete_dataset, \
    get_dataset, get_variable, get_data, update_variable_metadata, upload_data_put, update_dataset_metadata


class TestUpdateMetadata(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'

    def test_update_dataset(self):
        delete_variable(self.url)
        delete_dataset(self.url)

        dataset_id = 'unittestdataset'
        metadata = {
            'name': 'Updated Unit test dataset',
            'description': 'Updated test descrption',
            'url': 'http://unittest201.org'
        }

        dataset_id = create_dataset(self.url, dataset_id=dataset_id).json()['dataset_id']
        response = update_dataset_metadata(self.url, **metadata)
        self.assertTrue(response.status_code==200, response.text)
        result_metadata = response.json()
        for key, value in metadata.items():
            self.assertTrue(
                result_metadata[key]==value,
                'Expected result_metadata[{key}]=={value}, but got {result_metadata[key]}')

        delete_variable(self.url)
        delete_dataset(self.url)

    def test_update_variable_no_change(self):
        delete_variable(self.url)
        delete_dataset(self.url)

        description='A test variable'
        dataset_id = create_dataset(self.url).json()['dataset_id']
        _ = create_variable(self.url, dataset_id, description=description, tag=['tag1', 'tag2:True'])

        get_var_response = get_variable(self.url)
        self.assertTrue(get_var_response.status_code==200)

        response = update_variable_metadata(self.url, description=description)
        self.assertTrue(response.status_code==200)
        self.assertTrue(get_var_response.json()==response.json())

        delete_variable(self.url)
        delete_dataset(self.url)

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
        self.assertTrue(response.status_code == 200)
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
        self.assertTrue(response.status_code == 200)
        metadata = response.json()

        self.assertTrue('tag2:False' in metadata['tag'])
        self.assertTrue('tag3' in metadata['tag'])
        self.assertTrue('tag1' not in metadata['tag'])
        self.assertTrue(metadata['name']=='unit test variable')
        self.assertTrue(metadata['description']=='A test variable')

        delete_variable(self.url)
        delete_dataset(self.url)

    # 2021-05-20: Remove test for now
    def _test_update_variable_name(self):
        dataset_id = 'unittestuploaddataset'
        variable_id = 'ingo_changed'
        result = delete_variable_data(self.url, dataset_id=dataset_id, variable_id=variable_id)
        # print('delete_variable_data', result.status_code, result.text)
        result = delete_variable(self.url, dataset_id=dataset_id, variable_id=variable_id)
        # print('delete_variable', result.status_code, result.text)
        variable_id = 'ingo'
        result = delete_variable_data(self.url, dataset_id=dataset_id, variable_id=variable_id)
        # print('delete_variable_data', result.status_code, result.text)
        result = delete_variable(self.url, dataset_id=dataset_id, variable_id=variable_id)
        # print('delete_variable', result.status_code, result.text)
        result = delete_dataset(self.url, dataset_id=dataset_id)
        # print('delete_dataset', result.status_code, result.text)

        expected_metadata = {
            'corresponds_to_property': 'PVARIABLE-Qunittestuploaddataset-005',
            'dataset_id': 'unittestuploaddataset',
            'description': 'variable column in Qunittestuploaddataset',
            'name': 'INGO',
            'qualifier': [{'data_type': 'String',
                           'identifier': 'PQUALIFIER-Qunittestuploaddataset-007',
                           'name': 'Attack context'},
                          {'data_type': 'String',
                           'identifier': 'PQUALIFIER-Qunittestuploaddataset-004',
                           'name': 'City'},
                          {'data_type': 'String',
                           'identifier': 'PQUALIFIER-Qunittestuploaddataset-008',
                           'name': 'LOCATION_CHANGED'},
                          {'data_type': 'String',
                           'identifier': 'PQUALIFIER-Qunittestuploaddataset-006',
                           'name': 'Means of attack'},
                          {'data_type': 'String',
                           'identifier': 'PQUALIFIER-Qunittestuploaddataset-003',
                           'name': 'Region'},
                          {'identifier': 'P131',
                           'name': 'located in the administrative territorial entity'},
                          {'identifier': 'P585', 'name': 'point in time'},
                          {'identifier': 'P248', 'name': 'stated in'}],
            'variable_id': 'ingo'
        }
        expected_metadata['qualifier'].sort(key=itemgetter('name'))

        response = create_dataset(self.url, dataset_id=dataset_id)
        self.assertEqual(response.status_code, 201, response.text)

        # f_path = 'test/test_data/test_file_main_subject_country_simple_1.csv'
        f_path = Path(__file__).parent / 'test_data/test_file_main_subject_country_simple_1.csv'
        udp = upload_data_put(f_path, f'{self.url}/datasets/unittestuploaddataset/annotated')
        self.assertTrue(udp.status_code==201, udp.text)

        rjson = udp.json()[0]
        rjson['qualifier'].sort(key=itemgetter('name'))
        self.assertTrue(rjson == expected_metadata, rjson)

        new_name = 'International NGO'
        uvm = update_variable_metadata(self.url, dataset_id = dataset_id, variable_id = variable_id, name=new_name)
        updated_metadata = uvm.json()
        self.assertTrue(updated_metadata['name']=='International NGO')

        updated_metadata.pop('name')
        expected_metadata.pop('name')
        self.assertTrue(expected_metadata==expected_metadata)

        response = get_data(self.url, dataset_id=dataset_id, variable_id=variable_id)
        self.assertEqual(response.status_code, 200, response.text)

        data = pd.read_csv(StringIO(response.text))
        self.assertEqual(data.loc[0, 'variable'], new_name, f"{data.loc[0, 'variable']} == {new_name}")

        delete_variable_data(self.url, dataset_id=dataset_id, variable_id=variable_id)
        delete_variable(self.url, dataset_id=dataset_id, variable_id=variable_id)
        delete_dataset(self.url, dataset_id=dataset_id)
