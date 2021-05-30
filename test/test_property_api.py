import csv
import io
import unittest

import pandas as pd
import db.sql.utils as utils
from db.sql.kgtk import import_kgtk_dataframe

from requests import get, put, post, delete

config = dict(DB = dict(
    database = 'wikidata',
    host = 'localhost',
    port = 5433,
    user = 'postgres',
    password = 'postgres',
), STORAGE_BACKEND='postgres')

property_one_edges = pd.DataFrame(
    [['PUnitTestPropertyOne-P31', 'PUnitTestPropertyOne', 'P31', 'Q18616576'],
     ['PUnitTestPropertyOne-data_type', 'PUnitTestPropertyOne', 'data_type', 'Quantity'],
     ['PUnitTestPropertyOne-label', 'PUnitTestPropertyOne', 'label', '"TestPropertyOne"']],
    columns=['id', 'node1', 'label', 'node2']
)

property_two_edges = pd.DataFrame(
    [['PUnitTestPropertyTwo-P31', 'PUnitTestPropertyTwo', 'P31', 'Q18616576'],
     ['PUnitTestPropertyTwo-data_type', 'PUnitTestPropertyTwo', 'data_type', 'Quantity'],
     ['PUnitTestPropertyTwo-label', 'PUnitTestPropertyTwo', 'label', '"TestPropertyTwo"']],
    columns=['id', 'node1', 'label', 'node2']
)

redefined_property_one_edges = pd.DataFrame(
    [['PUnitTestPropertyOne-P31', 'PUnitTestPropertyOne', 'P31', 'Q18616576'],
     ['PUnitTestPropertyOne-data_type', 'PUnitTestPropertyOne', 'data_type', 'Quantity'],
     ['PUnitTestPropertyOne-label', 'PUnitTestPropertyOne', 'label', '"TestPropertyOne-redefined"']],
    columns=['id', 'node1', 'label', 'node2']
)



data = pd.DataFrame(
    [['PUnitTestPropertyOne-Q100', 'Q100', 'PUnitTestPropertyOne', 123.45]],
    columns=['id', 'node1', 'label', 'node2']
)

class TestPropertyAPI(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'
        self.property_one_name = 'PUnitTestPropertyOne'
        self.property_two_name = 'PUnitTestPropertyTwo'
        self.delete_edges()

    def tearDown(self):
        self.delete_edges()

    def delete_edges(self):
        # delete data edges
        utils.delete(f"delete from edges where label = '{self.property_one_name}'", config=config)
        utils.delete(f"delete from edges where label = '{self.property_two_name}'", config=config)
        # delete property
        utils.delete(f"delete from edges where node1 = '{self.property_one_name}'", config=config)
        utils.delete(f"delete from edges where node1 = '{self.property_two_name}'", config=config)


    def test_get_undefined_property(self):
        url = f'{self.url}/properties/{self.property_one_name}'
        response = get(url)
        self.assertEqual(response.status_code, 204, 'get_undefined_property: ' + url)

    def test_put_get(self):
        url = f'{self.url}/properties/{self.property_one_name}'
        put_response = put(url, data=property_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 201, put_response.text)

        get_response = get(url)
        self.assertEqual(get_response.status_code, 200, get_response.text)

        df = pd.read_csv(io.StringIO(get_response.text), sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')
        df = df.sort_values(['label']).reset_index(drop=True)

        self.assertTrue((df == property_one_edges).all().all())

    def test_put_fail_1(self):
        url = f'{self.url}/properties'
        put_response = put(url, data=property_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, f'put({url})')

    def test_put_fail_2(self):
        url = f'{self.url}/properties/pxyz'
        put_response = put(url, data=property_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, f'put({url})')

    def test_put_fail_3(self):
        url = f'{self.url}/properties/Pxyz'
        put_response = put(url, data=property_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, f'put({url})')

    def test_put_fail_4(self):
        url = f'{self.url}/properties/{self.property_one_name}'
        all_edges = pd.DataFrame.append(property_one_edges, property_two_edges).reset_index(drop=True)
        put_response = put(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, f'put({url})')

    def test_put_illegal_label(self):
        illegal = property_one_edges.append(
            pd.DataFrame(
                [['PUnitTestPropertyOne-extra', 'PUnitTestPropertyOne', 'extra', 123]],
                columns=['id', 'node1', 'label', 'node2']))
        url = f'{self.url}/properties/{self.property_one_name}'
        put_response = put(url, data=illegal.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, 'Should not accept labels not in white list')

    def test_post(self):
        url = f'{self.url}/properties'
        all_edges = pd.DataFrame.append(property_one_edges, property_two_edges).reset_index(drop=True)
        post_response = post(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 201 , post_response.text)

        part = {}
        for prop in [self.property_one_name, self.property_two_name]:
            url = f'{self.url}/properties/{prop}'
            get_response = get(url)
            self.assertEqual(get_response.status_code, 200, get_response.text)

            part[prop] = pd.read_csv(io.StringIO(get_response.text), sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')


        df = pd.DataFrame.append(part[self.property_one_name], part[self.property_two_name])
        df = df.sort_values(['node1', 'label']).reset_index(drop=True)
        all_edges = all_edges.sort_values(['node1', 'label']).reset_index(drop=True)

        self.assertTrue((df == all_edges).all().all())

    def test_post_illegal_label(self):
        illegal = property_one_edges.append(
            pd.DataFrame(
                [['PUnitTestPropertyOne-extra', 'PUnitTestPropertyOne', 'extra', 123]],
                columns=['id', 'node1', 'label', 'node2']))
        url = f'{self.url}/properties'
        post_response = post(url, data=illegal.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 400, 'Should not accept labels not in white list')

    def test_post_fail(self):
        url = f'{self.url}/properties/Pxyz'
        all_edges = pd.DataFrame.append(property_one_edges, property_two_edges).reset_index(drop=True)
        post_response = post(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 400 , f'post({url})')

    def test_post_fail_2(self):
        url = f'{self.url}/properties'
        all_edges = pd.DataFrame.append(property_one_edges, property_two_edges).reset_index(drop=True)
        post_response = post(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 201 , post_response.text)

        url = f'{self.url}/properties'
        all_edges = pd.DataFrame.append(redefined_property_one_edges, property_two_edges).reset_index(drop=True)
        post_response = post(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 409 , f'post({url})')

    def test_delete(self):
        url = f'{self.url}/properties/{self.property_one_name}'
        put_response = put(url, data=property_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 201, put_response.text)

        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 200, delete_response.text)

    def test_delete_fail_1(self):
        url = f'{self.url}/properties'
        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 400, f'delete({url})')

    def test_delete_fail_2(self):
        url = f'{self.url}/properties/pxyz'
        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 400, f'delete({url})')

    def test_delete_fail_3(self):
        url = f'{self.url}/properties/{self.property_one_name}'
        put_response = put(url, data=property_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 201, put_response.text)

        import_kgtk_dataframe(data, config=config)

        url = f'{self.url}/properties/{self.property_one_name}'
        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 400, 'Cannot delete property if in use')
