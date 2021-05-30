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
))

entity_one_edges = pd.DataFrame(
    [['QUnitTestEntityOne-P31', 'QUnitTestEntityOne', 'P31', 'Q18616576'],
     ['QUnitTestEntityOne-label', 'QUnitTestEntityOne', 'label', '"TestEntityOne"']],
    columns=['id', 'node1', 'label', 'node2']
)

entity_two_edges = pd.DataFrame(
    [['QUnitTestEntityTwo-P31', 'QUnitTestEntityTwo', 'P31', 'Q18616576'],
     ['QUnitTestEntityTwo-label', 'QUnitTestEntityTwo', 'label', '"TestEntityTwo"']],
    columns=['id', 'node1', 'label', 'node2']
)

data = pd.DataFrame(
    [['QUnitTestEntityOne-Q100', 'Q100', 'owl:sameAs', 'QUnitTestEntityOne']],
    columns=['id', 'node1', 'label', 'node2']
)

class TestEntityAPI(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'
        self.entity_one_name = 'QUnitTestEntityOne'
        self.entity_two_name = 'QUnitTestEntityTwo'
        self.entity_one_label = 'TestEntityOne'
        self.entity_two_label = 'TestEntityTwo'
        self.delete_edges()

    def tearDown(self):
        self.delete_edges()

    def delete_edges(self):
        # delete entities
        utils.delete(f"delete from edges where node1 = '{self.entity_one_name}'", config=config)
        utils.delete(f"delete from edges where node1 = '{self.entity_two_name}'", config=config)
        # delete data edges
        utils.delete(f"delete from edges where node2 = '{self.entity_one_name}'", config=config)
        utils.delete(f"delete from edges where node2 = '{self.entity_two_name}'", config=config)

    def test_get_undefined_entity_by_name(self):
        url = f'{self.url}/entities/{self.entity_one_name}'
        response = get(url)
        self.assertEqual(response.status_code, 204, response.text)

    def test_get_undefined_entity_by_label(self):
        url = f'{self.url}/entities?label={self.entity_one_label}'
        response = get(url)
        self.assertEqual(response.status_code, 204, response.text)

    def test_put_get(self):
        url = f'{self.url}/entities/{self.entity_one_name}'
        put_response = put(url, data=entity_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 201, put_response.text)

        get_response = get(f'{self.url}/entities/{self.entity_one_name}')
        self.assertEqual(get_response.status_code, 200, get_response.text)

        df = pd.read_csv(io.StringIO(get_response.text), sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')
        df = df.sort_values(['label']).reset_index(drop=True)

        self.assertTrue((df == entity_one_edges).all().all())

    def test_put_fail_1(self):
        url = f'{self.url}/entities'
        put_response = put(url, data=entity_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, put_response.text)

    def test_put_fail_2(self):
        url = f'{self.url}/entities/qxyz'
        put_response = put(url, data=entity_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, put_response.text)

    def test_put_fail_3(self):
        url = f'{self.url}/entities/Qxyz'
        put_response = put(url, data=entity_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, put_response.text)

    def test_put_fail_4(self):
        url = f'{self.url}/entities/{self.entity_one_name}'
        all_edges = pd.DataFrame.append(entity_one_edges, entity_two_edges).reset_index(drop=True)
        put_response = put(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, put_response.text)

    def test_put_illegal_label(self):
        illegal = entity_one_edges.append(
            pd.DataFrame(
                [['QUnitTestEntityOne-extra', 'QUnitTestEntityOne', 'extra', 123]],
                columns=['id', 'node1', 'label', 'node2']))
        url = f'{self.url}/entities/{self.entity_one_name}'
        put_response = put(url, data=illegal.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 400, 'Should not accept labels not in white list')

    def test_post(self):
        url = f'{self.url}/entities'
        all_edges = pd.DataFrame.append(entity_one_edges, entity_two_edges).reset_index(drop=True)
        post_response = post(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 201 , post_response.text)

        part = {}
        for entity in [self.entity_one_name, self.entity_two_name]:
            url = f'{self.url}/entities/{entity}'
            get_response = get(url)
            self.assertEqual(get_response.status_code, 200, get_response.text)

            part[entity] = pd.read_csv(io.StringIO(get_response.text), sep='\t', quoting=csv.QUOTE_NONE, dtype=object).fillna('')


        df = pd.DataFrame.append(part[self.entity_one_name], part[self.entity_two_name])
        df = df.sort_values(['node1', 'label']).reset_index(drop=True)
        all_edges = all_edges.sort_values(['node1', 'label']).reset_index(drop=True)

        self.assertTrue((df == all_edges).all().all())

    def test_post_illegal_label(self):
        illegal = entity_one_edges.append(
            pd.DataFrame(
                [['QUnitTestEntityOne-extra', 'QUnitTestEntityOne', 'extra', 123]],
                columns=['id', 'node1', 'label', 'node2']))
        url = f'{self.url}/entities'
        post_response = post(url, data=illegal.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 400, 'Should not accept labels not in white list')

    def test_post_fail(self):
        url = f'{self.url}/entities/Qxyz'
        all_edges = pd.DataFrame.append(entity_one_edges, entity_two_edges).reset_index(drop=True)
        post_response = post(url, data=all_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(post_response.status_code, 400 , f'put({url})')

    def test_delete(self):
        url = f'{self.url}/entities/{self.entity_one_name}'
        put_response = put(url, data=entity_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        # self.assertEqual(put_response.status_code, 201, f'put({url})')
        self.assertEqual(put_response.status_code, 201, put_response.text)

        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 200, delete_response.text)

    def test_delete_fail_1(self):
        url = f'{self.url}/entities'
        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 400, f'put({url})')

    def test_delete_fail_2(self):
        url = f'{self.url}/entities/qxyz'
        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 400, f'put({url})')

    def test_delete_fail_3(self):
        url = f'{self.url}/entities/{self.entity_one_name}'
        put_response = put(url, data=entity_one_edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))
        self.assertEqual(put_response.status_code, 201, f'put({url})')

        import_kgtk_dataframe(data, config=config)

        url = f'{self.url}/properties/{self.entity_one_name}'
        delete_response = delete(url)
        self.assertEqual(delete_response.status_code, 400, 'Cannot delete entity if in use')

if __name__ == '__main__':
    unittest.main()
