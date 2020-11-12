import unittest
import pandas as pd
from io import StringIO
from test.utility import create_variable, delete_variable, delete_dataset


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

        response = create_variable(self.url)

        self.assertEqual(response.status_code, 201)
        self.assertEqual(expected_response, response.json())
        delete_variable(self.url)
        delete_dataset(self.url)

    def test_create_variable_edges(self):
        delete_variable(self.url)
        delete_dataset(self.url)
        expected_response = {
            "name": "unit test variable",
            "variable_id": "unittestvariable",
            "dataset_id": "unittestdataset",
            "corresponds_to_property": "Punittestdataset-unittestvariable"
        }

        response = create_variable(self.url, return_edges=True)
        df = pd.read_csv(StringIO(response.text), sep='\t')
        print(df)

        # self.assertEqual(response.status_code, 201)
        # self.assertEqual(expected_response, response.json())
        delete_variable(self.url)
        delete_dataset(self.url)
