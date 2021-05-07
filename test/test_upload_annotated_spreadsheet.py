import unittest
from pathlib import Path
from requests import post, delete
from test.utility import upload_data_put


class TestUploadSpreadsheet(unittest.TestCase):
    def setUp(self):
        self.url = 'http://localhost:12543'
        self.metadata = {
            "name": "UnitTestUploadDataset",
            "dataset_id": "unittestuploaddataset",
            "description": "create variables and then delete them",
            "url": "http://unittest102.org"
        }

    def test_upload_annotated_spreadsheet_1(self):
        f_path =  Path(__file__).parent / 'test_data/test_file_main_subject_country.xlsx'
        post(f'{self.url}/metadata/datasets', json=self.metadata)
        response = upload_data_put(f_path, f'{self.url}/datasets/unittestuploaddataset/annotated')
        self.assertEqual(response.status_code, 201)
        delete(f'{self.url}/metadata/datasets/unittestuploaddataset')
