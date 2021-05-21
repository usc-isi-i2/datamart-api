import unittest
from pathlib import Path
from requests import post, delete, get
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
        expected_result = 'dataset_id,variable_id,variable,main_subject,main_subject_id,value,value_unit,time,time_precision,country,country_id,admin1,admin2,admin3,region_coordinate,stated_in,stated_in_id,stated in,City,Means of attack,Attack context,Loc\nunittestuploaddataset,ingo,INGO,,Q16,0.0,person,1997-09-24T00:00:00Z,day,,,,,,,,,,roadside,Shooting,Individual attack,Unknown\nunittestuploaddataset,ingo,INGO,,Q16,0.0,person,1998-06-25T00:00:00Z,day,,,,,,,,,,travelling from Gode to Degeh Bur,Kidnapping,Ambush,Road\nunittestuploaddataset,ingo,INGO,,Q16,1.0,person,1999-04-01T00:00:00Z,day,,,,,,,,,,around the corner,Kidnapping,Unknown,Unknown\n'

        delete(f'{self.url}/metadata/datasets/unittestuploaddataset?force=True')

        response = post(f'{self.url}/metadata/datasets', json=self.metadata)
        self.assertEqual(response.status_code, 201, response.text)

        f_path =  Path(__file__).parent / 'test_data/test_file_main_subject_country_wikifier.xlsx'
        response = upload_data_put(f_path, f'{self.url}/datasets/unittestuploaddataset/annotated')
        self.assertEqual(response.status_code, 201, response.text)

        response = get(f'{self.url}/datasets/unittestuploaddataset/variables/ingo')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.text, expected_result, response.text)

        response = delete(f'{self.url}/metadata/datasets/unittestuploaddataset?force=True')
        self.assertEqual(response.status_code, 200, response.text)
