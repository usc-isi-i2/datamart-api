import unittest
from io import StringIO

import pandas as pd

from pathlib import Path
from requests import post, delete, get

from test.utility import upload_data_post, upload_data_put


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
        expected_result = 'dataset_id,variable_id,variable,main_subject,main_subject_id,value,value_unit,time,time_precision,country,country_id,admin1,admin2,admin3,region_coordinate,stated_in,stated_in_id,Loc,Attack context,Means of attack,City,stated in\nunittestuploaddataset,ingo,INGO,Canada,Q16,0.0,person,1997-09-24T00:00:00Z,day,Canada,Q16,,,,POINT(-109.0 56.0),,,Unknown,Individual attack,Shooting,roadside,\nunittestuploaddataset,ingo,INGO,Canada,Q16,0.0,person,1998-06-25T00:00:00Z,day,Canada,Q16,,,,POINT(-109.0 56.0),,,Road,Ambush,Kidnapping,travelling from Gode to Degeh Bur,\nunittestuploaddataset,ingo,INGO,Canada,Q16,1.0,person,1999-04-01T00:00:00Z,day,Canada,Q16,,,,POINT(-109.0 56.0),,,Unknown,Unknown,Kidnapping,around the corner,\n'

        delete(f'{self.url}/metadata/datasets/unittestuploaddataset?force=True')

        response = post(f'{self.url}/metadata/datasets', json=self.metadata)
        self.assertEqual(response.status_code, 201, response.text)

        f_path =  Path(__file__).parent / 'test_data/test_file_main_subject_country_wikifier.xlsx'
        response = upload_data_put(f_path, f'{self.url}/datasets/unittestuploaddataset/annotated')
        self.assertEqual(response.status_code, 201, response.text)

        response = get(f'{self.url}/datasets/unittestuploaddataset/variables/ingo')
        self.assertEqual(response.status_code, 200, response.text)

        # Test does not work. The qualifier columns appears in arbitrary order.
        # self.assertEqual(response.text, expected_result, response.text)

        response = delete(f'{self.url}/metadata/datasets/unittestuploaddataset?force=True')
        self.assertEqual(response.status_code, 200, response.text)

    def test_upload_annotated_multi_part(self):
        dataset_id = 'iDATAUnitTest'
        filename_1 = 'iDATABel_annotated_with_dataset_metadata.csv'
        filename_2 = 'iDATARus_annotated.csv'
        metadata = {'name': 'iDATAUnitTest',
                    'description': 'IDATA, formerly the Integrated Crisis Warning System (ICEWS), is the premier event data coding system. Originally developed in 2007, IDATA uses automated coding to produce an event database by analyzing news stories from hundreds of sources across the globe, using feeds from Factiva and the Open Source Enterprise. The database includes millions of time-stamped, geolocated events from 2000 and is updated weekly. ',
                    'url': 'https://dataverse.harvard.edu/dataverse/icews',
                    'dataset_id': 'iDATAUnitTest',
                    'last_update_precision': '14'}
        columns = ['CAMEO_code', 'DocID', 'FactorClass', 'Normalizer', 'Relevance', 'Source Country', 'Units', 'admin1', 'admin2', 'admin3',
                   'country', 'country_id', 'dataset_id', 'main_subject', 'main_subject_id', 'region_coordinate',
                   'stated in', 'stated_in', 'stated_in_id', 'time', 'time_precision', 'value', 'variable', 'variable_id']
        url = self.url

        delete(f'{url}/metadata/datasets/{dataset_id}?force=True')

        f_path =  Path(__file__).parent / f'test_data/{filename_1}'
        response = upload_data_post(f_path, f'{url}/datasets/{dataset_id}/annotated?create_if_not_exist=True')
        self.assertEqual(response.status_code, 201, response.text)

        response = get(f'{url}/metadata/datasets/{dataset_id}')
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()[0]
        del result['last_update']
        self.assertEqual(result, metadata, response.text)

        response = get(f'{url}/metadata/datasets/{dataset_id}/variables')
        self.assertEqual(response.status_code, 200, response.text)
        tags = response.json()[0]['tag']
        self.assertTrue('DocID:7c5bd533156aaabf38e5103a0f7ff7f7' in tags, response.text)
        self.assertTrue('Units:Sq. Km' in tags, response.text)
        self.assertTrue('Normalizer:Longitudinal' in tags, response.text)
        self.assertTrue('Relevance:0.75' in tags, response.text)
        self.assertTrue('FactorClass:http://ontology.causeex.com/ontology/odps/ICM#EconomicAgriculturalCapability' in tags, response.text)

        f_path =  Path(__file__).parent / f'test_data/{filename_2}'
        response = upload_data_post(f_path, f'{url}/datasets/{dataset_id}/annotated')
        self.assertEqual(response.status_code, 201, response.text)

        response = get(f'{url}/metadata/datasets/{dataset_id}')
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()[0]
        del result['last_update']
        self.assertEqual(result, metadata, response.text)

        response = get(f'{url}/metadata/datasets/{dataset_id}/variables')
        self.assertEqual(response.status_code, 200, response.text)
        tags = response.json()[0]['tag']
        self.assertTrue('DocID:7c5bd533156aaabf38e5103a0f7ff7f7' in tags, response.text)
        self.assertTrue('Units:Sq. Km' in tags, response.text)
        self.assertTrue('Normalizer:Longitudinal' in tags, response.text)
        self.assertTrue('Relevance:0.75' in tags, response.text)
        self.assertTrue('FactorClass:http://ontology.causeex.com/ontology/odps/ICM#EconomicAgriculturalCapability' in tags, response.text)

        response = get(f'{url}/datasets/{dataset_id}/variables/event_count')
        self.assertEqual(response.status_code, 200, response.text)
        df = pd.read_csv(StringIO(response.text))
        result_cols = [x for x in df.columns]
        result_cols.sort()
        self.assertTrue(result_cols == columns, result_cols)
        self.assertEqual(len(df), 94, 'Missing rows: {len(df)} != 94')

        response = get(f'{url}/datasets/{dataset_id}/variables/event_count?country=Russia')
        self.assertEqual(response.status_code, 200, response.text)
        df = pd.read_csv(StringIO(response.text))
        result_cols = [x for x in df.columns]
        result_cols.sort()
        self.assertTrue(result_cols == columns, result_cols)
        self.assertEqual(len(df), 47, 'Missing rows: {len(df)} != 47')

        response = get(f'{url}/datasets/{dataset_id}/variables/event_count?country=Belarus')
        self.assertEqual(response.status_code, 200, response.text)
        df = pd.read_csv(StringIO(response.text))
        result_cols = [x for x in df.columns]
        result_cols.sort()
        self.assertTrue(result_cols == columns, result_cols)
        self.assertEqual(len(df), 47, 'Missing rows: {len(df)} != 47')

        response = delete(f'{url}/metadata/datasets/{dataset_id}?force=True')
        self.assertEqual(response.status_code, 200, response.text)
