import os
import json
import time

from io import StringIO

from requests import get, put, delete, post
import pandas as pd

datamart_api_url = 'http://localhost:12543'
# datamart_api_url = 'http://localhost:5000'
# datamart_api_url = 'http://dsbox02.isi.edu:14080'


file_paths = [
    'test_data/FSI_all_Annotated.xlsx',
    'test_data/FSI_extra_column.xlsx',
    'test_data/FSI_4_qualifiers.xlsx',
]

dataset_ids = [
    'FSIall_AN',
    'FSIall_AN_extra',
    'FSIall_AN_4Q',
]
# test_dataset = {
#     'name': dataset_id,
#     'dataset_id': dataset_id,
#     'description': f"Test dataset {dataset_id}",
#     'url': f"https://{dataset_id}.org/"
# }

class Timer:
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.start = time.perf_counter()
    def __exit__(self, exc_type, exc_value, exc_tab):
        self.stop = time.perf_counter()
        print(f'{self.name} took {self.stop-self.start:.2f} seconds')

def upload_data_post(file_path, url, json_result=True):
    file_name = os.path.basename(file_path)
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    response = post(url, files=files)
    if response.status_code == 400:
        print(json.dumps(response.json(), indent=2))
    elif json_result:
        try:
            print(response.json())
        except:
            print(response.text)
    return response

for dataset_id, file_path in zip(dataset_ids, file_paths):
    print(dataset_id, file_path)
    test_dataset = {
        'name': dataset_id,
        'dataset_id': dataset_id,
        'description': f"Test dataset {dataset_id}",
        'url': f"https://{dataset_id}.org/"
    }
    with Timer(f'create_dataset {dataset_id}'):
        td_response = post(f'{datamart_api_url}/metadata/datasets', json=test_dataset)
        print(td_response.text)

    with Timer(f'post_dataset {dataset_id}'):
        url = f'{datamart_api_url}/datasets/{dataset_id}/annotated'
        post_response = upload_data_post(file_path, url)
        print(post_response.text)

all_variables = {}
for dataset_id in dataset_ids:
    with Timer(f'get_all_variables {dataset_id}'):
        url = f'{datamart_api_url}/datasets/{dataset_id}/variables'
        get_all_response = get(url)
        print(f'status: {get_all_response.status_code}')

    all_variables[dataset_id] = pd.read_csv(StringIO(get_all_response.text))

#all_variables_df = pd.read_csv(StringIO(get_all_response.text))


for dataset_id in dataset_ids:
    variable_id = "x1_external_intervention"
    with Timer(f'get_variable china {variable_id} {dataset_id}'):
        response = get(f'{datamart_api_url}/datasets/{dataset_id}/variables/{variable_id}?country=china')
        print(response.text)

    variable_id = "p2_public_services"
    with Timer(f'get_variable china {variable_id} {dataset_id}'):
        response = get(f'{datamart_api_url}/datasets/{dataset_id}/variables/{variable_id}?country=china')
        print(response.text)

    with Timer(f"get_variable Cote d\\'Ivoire {variable_id} {dataset_id}"):
        response = get(f"{datamart_api_url}/datasets/{dataset_id}/variables/{variable_id}?country=Cote d\\'Ivoire")
        print(response.text)

    with Timer(f"get_variable Congo Republic {variable_id} {dataset_id}"):
        variable_id = "x1_external_intervention"
        response = get(f"{datamart_api_url}/datasets/{dataset_id}/variables/{variable_id}?country=Congo Republic")
        print(response.text)
