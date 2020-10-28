import os
import json
from requests import get,post,put,delete

def generate_targets(datasets_path: str) -> [str]:
    patterns = []
    if os.path.isfile(datasets_path):
        patterns.append(datasets_path)
    else:
        if datasets_path.endswith('/'):
            datasets_path = datasets_path[:-1]
        patterns.append(datasets_path + '/*.csv')
        patterns.append(datasets_path + '/*.xlsx')
    return patterns

def erase_dataset(datamart_api_url: str, dataset_id: str) -> None:
    response = delete(f'{datamart_api_url}/metadata/datasets/{dataset_id}?force=true')
    if response.status_code == 400:
        print(json.dumps(response.json(), indent=2))

def upload_data_annotated(file_path: str, url: str, put_data: bool=True) -> None:
    file_name = os.path.basename(file_path)
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    if put_data:
        response = put(url, files=files)
    else:
        response = post(url, files=files)
    if response.status_code == 400:
        print(json.dumps(response.json(), indent=2))
    else:
        print(json.dumps(response.json(), indent=2))

def upload_frame_annotated(buffer: str, url: str, put_data: bool=True) -> None:

    buffer.seek(0)

    files = {
        'file': ('buffer.csv', buffer, 'application/octet-stream')
    }

    if put_data:
        response = put(url, files=files)
    else:
        response = post(url, files=files)
    if response.status_code == 400:
        print(json.dumps(response.json(), indent=2))
