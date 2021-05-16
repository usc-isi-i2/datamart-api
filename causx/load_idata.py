'''
Load STR's iDATA dataset
'''
import os

from pathlib import Path
from requests import post, delete

URL = 'http://localhost:12543'
DATA_DIR =  Path('/lfs1/ktyao/Shared/kgtk-private-data/Causx/STR/iDATA')

def upload_data_post(file_path, url):
    '''Post file'''
    file_name = os.path.basename(file_path)
    with open(file_path, mode='rb') as fd:
        files = {
            'file': (file_name, fd, 'application/octet-stream')
        }
        result = post(url, files=files)
    return result

# delete existing dataset
delete(f'{URL}/datasets/iDATA/variables/event_count')
delete(f'{URL}/metadata/datasets/iDATA/variables/event_count')
delete(f'{URL}/metadata/datasets/iDATA')

# Load data file with dataset metadata
for f in DATA_DIR.glob('*with_dataset_metadata.xlsx'):
    print('Loading', f)
    r0 = upload_data_post(f, f'{URL}/datasets/iDATA/annotated?create_if_not_exist=true')
    print(r0)
    break  # Only one of this file

# Load other data files
for f in DATA_DIR.glob('*annotated.xlsx'):
    print('Loading', f)
    r1 = upload_data_post(f, f'{URL}/datasets/iDATA/annotated')
    print(r1)
