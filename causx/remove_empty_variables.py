'''
Fix Causx database. Remove varaiables with no data.
'''
from pprint import pprint

from requests import delete, get

URL = 'http://localhost:12543'


dataset_response = get(f'{URL}/metadata/datasets')

if dataset_response.status_code > 300:
    print('dataset query:', dataset_response)
    pprint(dataset_response.json())
    raise Exception()

empty = []
variables = []
for dataset_metadata in dataset_response.json():
    dataset_id = dataset_metadata['dataset_id']
    print('Processing:', dataset_id)
    variable_response = get(f'{URL}/metadata/datasets/{dataset_id}/variables')
    if variable_response.status_code >= 300:
        print('  variable query:', variable_response)
        pprint(variable_response.text)
        print('  skipping')
        continue
    for variable_metadata in variable_response.json():
        variable_id = variable_metadata['variable_id']
        data_response = get(f'{URL}/datasets/{dataset_id}/variables/{variable_id}')
        if data_response.status_code >= 300:
            print('  data query:', data_response)
            pprint(data_response.text)
            continue
        lines = data_response.text.split('\n')
        if len(lines) == 1 or (len(lines) == 2 and lines[1] == ''):
            empty.append((dataset_id, variable_id))
        else:
            variables.append((dataset_id, variable_id))

print(f'Deleting {len(empty)} variables')
for (dataset_id, variable_id) in empty:
    response = delete(f'{URL}/metadata/datasets/{dataset_id}/variables/{variable_id}')
    if response.status_code >= 300:
        print(f'Failed to delete {dataset_id} {variable_id}')
