'''
Fix Causx database. WDI dataset has duplicate variables that ends with "_1".
'''
from requests import delete, get

URL = 'http://localhost:12543'
DATASET_ID = 'WDI'

delete_vars = []
other = []
variable_response = get(f'{URL}/metadata/datasets/{DATASET_ID}/variables')
for variable_metadata in variable_response.json():
    variable_id = variable_metadata['variable_id']
    if variable_id.endswith('_1'):
        delete_vars.append(variable_id)
    else:
        other.append(variable_id)

print(f'Deleting {len(delete_vars)} variables')
for variable_id in delete_vars:
    response = delete(f'{URL}/datasets/{DATASET_ID}/variables/{variable_id}')
    if response.status_code >= 300:
        print(f'Failed to delete data {DATASET_ID} {variable_id}')
    else:
        response = delete(f'{URL}/metadata/datasets/{DATASET_ID}/variables/{variable_id}')
        if response.status_code >= 300:
            print(f'Failed to delete metadata {DATASET_ID} {variable_id}')
