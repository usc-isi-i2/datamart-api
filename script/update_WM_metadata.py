import json
import pandas as pd
from requests import put, post, delete, get

datamart_host_url = 'http://localhost:12543'
# datamart_host_url = 'http://dsbox01.isi.edu:12543/'

desc_file = 'WM_allvars_expanded_descriptions_AG_PB_FINAL.csv'

def update_dataset_metadata(datamart_url, dataset_id, name=None, description=None, url=None):
    update = {}
    if name:
        update['name'] = name
    if description:
        update['description'] = description
    if url:
        update['url'] = url
    return put(f'{datamart_url}/metadata/datasets/{dataset_id}', json=update)

def update_variable_metadata(url, dataset_id, variable_id, name=None, description=None, tag=[]):
    update = {}
    if name:
        update['name'] = name
    if description:
        update['description'] = description
    if tag:
        update['tag'] = tag
    return put(f'{url}/metadata/datasets/{dataset_id}/variables/{variable_id}', json=update)

def dataset_metadata_changed(row):
    dataset_id = row['dataset_id']
    if dataset_id not in datasets_metadata:
        return False
    changed = not row['dataset_name'] == datasets_metadata[dataset_id]['name']
    changed |= not row['dataset_description'] == datasets_metadata[dataset_id]['description']
    return changed

def variable_metadata_changed(row):
    dataset_id = row['dataset_id']
    if dataset_id not in variables_metadata:
        return False
    variable_id = row['variable_id']
    changed = not row['variable_name'] == variables_metadata[dataset_id][variable_id]['name']
    changed |= not row['variable_description'] == variables_metadata[dataset_id][variable_id]['description']
    return changed

def get_dataset(url, dataset_id='unittestdataset'):
    return get(f'{url}/metadata/datasets/{dataset_id}')

def update_dataset(row):
    dataset_id = row['dataset_id']
    name = row['dataset_name']
    desc = row['dataset_description']
    # print(f'update_dataset_metadata("{datamart_host_url}", "{dataset_id}", name="{name}", description="{desc}"')
    response = update_dataset_metadata(datamart_host_url, dataset_id, name=name, description=desc)
    return response.status_code==200

def update_variable(row):
    dataset_id = row['dataset_id']
    variable_id = row['variable_id']
    name = row['variable_name']
    desc = row['variable_description']
    # print(f'update_variable_metadata("{datamart_host_url}", "{dataset_id}", "{variable_id}", name="{name}", description="{desc}"')
    response = update_variable_metadata(datamart_host_url, dataset_id, variable_id, name=name, description=desc)
    return response.status_code==200

# Read updated names and descriptions
update_df = pd.read_csv(desc_file, index_col=0)

# Should use this filter rows
# update_df = update_df[update_df['was this row edited by Alli'].isin(['Yes - Dataset description and name', 'Yes', 'Yes '])]

# Get current metadata
datasets_metadata = {}
variables_metadata = {}
dataset_update = update_df[['dataset_id', 'dataset_name', 'dataset_description']].drop_duplicates()
for dataset_id in dataset_update['dataset_id']:
    response = get_dataset(datamart_host_url, dataset_id)
    if response.status_code < 300:
        datasets_metadata[dataset_id] = response.json()[0]
        response = get_variable(datamart_host_url, dataset_id, variable_id=None)
        result_json = response.json()
        variables_metadata[dataset_id] = {x['variable_id']:x for x in result_json}
    else:
        print('!!!Not able to find dataset:', dataset_id)

# Find datasets and variables to update
need_to_update_dataset = dataset_update.apply(dataset_metadata_changed, axis=1)
need_to_update_variable = update_df.apply(variable_metadata_changed, axis=1)

# Do the updates
dataset_result = dataset_update[need_to_update_dataset].apply(update_dataset, axis=1)
print(f'Updated datasets {len(dataset_result)} of {need_to_update_variable.sum()}')
variable_result = update_df[need_to_update_variable].apply(update_variable, axis=1)
print(f'Updated variables {len(variable_result)} of {need_to_update_variable.sum()}')
