import io
import csv
import os
import typing

import pandas as pd

from requests import put
from requests import post, delete, get


def upload_data_put(file_path, url):
    file_name = os.path.basename(file_path)
    with open(file_path, mode='rb') as fd:
        files = {
            'file': (file_name, fd, 'application/octet-stream')
        }
        result = put(url, files=files)
    return result


def create_dataset(p_url, return_edges=False, name='Unit Test Dataset', dataset_id='unittestdataset',
                   description='will be deleted in this unit test', url='http://unittest101.org'):
    metadata = {
        'name': name,
        'dataset_id': dataset_id,
        'description': description,
        'url': url
    }
    if return_edges:
        post_url = f'{p_url}/metadata/datasets?tsv=true'
    else:
        post_url = f'{p_url}/metadata/datasets'

    return post(post_url, json=metadata)


def create_dataset_with_edges(
        p_url, name='Unit Test Dataset', dataset_id='unittestdataset',
        description='will be deleted in this unit test', url='http://unittest101.org',
        extra_edges=[], delete_labels=[]):
    qnode = 'Q' + dataset_id
    edge_list = []
    for label, node2 in [('P31', 'Q1172284'),
                         ('label', f'"{name}"'),
                         ('P1476', f'"{name}"'),
                         ('description', f'"{description}"'),
                         ('P2699', f'"{url}"'),
                         ('P1813', f'"{dataset_id}"'),
                         ('P5017', '^2021-03-05T10:14:11/14')] + extra_edges:
        if label not in delete_labels:
            edge_list.append([qnode, label, node2, f'{qnode}-{label}'])
    edges = pd.DataFrame(edge_list, columns=['node1', 'label', 'node2', 'id'])
    post_url = f'{p_url}/metadata/datasets'

    return post(post_url, files={'file': io.StringIO(edges.to_csv(sep='\t', quoting=csv.QUOTE_NONE, index=False))})


def delete_dataset(url, dataset_id='unittestdataset'):
    return delete(f'{url}/metadata/datasets/{dataset_id}')


def get_dataset(url, dataset_id='unittestdataset'):
    return get(f'{url}/metadata/datasets/{dataset_id}')


def create_variable(p_url, dataset_id, variable_id='unittestvariable', name='unit test variable',
                    description: str = '', tag: typing.List[str] = [], return_edges=False):
    metadata = {
        'name': name,
        'variable_id': variable_id
    }

    if description:
        metadata['description'] = description
    if tag:
        metadata['tag'] = tag

    if return_edges:
        post_url = f'{p_url}/metadata/datasets/{dataset_id}/variables?tsv=true'
    else:
        post_url = f'{p_url}/metadata/datasets/{dataset_id}/variables'
    return post(post_url, json=metadata)


def get_variable(p_url, dataset_id='unittestdataset', variable_id='unittestvariable'):
    if variable_id is None:
        return get(f'{p_url}/metadata/datasets/{dataset_id}/variables')
    else:
        return get(f'{p_url}/metadata/datasets/{dataset_id}/variables/{variable_id}')


def delete_variable(url, dataset_id='unittestdataset', variable_id='unittestvariable'):
    return delete(f'{url}/metadata/datasets/{dataset_id}/variables/{variable_id}')

def delete_variable_data(url, dataset_id='unittestdataset', variable_id='unittestvariable'):
    return delete(f'{url}/datasets/{dataset_id}/variables/{variable_id}')

def update_variable_metadata(url, dataset_id='unittestdataset', variable_id='unittestvariable', name=None, description=None, tag=[]):
    update = {}
    if name:
        update['name'] = name
    if description:
        update['description'] = description
    if tag:
        update['tag'] = tag
    return put(f'{url}/metadata/datasets/{dataset_id}/variables/{variable_id}', json=update)

def update_dataset_metadata(datamart_url, dataset_id='unittestdataset', name=None, description=None, url=None):
    update = {}
    if name:
        update['name'] = name
    if description:
        update['description'] = description
    if url:
        update['url'] = url
    return put(f'{datamart_url}/metadata/datasets/{dataset_id}', json=update)

def get_data(datamart_url, dataset_id='unittestdataset', variable_id='unittestvariable'):
    url = f'{datamart_url}/datasets/{dataset_id}/variables/{variable_id}'
    return get(url)
