# This script keeps just the CausX variables specified in a spreadshet

import argparse
import requests
import xlrd

def parse_args():
    parser = argparse.ArgumentParser(description="Remove unnecessary variables and datasets")
    parser.add_argument('filename', type=str, help="Excel spreadsheet with filename")
    parser.add_argument('--datamart-api', type=str, default='http://localhost:12543', help="Datamart API URL")
    parser.add_argument('--keep-empty-datasets', action="store_true", default=False, help="Do not delete empty datasets")
    parser.add_argument('--dry-run', action='store_true', default=False, help="Do not actually delete variables and datasets")

    return parser.parse_args()

def process_spreadsheet(filename):
    # Returns a map from dataset to list of variables
    wb = xlrd.open_workbook(filename)
    sheet = wb.sheet_by_index(0)

    var_links = []
    for row_idx in range(1, sheet.nrows):
        cell = sheet.cell(row_idx, 10)
        var_links.append(cell.value)

    variables = {}
    for link in var_links:
        parts = link.split('/')
        if parts[-2] != 'variables' or parts[-4] != 'datasets':
            raise ValueError("Can't parse variable link " + link)
        dataset, variable = parts[-3], parts[-1]
        if dataset not in variables:
            variables[dataset] = []
        if variable not in variables[dataset]:
            variables[dataset].append(variable)

    return variables

datamart_url = None
def store_datamart_url(url):
    global datamart_url
    datamart_url = url
    if datamart_url[:7] != 'http://' and datamart_url[:8] != 'https://':
        datamart_url = 'http://' + datamart_url
    if datamart_url[-1] == '/':
        datamart_url = datamart_url[:-1]

def datamart_request(url, delete=False):
    global datamart_url
    url = datamart_url + url

    if not delete:
        response = requests.get(url)
    else:
        response = requests.delete(url)
    if response.status_code > 299 or response.status_code < 200:
        raise ValueError("Bad response for request to " + url)
    return response.json()

def get_datamart_datasets():
    response = datamart_request('/metadata/datasets')
    datasets = [d['dataset_id'] for d in response]
    return datasets

def get_datamart_variables(dataset):
    response = datamart_request(f'/metadata/datasets/{dataset}/variables')
    variables = [d['variable_id'] for d in response]
    return variables

def add_extra_datasets(variables):
    # Add the datasets that are not listed in the CausX spreadsheet, with no variables,
    # so all of their variables are deleted
    datamart_datasets = get_datamart_datasets()

    for dataset in variables.keys():
        if dataset not in datamart_datasets:
            raise ValueError(f"Dataset {dataset} is expected in CausX, but not in datamart")
    
    for dataset in datamart_datasets:
        if dataset not in variables:
            variables[dataset] = []

def clean_dataset(dataset, causx_variables, dry_run):
    datamart_variables = get_datamart_variables(dataset)
    causx_variables_lower = [v.lower() for v in causx_variables]
    # First, make sure all causx_variables are in the dataset
    for variable in causx_variables:
        if variable.lower() not in [dv.lower() for dv in datamart_variables]:
            raise ValueError(f"In dataset {dataset}, variable {variable} is expected in CausX, but is not found in dataset")

    for variable in datamart_variables:
        if variable.lower() not in causx_variables_lower:
            print(f'Deleting {variable} from {dataset}')
            if not dry_run:
                datamart_request(f'/datasets/{dataset}/variables/{variable}', delete=True)
                datamart_request(f'/metadata/datasets/{dataset}/variables/{variable}', delete=True)

    
def remove_unused_datasets(variables, dry_run):
    for dataset, dataset_variables in variables.items():
        if not dataset_variables:
            print(f'Deleting unused dataset {dataset}')
            if not dry_run:
                datamart_request(f'/metadata/datasets/{dataset}', delete=True)

def run():
    global datamart_url

    args = parse_args()
    store_datamart_url(args.datamart_api)

    variables = process_spreadsheet(args.filename)
    add_extra_datasets(variables)
    for dataset in variables.keys():
        clean_dataset(dataset, variables[dataset], args.dry_run)

    if not args.keep_empty_datasets:
        remove_unused_datasets(variables, args.dry_run)
    
    print("Done")

if __name__ == '__main__':
    run()