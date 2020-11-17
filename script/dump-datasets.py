
import argparse
import os
import sys
import tarfile
import requests


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('output', type=str, help='Output directory for dump')
    parser.add_argument('--datamart-api', type=str, default='http://localhost:12543', help="Datamart API URL")
    parser.add_argument('--package', type=str, default=None, help='Package dump into one tar.gz file specified by this argument')

    return parser.parse_args()

datamart_url = None
def store_datamart_url(url):
    global datamart_url
    datamart_url = url
    if datamart_url[:7] != 'http://' and datamart_url[:8] != 'https://':
        datamart_url = 'http://' + datamart_url
    if datamart_url[-1] == '/':
        datamart_url = datamart_url[:-1]

def datamart_request(url, delete=False, json=True):
    global datamart_url
    url = datamart_url + url

    if not delete:
        response = requests.get(url)
    else:
        response = requests.delete(url)
    if response.status_code > 299 or response.status_code < 200:
        raise ValueError("Bad response for request to " + url)

    if not json:
        return response.text
    return response.json()

def get_datamart_datasets():
    response = datamart_request('/metadata/datasets')
    datasets = [d['dataset_id'] for d in response]
    return datasets

def get_dataset_variables(dataset):
    response = datamart_request(f'/datasets/{dataset}/variables?limit=1000000', json=False)
    return response

def dump_variables(dataset, dir):
    print('Generating dump for ', dataset)
    dump = get_dataset_variables(dataset)
    dump_path = os.path.join(dir, f'{dataset}.csv')
    with open(dump_path, 'w', encoding='utf-8') as of:
        print(dump, file=of)

def package_dump(dump_dir, tar_gz_path):
    print(f'Packaging dump into {tar_gz_path}')
    # tar-gzip file in folders, based on https://stackoverflow.com/a/17081026/871910
    with tarfile.open(tar_gz_path, "w:gz") as tar:
        tar.add(dump_dir, arcname='datamart-dump')

def run():
    args = parse_args()
    store_datamart_url(args.datamart_api)

    try:
        os.makedirs(args.output, exist_ok=True)
    except:
        print("Can't create output directory ", args.output, file=sys.stderr)
        return

    datasets = get_datamart_datasets()
    print(f'Dumping data from {len(datasets)} datasets')

    for dataset in datasets:
        dump_variables(dataset, args.output)

    if args.package:
        package_dump(args.output, args.package)

    print('Done')

if __name__ == '__main__':
    run()