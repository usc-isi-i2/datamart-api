import io, os, json
import tempfile, csv, tarfile, shutil
import pandas as pd
from pandas import DataFrame
from utils.spreadsheet import create_annotated_sheet
from requests import post, put
from requests.models import Response
from typing import Tuple, Dict, Optional, NoReturn

'''
This module contains the following functions:
    submit_files() -> submit some files to Datamart with given parameters
    submit_tsv() -> submit exploded kgtk file to Datamart with given parameters
    submit_annotated_sheet() -> submit annotated spreadsheet (csv or xlsx) file to Datamart,
                                  can add optional YAML file and save_tsv path,
                                  can save t2wml output or exploded kgtk files
    submit_annotated_dataframe() -> submit annotated dataframe to Datamart,
                                        support similar interfaces like submit_annotated_sheet()
    submit_sheet_bulk() -> Given template and directory, submit a collection of annotated sheets to Datamart
'''

def submit_files(url:str, files: Dict, params: Dict, auth: Optional[Tuple]=None) -> Response:
    ''' Upload files to Datamart
        Args:
            url: Datamart API url
            files: The files to be uploaded
            params: Parameters of the request, must include key 'put_data'
        Returns:
            The HTTP response from Datamart
    '''

    # Upload the data to Datamart
    put_data = params.pop('put_data')
    if put_data:
        response = put(url, files=files, params=params, auth=auth)
    else:
        response = post(url, files=files, params=params, auth=auth)

    return response

def submit_tar(datamart_url: str, file_path: str, dataset_id: str, mode: str='tsv', params: Dict={},
                put_data: bool=True, verbose: bool=False, auth: Optional[Tuple]=None) -> bool:
    ''' Upload a compressed file to Datamart
        Must supply with dataset_id to upload the tar.gz file
    '''

    params['put_data'] = put_data

    file_name = os.path.basename(file_path)
    with open(file_path, mode='rb') as fd:
        # Supply arguments
        url = f'{datamart_url}/datasets/{dataset_id}/{mode}'
        files = { 'tar': (file_name, fd, 'application/octet-stream') }
        # Send to Datamart
        response = submit_files(url, files, params, auth=auth)

    if not response.status_code in [200, 201, 204]:
        print(response.text)
        return False

    if verbose:
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)
    return True

def submit_tsv(datamart_url: str, file_path: str, put_data: bool=True,
                verbose: bool=False, auth: Optional[Tuple]=None) -> bool:
    ''' Upload a tsv file to Datamart
        Args:
            datamart_url: Datamart base address
            file_path: The file to be uploaded
            put_data: Whether to PUT or POST the data to Datamart
            verbose: Whether to show variable metadata upon submission success
            auth: authentication information
        Returns:
            A boolean values indicates whether the sheet is uploaded successfully
    '''
    def find_dataset_id(file_path: str):
        x = pd.read_csv(file_path, sep='\t').query("label == 'P1813'")
        dataset_id = x[x['node1'].apply(lambda x: not x.startswith('QVARIABLE'))]['node2'].tolist()[0]
        return dataset_id

    if not file_path.endswith('.tsv'):
        print('Error: submit_tsv() does not accept non-tsv files')
        return False

    file_name = os.path.basename(file_path)
    dataset_id = find_dataset_id(file_path)

    with open(file_path, mode='rb') as fd:
        # Supply arguments
        url = f'{datamart_url}/datasets/{dataset_id}/tsv'
        files = { 'file': (file_name, fd, 'application/octet-stream') }

        # Temporary add create_if_not_exist to help Ed upload his files
        params = { 'put_data': put_data, 'create_if_not_exist' : True }
        # Send to Datamart
        response = submit_files(url, files, params, auth=auth)

    if not response.status_code in [200, 201, 204]:
        print(response.text)
        return False

    if verbose:
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)
    return True

def submit_annotated_dir(datamart_url: str, annotated_path: str, yaml_path: Optional[str]=None,
                            put_data: bool=False, verbose: bool=False, save_tsv: Optional[str]=None,
                            save_files: Optional[str]=None, validate: bool=True, auth: Optional[Tuple]=None,
                            wikifier_file: Optional[str]=None, extra_edges_file: Optional[str]=None) -> bool:

    def find_dataset_id(file_path: str):
        x = pd.read_csv(file_path, sep='\t').query("label == 'P1813'")
        dataset_id = x[x['node1'].apply(lambda x: not x.startswith('QVARIABLE'))]['node2'].tolist()[0]
        return dataset_id

    ct = 0

    files = [f for f in os.listdir(annotated_path) if f.endswith('.csv') or f.endswith('.xlsx')]
    td = tempfile.mkdtemp()
    tempdir1 = tempfile.mkdtemp()
    tempdir2 = tempfile.mkdtemp()

    for i, fname in enumerate(files):
        if save_tsv is None:
            save_tsv_p = None
        elif save_tsv.endswith('.tar.gz'):
            save_tsv_p = f'{tempdir1}/{os.path.basename(save_tsv)[:-7]}{i}.tar.gz'
        else:
            save_tsv_p = f'{tempdir1}/{os.path.basename(save_tsv)}{i}.tar.gz'

        if not submit_annotated_sheet(datamart_url, os.path.join(annotated_path, fname), yaml_path, put_data,
                                        verbose, save_tsv_p, save_files, validate, auth,
                                        wikifier_file, extra_edges_file):
            return False, ct

        if not save_tsv_p is None:
            with tarfile.open(save_tsv_p, 'r') as tar:
                
                import os
                
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tar, td)
            fname = [f for f in os.listdir(td)][0]
            # Cut metadata info if not the first one
            if i > 0:
                dataset_id = find_dataset_id(fname)
                df = pd.read_csv(f'{td}/{fname}', sep='\t')
                df = df[df['node1'].apply(lambda x: not x.startswith(f'Q{dataset_id}'))]
                df = df[df['node1'].apply(lambda x: not x.startswith(f'QVARIABLE-Q{dataset_id}'))]
                df = df[df['node1'].apply(lambda x: not x.startswith(f'PVARIABLE-Q{dataset_id}'))]
                df = df[df['node1'].apply(lambda x: not x.startswith(f'QUNIT-Q{dataset_id}'))]
                df.to_csv(f'{td}/{fname}', sep='\t', index=False)

            shutil.copy(f'{td}/{fname}', f'{tempdir2}/{fname[:-4]}{i}.tsv')
        ct += 1

    if not save_tsv is None:
        with tarfile.open(save_tsv, "w:gz") as tar:
            tar.add(tempdir2, arcname='.')

    shutil.rmtree(td)
    shutil.rmtree(tempdir1)
    shutil.rmtree(tempdir2)

    return True, ct


def submit_annotated_sheet(datamart_url: str, annotated_path: str, yaml_path: Optional[str]=None,
                            put_data: bool=False, verbose: bool=False, save_tsv: Optional[str]=None,
                            save_files: Optional[str]=None, validate: bool=True, auth: Optional[Tuple]=None,
                            wikifier_file: Optional[str]=None, extra_edges_file: Optional[str]=None) -> bool:
    ''' Submit an annotated sheet
        Args:
            datamart_url: Datamart base address
            annotated_path: The annotated file path
            yaml_path: The optional yaml file for parsing the dataset
            put_data: Whether to PUT or POST the data to Datamart
            verbose: Whether to show variable metadata upon submission success
            save_tsv: If supplied, the path of the tar.gz file it will be saved
            save_files: If supplied, the path of the tar.gz file to save T2WML configuration files
            auth: authentication information
        Returns:
            A boolean values indicates whether the sheet is uploaded successfully
    '''
    def find_dataset_id(file_path: str):
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, header=None, dtype=object).fillna('')
        else:
            df = pd.read_csv(file_path, header=None, encoding='latin1', dtype=object).fillna('')
        return df.iat[0,1]

    if not (annotated_path.endswith('.xlsx') or annotated_path.endswith('.csv')):
        print(f'Error: Unknown file type - {annotated_path}')
        return False

    file_name = os.path.basename(annotated_path)
    dataset_id = find_dataset_id(annotated_path)

    with open(annotated_path, mode='rb') as fd:
        # Supply arguments
        url = f'{datamart_url}/datasets/{dataset_id}/annotated'

        files = { 'file': (file_name, fd, 'application/octet-stream') }
        if yaml_path:
            files['t2wml_yaml'] = os.path.basename(yaml_path), open(yaml_path, mode='rb'), 'application/octet-stream'
        if wikifier_file:
            files['wikifier'] = os.path.basename(wikifier_file), open(wikifier_file, mode='rb'), 'application/octet-stream'
        if extra_edges_file:
            files['extra_edges'] = os.path.basename(extra_edges_file), open(extra_edges_file, mode='rb'), 'application/octet-stream'

        params = { 'put_data': put_data, 'create_if_not_exist': True,
                    'tsv': bool(save_tsv), 'files_only': bool(save_files),
                    'validate': validate }
        # Send to Datamart
        response = submit_files(url, files, params, auth=auth)
        if yaml_path:
            files['t2wml_yaml'][1].close()

    if not response.status_code in [200, 201, 204]:
        print(response.text)
        return False

    # Generate output
    if save_tsv:
        if not save_tsv.endswith('.tar.gz'):
            save_tsv += '-d.tar.gz'
        with open(save_tsv, 'wb') as fd:
            fd.write(response.content)

    if save_files:
        if not save_files.endswith('.tar.gz'):
            save_files += '-d.tar.gz'
        with open(save_files, 'wb') as fd:
            fd.write(response.content)

    if verbose and not save_tsv:
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print(response.text)
    return True

def submit_annotated_dataframe(datamart_url: str, annotated_df: DataFrame, yaml_path: Optional[str]=None,
                                put_data: bool=False, verbose: bool=False, save_tsv: Optional[str]=None,
                                save_files: Optional[str]=None, validate: bool=True, auth: Optional[Tuple]=None) -> bool:
    ''' Submit an annotated dataframe
        Function signature is exactly the same like submit_annotated_sheet() except the second
    '''
    input_file = tempfile.NamedTemporaryFile(mode='r+', suffix='.csv')
    annotated_df.to_csv(input_file.name, index=False, header=None, quoting=csv.QUOTE_NONE)

    return submit_annotated_sheet(datamart_url, input_file.name, yaml_path, put_data, verbose, save_tsv, save_files, validate, auth=auth)

def submit_sheet_bulk(datamart_api_url: str, template_path: str, dataset_path: str,
                        flag_combine_sheets: bool=False, put_data: bool=False) -> NoReturn:
    ''' Submit multiple annotated sheets to Datamart
        Args:
            datamart_api_url: Datamart url
            template_path: The path where template is stored
            dataset_path: The path where data is stored
            flag_combine_sheets: Whether to combine sheets in different files
                                    or POST them separatedly
        Returns:
            The number of sheets submitted
    '''
    sheets_submitted = 0
    file_counts = 0
    for annotated_sheet, ct in create_annotated_sheet(template_path, dataset_path, flag_combine_sheets):
        file_counts = ct
        if submit_annotated_dataframe(datamart_api_url, annotated_sheet, put_data=put_data):
            sheets_submitted += 1
    return file_counts, sheets_submitted
