import io, os, json
import pandas as pd
from pandas import DataFrame
from utils.spreadsheet import create_annotated_sheet
from requests import post, put
from requests.models import Response
from typing import Dict, Optional

'''
This module contains the following functions:
    submit_files() -> submit some files to Datamart with given parameters
    submit_tsv() -> submit exploded kgtk file to Datamart with given parameters
    submit_annotated_sheet() -> submit annotated spreadsheet (csv or xlsx) file to Datamart,
                                  can add optional YAML file and save_tsv path,
                                  can save t2wml output or exploded kgtk files

'''

sheet_id = 0

def submit_files(url:str, files: Dict, params: Dict) -> Response:
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
        response = put(url, files=files, params=params)
    else:
        response = post(url, files=files, params=params)

    return response

def submit_tsv(datamart_url: str, file_path: str, put_data: Optional[bool] = True,
                verbose: Optional[bool] = False) -> bool:
    ''' Upload a tsv file to Datamart
        Args:
            datamart_url: Datamart base address
            file_path: The file to be uploaded
            put_data: Whether to PUT or POST the data to Datamart
            verbose: Whether to show variable metadata upon submission success
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

    # Supply arguments
    url = f'{datamart_url}/datasets/{dataset_id}/tsv'
    files = { 'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream') }
    params = { 'put_data': put_data }

    # Send to Datamart
    response = submit_files(url, files, params)

    if not response.status_code in [200, 201, 204]:
        print(json.dumps(response.json(), indent=2))
        return False

    if verbose:
        print(json.dumps(response.json(), indent=2))
    return True

def submit_annotated_sheet(datamart_url: str, annotated_path: str, yaml_path: Optional[str]=None,
                            put_data: Optional[bool]=False, verbose: Optional[bool] = False,
                            save_tsv: Optional[str]=None, save_files: Optional[str]=None) -> bool:
    ''' Submit an annotated sheet
        Args:
            datamart_url: Datamart base address
            annotated_path: The annotated file path
            yaml_path: The optional yaml file for parsing the dataset
            put_data: Whether to PUT or POST the data to Datamart
            verbose: Whether to show variable metadata upon submission success
            save_tsv: If supplied, the path of the tar.gz file it will be saved
            save_files: If supplied, the path of the tar.gz file to save T2WML configuration files
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

    # Supply arguments
    url = f'{datamart_url}/datasets/{dataset_id}/annotated'

    files = { 'file': (file_name, open(annotated_path, mode='rb'), 'application/octet-stream') }
    if yaml_path:
        files['t2wml_yaml'] = os.path.basename(yaml_path), open(yaml_path, mode='rb'), 'application/octet-stream'

    params = { 'put_data': put_data, 'create_if_not_exist': True,
                'tsv': bool(save_tsv), 'files_only': bool(save_files) }

    # Send to Datamart
    response = submit_files(url, files, params)

    if not response.status_code in [200, 201, 204]:
        print(json.dumps(response.json(), indent=2))
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

    if verbose:
        print(json.dumps(response.json(), indent=2))
    return True

def upload_data_annotated(url: str, file_path: str, yamlfile_path: str=None,
                            fBuffer: io.StringIO=None, put_data: bool=False) -> bool:
    ''' Upload an annotated sheet to Datamart
        Args:
            datamart_api_url: Datamart API url
            file_path: If input is a file, this will be the place where the data is located
            yamlfile_path: If user supplies a yaml file, it would be uploaded to Datamart
            fBuffer: If input is buffer, this will be the serialized annotated sheet
            put_data: Whether to PUT or POST the data to Datamart
        Returns:
            A boolean values indicates whether the sheet is uploaded successfully
    '''

    global sheet_id

    # Prepare data, comply with the PUT/POST API of *request*
    if fBuffer is None:
        file_name = os.path.basename(file_path)
        files = { 'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream') }
    else:
        sheet_id += 1
        file_name = 'buffer' + str(sheet_id) + '.csv'
        fBuffer.seek(0)
        files = { 'file': (file_name, fBuffer, 'application/octet-stream') }

    if yamlfile_path:
        files['t2wml_yaml'] = (os.path.basename(yamlfile_path), open(yamlfile_path, mode='rb'), 'application/octet-stream')

    # Upload the data to Datamart
    if put_data:
        response = put(url, files=files)
    else:
        response = post(url, files=files)

    # Show logs
    print(json.dumps(response.json(), indent=2))

    if response.status_code != 201:
        return False
    return True

def submit_sheet(datamart_api_url: str, annotated_sheet: DataFrame,
                    put_data: bool=False, tsv: bool=False) -> bool:
    ''' Submit an annotated sheet to Datamart
        Args:
            datamart_api_url: Datamart url
            annotated_sheet: The annotated sheet as pd.DataFrame
            put_data: Whether to PUT or POST the data to Datamart
        Returns:
            A boolean values indicates whether the sheet is uploaded successfully
    '''
    buffer = io.StringIO()
    dataset_id = annotated_sheet.iat[0,1]

    annotated_sheet.to_csv(buffer, index=False, header=False)
    url = f'{datamart_api_url}/datasets/{dataset_id}/annotated?create_if_not_exist=true'
    if tsv:
        url += '&tsv=true'
    return upload_data_annotated(url, '', None, buffer, put_data)

def submit_sheet_bulk(datamart_api_url: str, template_path: str, dataset_path: str,
                        flag_combine_sheets: bool=False) -> None:
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
        if submit_sheet(datamart_api_url, annotated_sheet):
            sheets_submitted += 1
    return file_counts, sheets_submitted
