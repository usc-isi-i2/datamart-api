import io, os, json
import pandas as pd
from pandas import DataFrame
from utils.spreadsheet import create_annotated_sheet
from requests import post, put
from requests.models import Response
from typing import Dict, Optional

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

    url = f'{datamart_url}/datasets/{dataset_id}/tsv'
    files = { 'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream') }
    params = { 'put_data': put_data }

    response = submit_files(url, files, params)

    if response.status_code in [200, 201, 204]:
        if verbose:
            print(json.dumps(response.json(), indent=2))
        return True

    print(json.dumps(response.json(), indent=2))
    return False

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

def submit_annotated_sheet(datamart_api_url: str, annotated_sheet: str, yamlfile_path: str=None,
                            put_data: bool=False, tsv: bool=False) -> bool:
    ''' Submit an annotated sheet
        Args:
            datamart_api_url: Datamart url
            annotated_sheet: The annotated sheet path
        Returns:
            A boolean values indicates whether the sheet is uploaded successfully
    '''

    if annotated_sheet.endswith('.xlsx'):
        df = pd.read_excel(annotated_sheet, header=None, dtype=object).fillna('')
    elif annotated_sheet.endswith('.csv'):
        df = pd.read_csv(annotated_sheet, header=None, encoding='latin1', dtype=object).fillna('')
    else:
        print(f'Unknown file type: {annotated_sheet}')
        return False

    dataset_id = df.iat[0,1]
    url = f'{datamart_api_url}/datasets/{dataset_id}/annotated?create_if_not_exist=true'
    if tsv:
        url += '&tsv=true'

    return upload_data_annotated(url, annotated_sheet, yamlfile_path, None, put_data)

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
