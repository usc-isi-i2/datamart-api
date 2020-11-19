import io, json
from pandas import DataFrame
from utils.spreadsheet import create_annotated_sheet
from requests import post, put

sheet_id = 0

def upload_data_annotated(datamart_api_url: str, file_path: str,
                            fBuffer: io.StringIO=None, put_data: bool=False) -> bool:
    ''' Upload an annotated sheet to Datamart
        Args:
            datamart_api_url: Datamart url
            file_path: If input is a file, this will be the place where the data is located
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

    # Upload the data to Datamart
    if put_data:
        response = put(datamart_api_url, files=files)
    else:
        response = post(datamart_api_url, files=files)

    # Show logs
    print(json.dumps(response.json(), indent=2))

    if response.status_code != 200:
        return False
    return True

def submit_sheet(datamart_api_url: str, annotated_sheet: DataFrame,
                    put_data: bool=False) -> None:
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
    return upload_data_annotated(url, '', buffer, put_data)

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
