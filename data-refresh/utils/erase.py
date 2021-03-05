import json
from requests import get, delete
from utils.get_ import metadata
from typing import NoReturn, Tuple, Optional

def erase_dataset(datamart_api_url: str, dataset_id: str, auth: Optional[Tuple]=None) -> NoReturn:
    ''' Erase a dataset from Datamart
        Args:
            datamart_api_url: url of the Datamart server
            dataset_id: the ID of the dataset
            auth: username as password
        Returns:
            No Return
        Note:
            Log will print out to indicate the result of the delete request
    '''

    # Remove a dataset
    response = delete(f'{datamart_api_url}/metadata/datasets/{dataset_id}?force=true', auth=auth)

    # Show logs
    print(response.text)

def erase_all(datamart_api_url: str, auth: Optional[Tuple]=None) -> NoReturn:
    ''' Erase all datasets '''

    df = metadata(datamart_api_url, auth)
    try:
        for dataset_id in df['dataset_id']:
            erase_dataset(datamart_api_url, dataset_id, auth)
    except Exception as e:
        print('Error:', e)
