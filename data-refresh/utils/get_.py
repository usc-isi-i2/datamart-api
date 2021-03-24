import json, io
import pandas as pd
from requests import get
from pandas import DataFrame
from typing import NoReturn, Dict, Tuple, Optional, Union

def metadata(datamart_api_url: str, auth: Optional[Tuple]=None) -> Union[DataFrame,None]:

    response = get(f'{datamart_api_url}/metadata/datasets', auth=auth)

    if response.status_code != 200:
        print(response.text)
        return None

    return pd.DataFrame(response.json())

def variable_metadata(datamart_api_url: str, dataset_id: str, auth: Optional[Tuple]=None) -> Union[DataFrame,None]:

    response = get(f'{datamart_api_url}/metadata/datasets/{dataset_id}/variables', auth=auth)

    if response.status_code != 200:
        print(response.text)
        return None

    return pd.DataFrame(response.json())

def variable_data(datamart_api_url: str, dataset_id: str, variable_id,
                    auth: Optional[Tuple]=None) -> Union[DataFrame,None]:

    response = get(f'{datamart_api_url}/datasets/{dataset_id}/variables/{variable_id}', auth=auth)

    if response.status_code != 200:
        print(response.text)
        return None

    return pd.read_csv(io.StringIO(response.text))
