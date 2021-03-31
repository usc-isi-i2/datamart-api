import json
from requests import delete

def erase_dataset(datamart_api_url: str, dataset_id: str) -> None:
    ''' Erase a dataset from Datamart
        Args:
            datamart_api_url: url of the Datamart server
            dataset_id: the ID of the dataset
        Returns:
            None
        Note:
            Log will print out to indicate the result of the delete request
    '''

    # Remove a dataset
    response = delete(f'{datamart_api_url}/metadata/datasets/{dataset_id}?force=true')

    # Show logs
    print(json.dumps(response.json(), indent=2))
