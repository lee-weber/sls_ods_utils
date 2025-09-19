from .config import get_api_key, get_ods_url
import requests

def get_uid_by_id(dataset_id: str) -> str:
    '''
    Returns named dataset's uid or None if it wasn't found.
    '''

    payload = {"apikey": get_api_key(), "dataset_id": dataset_id}
    response = requests.get(f"{get_ods_url()}/datasets/", payload)
    try:
        dataset_uid = response.json()["results"][0]["uid"]
    except:
        return None
    return dataset_uid
