from .config import get_ods_url, get_api_key
from .get_uid_by_id import get_uid_by_id

import requests
import datetime

def create_dataset(
    new_id: str = None,
    desc: str = None,
    is_restricted: bool = None
) -> str:
    '''
    Creates a dataset with on the ods server defined in .env
    
    Parameters:
        new_id (str):
            ID and title of the new dataset

        desc (str):
            Description of the new dataset

        is_restricted (bool):
            whether or not the dataset should be set as restricted

    Returns:
        The uid of the successfully created dataset or `None` if unsuccessful
    '''
    if new_id is None:
        exit("Please provide a name (`new_id`).")
    if desc is None:
        desc = "Bitte Beschreibung hinzufügen."
    if is_restricted is None:
        is_restricted = False

    # Check if name is already taken
    id_taken_by = get_uid_by_id(new_id)
    if id_taken_by:
        print(f"id \"{new_id}\" already taken by dataset {id_taken_by}.")
        return None

    # Construct post data
    headers = {
            "Authorization": f"apikey {get_api_key()}",
            "Content-Type": "application/json"
    }
    api_url = get_ods_url() + "/datasets"
    modified = datetime.datetime.now()
    modified_str = modified.strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "dataset_id": new_id,
        "is_restricted": is_restricted,
        "metadata": {
            "default": {
                "title": {"value": new_id},
                "description": {"value": desc},
                "modified": {"value": modified_str},
                "language": {"value": "de"}
            }
        }
    }

    # Send request
    response = requests.post(api_url, headers=headers, json=payload)

    print(f'posting:\n{payload}\nto {api_url}\nwith:\n{headers}')

    if response.status_code not in {200,201}:
        print("Failure.")
        print(response.status_code)
        print(response.text)
        return None

    print("Success.")
    
    # Return uid of new dataset
    # TODO: Make more efficient by parsing response text
    return get_uid_by_id(new_id)
        
