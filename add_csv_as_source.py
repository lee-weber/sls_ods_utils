from .get_uid_by_id import get_uid_by_id
from .config import get_ods_url, get_api_key

import requests
from typing import Dict, IO
from ftplib import FTP

def add_csv_as_source(
        dataset_id: str = None,
        dataset_uid: str = None,
        file_obj: IO[str] = None,
        remote_filename: str = None,
        ftp_config: Dict[str, str] = None
) -> None:
    """
    Used to add a .csv file as a source to a dataset.
    
    Either `dataset_id` or `dataset_uid` can be used to specify the dataset.
    
    Args:
        dataset_id (str, optional):
            The unique integer identifier of the dataset.

        dataset_uid (str, optional):
            The unique string identifier (UID) of the dataset.

        file_obj (IO[str]):
            The file to be uploaded. Example usage:
                df = pd.DataFrame(...)
                buffer = BytesIO()
                df.to_csv(buffer, index=False)
                add_csv_as_source(..., file_obj=buffer, ...) 

        remote_filename (str):
            The target filename (including path, if needed)
        
        ftp_config (Dict[str,str]):
            A dictionary containing FTP connection parameters:
                - "host": FTP server hostname or IP address
                - "username": FTP username
                - "password": FTP password

    Returns:
        None
    """
    if dataset_id is not None and dataset_uid is not None:
        exit(f"Error: dataset_id ({dataset_id}) and dataset_uid ({dataset_uid}) can't both be specified.")
    if dataset_id is None and dataset_uid is None:
        exit("Error: dataset_id or dataset_uid have to be specified.")
    if dataset_id is not None:
        dataset_uid = get_uid_by_id(dataset_id)
    if file_obj is None or remote_filename is None or ftp_config is None:
        exit("Error: please provide file, filename and ftp configuration")
    base_url = get_ods_url()
    headers = {"Authorization": f"apikey {get_api_key()}"}

    # Upload file to ftp server
    ftp_host = ftp_config["host"]
    ftp_username = ftp_config["username"]
    ftp_password = ftp_config["password"]
    
    print(f"Uploading {remote_filename} to {ftp_host}.")
    with FTP(ftp_host, ftp_username, ftp_password) as ftp:
        file_obj.seek(0)
        ftp.storbinary(f"STOR {remote_filename}", file_obj)
    
    # Check if source is already added to dataset
    response = requests.get(f"{base_url}/datasets/{dataset_uid}/resources", headers=headers)
    response_json = response.json()
    for i in range(response_json["total_count"]):
        connection = response_json["results"][i]["datasource"]["relative_url"]
        if connection == f"/{remote_filename}":
            print(f"{ftp_host}/{remote_filename} is already linked as a source to dataset {dataset_uid}")
            return

    # Add ftp file url as source for ods dataset
    payload = {
        "type": "csvfile",
        "title": remote_filename,
        "params": {
            "doublequote": False,
            "encoding": "utf-8",
            "first_row_no": 1,
            "headers_first_row": True,
            "separator": ","
        },
        "datasource": {
            "type": "ftp",
            "connection": {
                "type": "ftp",
                "url": "ftp://"+ftp_host,
                "auth": {
                    "type": "basic_auth",
                    "username": ftp_username,
                    "password": ftp_password
                }
            },
            "relative_url": remote_filename
        }
    }

    # Send request to Automation API
    print(f"Adding ftp://{ftp_host}/{remote_filename} as source to dataset {dataset_uid}.")
    response = requests.post(f"{base_url}/datasets/{dataset_uid}/resources/", headers=headers, json=payload)
    
    if response.status_code in {200,201}:
        print("Source added successfully.")
    else:
        print("Failure:")
        print(response.status_code)
        print(response.text)
    return
