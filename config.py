import os

def get_ods_url():
    return f"https://{os.environ["ODS_DOMAIN"]}/api/{os.environ["ODS_API_TYPE"]}"

def get_api_key():
    return os.environ["ODS_API_KEY"]
