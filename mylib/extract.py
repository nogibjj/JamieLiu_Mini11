import requests
from dotenv import load_dotenv
import os
import json
import base64

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_proj11"
headers = {'Authorization': 'Bearer %s' % access_token}
base_url = "https://" + server_h + "/api/2.0"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', base_url + path,
                           data=json.dumps(data),
                           verify=True,
                           headers=headers)
    return resp.json()


def mkdirs(path, headers):
    """
    Creates a directory in DBFS if it doesn't exist.
    """
    _data = {'path': path}
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)


def create(path, overwrite, headers):
    """
    Initializes a file handle in DBFS for uploading data.
    """
    _data = {'path': path, 'overwrite': overwrite}
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    """
    Uploads a chunk of data to DBFS using the file handle.
    """
    _data = {'handle': handle, 'data': data}
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    """
    Closes the file handle to finalize the upload.
    """
    _data = {'handle': handle}
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    """
    Downloads a file from a URL and uploads it to DBFS.
    """
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print(f"Uploading file to {dbfs_path}...")
        for i in range(0, len(content), 2**20):
            add_block(handle,
                      base64.standard_b64encode(content[i:i+2**20]).decode(),
                      headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
        url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/alcohol-consumption/drinks.csv",
        file_path=FILESTORE_PATH + "/drinks.csv",
        directory=FILESTORE_PATH,
        overwrite=True
):
    """
    Downloads a dataset from a URL and saves it to DBFS.
    """
    # Create the target directory
    mkdirs(path=directory, headers=headers)

    # Upload the file to DBFS
    put_file_from_url(url, file_path, overwrite, headers=headers)

    return file_path


if __name__ == "__main__":
    # Call the extract function with the dataset URL
    extract()
