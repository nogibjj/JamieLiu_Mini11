"""
Test Databricks functionality for the Drinks Dataset project
"""
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_proj11/drinks.csv"
url = f"https://{server_h}/api/2.0"

# Function to check if a file path exists in DBFS and validate auth settings
def check_filestore_path(path, headers):
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        data = response.json()
        if "path" in data:
            print(f"File path exists: {data['path']}")
            return True
        else:
            print("File path does not exist.")
            return False
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False


# Test the drinks dataset file in DBFS
def test_databricks():
    headers = {"Authorization": f"Bearer {access_token}"}
    print(f"Testing if file exists at: {FILESTORE_PATH}")
    assert (
        check_filestore_path(FILESTORE_PATH, headers) is True
    ), f"Test failed: File path {FILESTORE_PATH} does not exist or cannot be accessed."


if __name__ == "__main__":
    test_databricks()
