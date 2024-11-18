"""
Run Databricks Job
"""

import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
access_token = os.getenv("ACCESS_TOKEN")
job_id = os.getenv("JOB_ID")
server_h = os.getenv("SERVER_HOSTNAME")

# Construct the Databricks API endpoint URL
url = f"https://{server_h}/api/2.0/jobs/run-now"

# Set up headers for authentication
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

# Define the payload to trigger the job
data = {"job_id": job_id}


def trigger_job():
    """
    Sends a request to the Databricks REST API to trigger a job.
    """
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raise an HTTPError for bad responses(4xx and 5xx)
        response_data = response.json()
        print(f"Job {job_id} run successfully triggered.")
        print(f"Run ID: {response_data.get('run_id', 'Unknown')}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to trigger job: {e}")
        print(f"Response: {response.text if response else 'No response'}")


if __name__ == "__main__":
    trigger_job()
