import functions_framework
import datetime
import requests
import pandas as pd
import os
from datetime import datetime, timezone
import pytz
from google.cloud import secretmanager
from google.cloud import storage


BASEURL = "api.wahooligan.com"
PROJECTID = "learngcp-408315"

# Create the Secret Manager client.
secret_manager_client = secretmanager.SecretManagerServiceClient()

print("Beginning")


def access_secret_version(
    secret_id, project_id=PROJECTID, client=secret_manager_client
):
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    payload = response.payload.data.decode("UTF-8")
    return payload


def destroy_secret_version(
    secret_id, project_id=PROJECTID, client=secret_manager_client
):
    version = get_secret_version(secret_id)

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

    # Destroy the secret version.
    response = client.destroy_secret_version(request={"name": name})

    print(f"Destroyed secret version: {response.name}")


def add_secret_version(
    secret_id, payload, project_id=PROJECTID, client=secret_manager_client
):
    parent = client.secret_path(project_id, secret_id)

    # convert payload from string to bytes
    payload_bytes = payload.encode("UTF-8")

    # Add the secret version.
    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload_bytes},
        }
    )

    print(f"Added secret version: {response.name}")


def get_secret_version(secret_id, project_id=PROJECTID, client=secret_manager_client):
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Get the secret version.
    response = client.get_secret_version(request={"name": name})

    # Print information about the secret version.
    state = response.state.name

    version = (response.name).split("/")[-1]
    print(f"Got secret version {response.name} with state {state}")
    return version


def refreshtokens(refresh_token, BASEURL, CLIENTID, CLIENTSECRET):
    url = f"https://{BASEURL}/oauth/token?client_secret={CLIENTSECRET}&client_id={CLIENTID}&grant_type=refresh_token&refresh_token={refresh_token}"
    r = requests.post(url)
    data = r.json()
    access_token = data["access_token"]
    refresh_token = data["refresh_token"]

    # get_secret_version('wahoo_access_token')
    destroy_secret_version("wahoo_access_token")
    add_secret_version("wahoo_access_token", access_token)

    # get_secret_version('wahoo_refresh_token')
    destroy_secret_version("wahoo_refresh_token")
    add_secret_version("wahoo_refresh_token", refresh_token)
    return access_token, refresh_token


def get_workouts(access_token, per_page=7):
    r = requests.get(
        "https://api.wahooligan.com/v1/workouts",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"per_page": per_page},
    )
    data = r.json()
    data = data["workouts"]
    return data


def transform_data(data):
    df = pd.json_normalize(data)

    columns_to_drop = [
        "plan_id",
        "workout_summary.ascent_accum",
        "workout_summary.duration_paused_accum",
        "workout_summary.heart_rate_avg",
        "workout_summary.file.url",
        "workout_summary.files",
    ]
    df = df.drop(columns_to_drop, axis=1)
    df = df.convert_dtypes(infer_objects=True)
    df.columns = df.columns.str.replace(".", "_")
    return df


# Triggered when message published in topic
def main(event, context):
    CLIENTID = access_secret_version("wahoo_client_id")
    CLIENTSECRET = access_secret_version("wahoo_client_secret")
    ACCESSTOKEN = access_secret_version("wahoo_access_token")
    REFRESHTOKEN = access_secret_version("wahoo_refresh_token")

    ACCESSTOKEN, REFRESHTOKEN = refreshtokens(
        REFRESHTOKEN, BASEURL, CLIENTID, CLIENTSECRET
    )
    workout_data = get_workouts(ACCESSTOKEN)
    df = transform_data(workout_data)

    # Construct a BigQuery client object.
    big_query_client = storage.Client()
    bucket_name = "input_data_pipeline"
    bucket = big_query_client.bucket(bucket_name)

    today = datetime.now(pytz.timezone("America/New_York")).strftime("%Y%m%d")
    bucket.blob(f"wahoo_{today}.csv").upload_from_string(
        df.to_csv(index=False), "text/csv"
    )

    print("End")
