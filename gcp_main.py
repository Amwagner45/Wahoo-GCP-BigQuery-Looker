import os
import dotenv
import requests
import json
import pandas as pd
import logging
from google.cloud import storage

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

pd.options.display.max_columns = 100

BASEURL = "api.wahooligan.com"
CLIENTID = os.environ.get("CLIENTID")
CLIENTSECRET = os.environ.get("CLIENTSECRET")
ACCESSTOKEN = os.environ.get("ACCESSTOKEN")
REFRESHTOKEN = os.environ.get("REFRESHTOKEN")
PROJECTID = os.environ.get("learngcp-408315")
TOPICID = os.environ.get("workout_data_call_topic")


# Helper function to update environment variable
def update_env(key, value):
    os.environ[key] = value
    dotenv.set_key(dotenv_file, key, os.environ[key])


def refreshtokens(refresh_token):
    url = f"https://{BASEURL}/oauth/token?client_secret={CLIENTSECRET}&client_id={CLIENTID}&grant_type=refresh_token&refresh_token={refresh_token}"
    r = requests.post(url)
    data = r.json()
    print(data)
    access_token = data["access_token"]
    refresh_token = data["refresh_token"]

    update_env("ACCESSTOKEN", access_token)
    update_env("REFRESHTOKEN", refresh_token)
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
    return df


def quickstart():
    storage_client = storage.Client()
    bucket_name = "kappa-bucket-1"
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        print(blob.name)


refreshtokens(REFRESHTOKEN)
data = get_workouts(ACCESSTOKEN)
df = transform_data(data)
print(df.head)


# def pull_from_api(event, context):
#     logging.basicConfig(level=logging.INFO)

#     # insert entries into table
#     response = requests.get(f"{BASEURL}?key={API_KEY}&q={Q}")
#     json_response = response.json()

#     # publish data to the topic
#     publisher = pubsub_v1.PublisherClient()
#     # The `topic_path` method creates a fully qualified identifier
#     # in the form `projects/{project_id}/topics/{topic_id}`
#     topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

#     message = json.dumps(json_response).encode("utf-8")
#     future1 = publisher.publish(topic_path, message)
#     print(future1.result())
