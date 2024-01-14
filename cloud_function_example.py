import functions_framework
import re
import datetime
import requests
import dotenv
import pandas as pd
import os

# Libraries
from google.cloud import storage

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

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


# Triggered when file is created in storage bucket
@functions_framework.cloud_event
def main(cloud_event):
    data = cloud_event.data

    print(f"Data: {data}")

    refreshtokens(REFRESHTOKEN)
    workout_data = get_workouts(ACCESSTOKEN)
    df = transform_data(workout_data)
    print(df.head)

    # Construct a BigQuery client object.
    client = storage.Client()
    bucket_name = "input_data_pipeline"
    bucket = client.bucket(bucket_name)

    today = datetime.datetime.now().strftime("%Y%m%d")
    bucket.blob(f"wahoo_{today}.csv").upload_from_string(df.to_csv(), "text/csv")

    print("Completed job: {}")


'access_token': 'XWgw-zFoQqbWz0ls8CaM1N_9g7rsCVQQqSm-TOtojo4', 'token_type': 'Bearer', 'expires_in': 7200, 'refresh_token': 'XGOS2MsC_JSNzIChcKqJfdk1bTyfG0wBPXtCqgid_8E', 'scope': 'user_read workouts_read workouts_write', 'created_at': 1704671113