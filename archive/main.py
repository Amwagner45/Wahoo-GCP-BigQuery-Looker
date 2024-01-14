import os
import dotenv
import requests
import json
import pandas as pd
import datetime
from datetime import timedelta

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)

pd.options.display.max_columns = 100

BASEURL = "api.wahooligan.com"
SCOPE = "user_read+workouts_read+workouts_write"
CLIENTID = os.environ.get("CLIENTID")
CLIENTSECRET = os.environ.get("CLIENTSECRET")
REDIRECTURI = os.environ.get("REDIRECTURI")
AUTHORIZATIONCODE = os.environ.get("AUTHORIZATIONCODE")
ACCESSTOKEN = os.environ.get("ACCESSTOKEN")
REFRESHTOKEN = os.environ.get("REFRESHTOKEN")


# Helper function to update environment variable
def update_env(key, value):
    os.environ[key] = value
    dotenv.set_key(dotenv_file, key, os.environ[key])


def authorize():
    url = f"https://{BASEURL}/oauth/authorize?client_id={CLIENTID}&redirect_uri={REDIRECTURI}&scope={SCOPE}&response_type=code"
    print(url)
    AUTHORIZATIONCODE = input("\nEnter code:")
    update_env("AUTHORIZATIONCODE", AUTHORIZATIONCODE)
    return AUTHORIZATIONCODE


def gettokens(AUTHORIZATIONCODE):
    url = f"https://{BASEURL}/oauth/token?client_secret={CLIENTSECRET}&code={AUTHORIZATIONCODE}&redirect_uri={REDIRECTURI}&grant_type=authorization_code&client_id={CLIENTID}"
    r = requests.post(url)
    data = r.json()
    print(data)
    access_token = data["access_token"]
    refresh_token = data["refresh_token"]

    update_env("ACCESSTOKEN", access_token)
    update_env("REFRESHTOKEN", refresh_token)
    return access_token, refresh_token


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


def get_workouts(access_token, per_page=3):
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
    today = datetime.datetime.today()
    yesterday = today - timedelta(days=1)
    df = df[df["starts"].str[0:10] == yesterday.strftime("%Y-%m-%d")]
    return df


# authorize()
# gettokens(AUTHORIZATIONCODE)
# refreshtokens(REFRESHTOKEN)
data = get_workouts(ACCESSTOKEN)
df = transform_data(data)
json_message = df.to_json(orient="index")
print(df.head)
print(json_message)

"""
1. Optional allow for authorization
2. Have access_token, refresh token, expiration_date stored in environment variables
3. Refresh the access_token and update environment variables
4. Use token to get data for past week
5. Add only new data to Bigquery
"""
