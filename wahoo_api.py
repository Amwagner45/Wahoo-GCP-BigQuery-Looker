from gcp_secret_manager import *
import requests
import pandas as pd

BASEURL = "api.wahooligan.com"


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


def get_workouts(access_token, per_page=1000000):
    r = requests.get(
        "https://api.wahooligan.com/v1/workouts",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"per_page": per_page},
    )
    data = r.json()
    data = data["workouts"]
    return data


def extract_workout_data():

    CLIENTID = access_secret_version("wahoo_client_id")
    CLIENTSECRET = access_secret_version("wahoo_client_secret")
    ACCESSTOKEN = access_secret_version("wahoo_access_token")
    REFRESHTOKEN = access_secret_version("wahoo_refresh_token")

    ACCESSTOKEN, REFRESHTOKEN = refreshtokens(
        REFRESHTOKEN, BASEURL, CLIENTID, CLIENTSECRET
    )
    workout_data = get_workouts(ACCESSTOKEN)
    df = pd.json_normalize(workout_data)

    columns_to_remove = [
        "plan_id",
        "workout_summary.ascent_accum",
        "workout_summary.duration_paused_accum",
        "workout_summary.heart_rate_avg",
        "workout_summary.file.url",
        "workout_summary.files",
    ]

    df = df[~df[columns_to_remove]].copy()
    df.columns = df.columns.str.replace(".", "_")
    return df
