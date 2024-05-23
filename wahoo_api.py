from gcp_secret_manager import *
import requests
import pandas as pd


def refreshtokens(refresh_token, CLIENTID, CLIENTSECRET):
    url = f"https://api.wahooligan.com/oauth/token?client_secret={CLIENTSECRET}&client_id={CLIENTID}&grant_type=refresh_token&refresh_token={refresh_token}"
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
