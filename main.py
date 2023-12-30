import os
import dotenv
import requests
import json

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)


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


def get_workouts(access_token, per_page=7):
    r = requests.get(
        "https://api.wahooligan.com/v1/workouts",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"per_page": per_page},
    )
    data = r.json()
    data = data["workouts"]
    return data


def transform_workouts(workout_data):
    data = {}
    data["total"] = workout_data["total"]
    return data


# try storing data as json and checking if access_token is expired before refreshing new one
# dump all workouts into json file, then locally convert to dataframe and identify what to update on wahoo
# check if data syncs to strava after it has been updated in wahoo
# move to its own directory, use environment variables, publish to github

print(get_workouts(ACCESSTOKEN))

# change environment variable within session
# os.environ["AUTHORIZATIONCODE"] = "100"
# print(os.environ["AUTHORIZATIONCODE"])

# Update .env file
# dotenv.set_key(dotenv_file, "AUTHORIZATIONCODE", os.environ["AUTHORIZATIONCODE"])


"""
1. Optional allow for authorization
2. Have access_token, refresh token, expiration_date stored in environment variables
3. Refresh the access_token and update environment variables
4. Use token to get data for past week
5. Add only new data to Bigquery
"""
