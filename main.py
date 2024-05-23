from wahoo_api import *
from gcp_bigquery import load_to_bigquery
from gcp_secret_manager import access_secret_version

import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROJECTID = os.getenv("PROJECTID")
DATASET = os.getenv("DATASET")
TABLENAME = os.getenv("TABLENAME")
TABLEID = f"{PROJECTID}.{DATASET}.{TABLENAME}"


def extract_workout_data():

    CLIENTID = access_secret_version("wahoo_client_id")
    CLIENTSECRET = access_secret_version("wahoo_client_secret")
    ACCESSTOKEN = access_secret_version("wahoo_access_token")
    REFRESHTOKEN = access_secret_version("wahoo_refresh_token")

    ACCESSTOKEN, REFRESHTOKEN = refreshtokens(REFRESHTOKEN, CLIENTID, CLIENTSECRET)
    workout_data = get_workouts(ACCESSTOKEN)
    df = pd.json_normalize(workout_data)

    return df


def transform_workout_data(df):
    columns_to_remove = [
        "plan_id",
        "workout_summary.ascent_accum",
        "workout_summary.duration_paused_accum",
        "workout_summary.heart_rate_avg",
        "workout_summary.file.url",
        "workout_summary.files",
    ]

    df = df.drop(columns_to_remove, axis=1)
    df = df.convert_dtypes(infer_objects=True)
    df.columns = df.columns.str.replace(".", "_", regex=True)
    return df


def load_workout_data(df):
    load_to_bigquery(TABLEID, df)
    print("Completed loading dataset to BigQuery")


def main():
    df = extract_workout_data()
    df = transform_workout_data(df)
    load_to_bigquery(TABLEID, df)


if __name__ == "__main__":
    main()
