import pandas as pd
import json

## Pandas Settings
pd.options.display.max_columns = 100

## Load JSON from File
with open("workouts.json", "r") as file:
    data = json.load(file)

## Create dataframe using JSON and normalize the output
df = pd.json_normalize(data)

## Drop unneccesary columns from DataFrame
columns_to_drop = [
    "plan_id",
    "workout_summary.ascent_accum",
    "workout_summary.duration_paused_accum",
    "workout_summary.heart_rate_avg",
    "workout_summary.file.url",
    "workout_summary.files",
]
df = df.drop(columns_to_drop, axis=1)

## Change datatypes of columns
df = df.convert_dtypes(infer_objects=True)
print(df.dtypes)

## Print Results
print(df.head())
print(df.describe())

df.to_csv("workouts.csv", index=False)
