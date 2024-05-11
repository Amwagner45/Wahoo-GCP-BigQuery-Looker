from gcp_bigquery import load_to_bigquery
import pandas as pd
import os

PROJECTID = os.getenv("PROJECTID")
DATASET = os.getenv("DATASET")
TABLENAME = os.getenv("TABLENAME")


def generate_fake_data(n):
    df = []
    for i in range(1, n + 1):
        row = {"id": i}
        df.append(row)
    return df


fake_data = generate_fake_data(3)
df = pd.DataFrame(fake_data)

df = df[df["id"] > 1]
print(df)

# load_to_bigquery(df)
