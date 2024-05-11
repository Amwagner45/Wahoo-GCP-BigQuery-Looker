from google.cloud import bigquery
from google.oauth2 import service_account

# Load Credentials from service account json file
bqcreds = service_account.Credentials.from_service_account_file(
    "service_account_creds.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Construct a BigQuery client object.
bigquery_client = bigquery.Client(
    credentials=bqcreds,
    project=bqcreds.project_id,
)


# Create function to load provided dataframe to bigquery. Default mode is to truncate table then write data to table.
def load_to_bigquery(TABLEID, data, write_disposition="WRITE_TRUNCATE"):
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

    job = bigquery_client.load_table_from_dataframe(
        data, TABLEID, job_config=job_config
    )

    job.result()
