import functions_framework
import re
from google.cloud import bigquery

client = bigquery.Client()

stage_table_id = "learngcp-408315.prod.wahoo_stage"
table_id = "learngcp-408315.prod.wahoo"

file_pattern = r"wahoo_[0-9]{8}\.csv$"

table_schema = [
    bigquery.SchemaField("id", "NUMERIC"),
    bigquery.SchemaField("starts", "TIMESTAMP"),
    bigquery.SchemaField("minutes", "NUMERIC"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("workout_token", "STRING"),
    bigquery.SchemaField("workout_type_id", "NUMERIC"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("workout_summary_id", "NUMERIC"),
    bigquery.SchemaField("workout_summary_calories_accum", "NUMERIC"),
    bigquery.SchemaField("workout_summary_cadence_avg", "NUMERIC"),
    bigquery.SchemaField("workout_summary_distance_accum", "NUMERIC"),
    bigquery.SchemaField("workout_summary_duration_active_accum", "NUMERIC"),
    bigquery.SchemaField("workout_summary_duration_total_accum", "NUMERIC"),
    bigquery.SchemaField("workout_summary_power_avg", "NUMERIC"),
    bigquery.SchemaField("workout_summary_power_bike_np_last", "NUMERIC"),
    bigquery.SchemaField("workout_summary_power_bike_tss_last", "NUMERIC"),
    bigquery.SchemaField("workout_summary_speed_avg", "NUMERIC"),
    bigquery.SchemaField("workout_summary_work_accum", "NUMERIC"),
    bigquery.SchemaField("workout_summary_created_at", "TIMESTAMP"),
    bigquery.SchemaField("workout_summary_updated_at", "TIMESTAMP"),
]


# Triggered when file is created in storage bucket
@functions_framework.cloud_event
def main(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    print(f"Bucket: {bucket}")
    print(f"File: {name}")

    if not re.match(file_pattern, name):
        print(f"{name} not match file_pattern")
        return

    job_config = bigquery.LoadJobConfig(
        schema=table_schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    gcs_uri = f"gs://{bucket}/{name}"

    print("About to load data from uri")

    load_job = client.load_table_from_uri(
        gcs_uri, stage_table_id, job_config=job_config
    )  # Make an API request.

    print("Loading")

    load_job.result()

    destination_table = client.get_table(stage_table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

    # Merge Query
    merge_sql_file = open("merge.sql", "r")
    merge_sql = merge_sql_file.read()
    merge_sql = merge_sql.replace("$stage_table_id", stage_table_id)
    merge_sql = merge_sql.replace("$table_id", table_id)

    query_job = client.query(
        merge_sql,
        location="us-east1",
        job_id_prefix="wahoo_merge_",
    )

    query_job.result()
    print("Completed job: {}".format(query_job.job_id))
