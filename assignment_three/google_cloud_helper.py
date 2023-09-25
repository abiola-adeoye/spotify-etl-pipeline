import pandas as pd

from log import load_logging
from google.cloud import storage, bigquery


logger = load_logging("website-to-gcs")


def check_bucket_exist(client, bucket_name):
    global logger

    buckets = client.list_buckets()
    for bucket in buckets:
        if bucket_name == bucket.name:
            logger.info(f"the bucket {bucket_name} exist, will not attempt to create")
            return {"status":True, "bucket_object":bucket}
    return {"status":False}


def check_blob_exist(bucket_name, file_to_check):
    global logger

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        if file_to_check == blob.name:
            logger.info(f"the blob file {file_to_check} exist, will not attempt to upload")
            return True


def create_bucket(bucket_name):
    global logger
    client = storage.Client()

    # checks if bucket exist and returns it
    check = check_bucket_exist(client, bucket_name)
    if check['status'] is True:
        return check

    # create bucket if it doesn't exist
    bucket = client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"

    try:
        new_bucket = client.create_bucket(bucket, location="us")
        logger.info(f"Created bucket {new_bucket.name} in {new_bucket.location}"
                    f" with storage class {new_bucket.storage_class}")
        return {"status": True, "bucket_object": new_bucket}
    except Exception:
        logger.error(f"error creating the bucket: {bucket_name}", exc_info=True)
        return {"status":False}


def upload_to_gcs(bucket_object, file_name, file):
    global logger

    check = check_blob_exist(bucket_object.name, file_name)
    if check is True:
        return None

    blob = bucket_object.blob(file_name)
    blob.upload_from_filename(file)
    logger.info(f"blob {file_name} is done uploading")


def load_to_bq(spotify_blob_link):
    bq_client = bigquery.Client()
    table_name = "recently_played_list"
    dataset_name = "spotifydata"

    dataset_id = f"{bq_client.project}.{dataset_name}"
    table_id = f"{bq_client.project}.{dataset_name}.{table_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = bq_client.create_dataset(dataset, timeout=30, exists_ok=True)  # Make an API request.
    print("Created dataset {}.{}".format(bq_client.project, dataset.dataset_id))

    schema = [
        bigquery.SchemaField("track_name", "STRING", mode="REQUIRED"),  # correct
        bigquery.SchemaField("track_popularity", "INT64"),  #correct
        bigquery.SchemaField("track_countries_available", "INT64", mode="REQUIRED"),    #correct
        bigquery.SchemaField("track_duration_ms", "INT64", mode="REQUIRED"),    #correct
        bigquery.SchemaField("artist_name", "STRING", mode="REQUIRED"),     #correct
        bigquery.SchemaField("artist_followers", "INT64"),  #correct
        bigquery.SchemaField("featured_artist", "STRING"),  #correct
        bigquery.SchemaField("in_album", "BOOL", mode="REQUIRED"),      #correct
        bigquery.SchemaField("album_countries_available", "INT64"),     #correct
        bigquery.SchemaField("album_name", "STRING"),       #correct
        bigquery.SchemaField("played_at", "STRING"),
        bigquery.SchemaField("time_stamp", "STRING"),
        bigquery.SchemaField("played_from", "STRING"),  # correct
        bigquery.SchemaField("track_details", "STRING")
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )

    # Load data from Spotify blob link to BigQuery table
    uri = spotify_blob_link
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()