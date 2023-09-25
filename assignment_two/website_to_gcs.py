import tempfile
import requests
import pandas as pd
from google.cloud import storage

from log import load_logging

website_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
cab_types = {1: "yellow", 2: "green", 3:"fhv"}
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
file_name = "tripdata_2019"
file_type = "csv.gz"
logger = load_logging("website-to-gcs")

unique_preffix = "abiola_adeoye_de"


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


for cab_type in cab_types.items():
    folder = f"{cab_type[1]}_folder"
    full_bucket_name = f"{unique_preffix}_{cab_type[1]}"
    bucket_status = create_bucket(full_bucket_name)
    for month in months:
        full_download_path = f"{website_url}/{cab_type[1]}/{cab_type[1]}_{file_name}-{month}.{file_type}"

        full_file_name = f"{cab_type[1]}_{file_name}-{month}.{file_type}"
        check = check_blob_exist(full_bucket_name, full_file_name.replace('.csv.gz', '.parquet')) #don't download if exist
        if check is True:
            continue

        with tempfile.TemporaryDirectory() as directory:
            try:
                downloaded_file = requests.get(full_download_path)
                logger.info(f" done dowloading file: {full_file_name}")

            except Exception:
                logger.error(f" file does not exist or error downloading file", exc_info=True)
                continue

            open(f"{directory}/{full_file_name}", 'wb').write(downloaded_file.content)
            try:
                file_df = pd.read_csv(f"{directory}/{full_file_name}", encoding='unicode_escape')
            except UnicodeDecodeError:
                file_df = pd.read_csv(f"{directory}/{full_file_name}", delimiter=',')
            except Exception:
                logger.error(f"error reading file {full_file_name}, will skip",exc_info=True)
                continue
            full_file_name = full_file_name.replace('.csv.gz', '.parquet')
            file_df.to_parquet(f"{directory}/{full_file_name}", engine='pyarrow')
            logger.info("file successfully changed to parquet")

            if bucket_status['status'] is True:
                upload_to_gcs(bucket_status['bucket_object'], full_file_name, f"{directory}/{full_file_name}")
logger.info("Finished website to GCS process")
