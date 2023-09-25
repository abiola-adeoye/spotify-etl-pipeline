import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from src.spotify_helper import SpotifyData
from src.mongodb_helper import load_to_mongo
from src.google_cloud_helper import create_bucket, upload_to_gcs, load_to_bq


load_dotenv()

SPOTIFY_ACCESS_TOKEN = os.getenv("SPOTIFY_ACCESS_TOKEN")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

file_name = "2023_07_12_to_2023_07_19.csv"

spotify_bucket = 'abiola_adeoye_de_spotify_etl'
mongo_connection_str = "Enter your connection string"
mongo_db_name = "Enter your mongo database name"


def get_spotify_data(past_days=7):
    bucket_status = create_bucket(spotify_bucket)
    today = datetime.now()
    past_days_seconds = today - timedelta(days= past_days + 1)
    past_days_unix_timestamp = int(past_days_seconds.timestamp()) * 1000

    spotify = SpotifyData(access_token=SPOTIFY_ACCESS_TOKEN, refresh_token=SPOTIFY_REFRESH_TOKEN,
                       client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)

    params = {"limit": 50, 'after': past_days_unix_timestamp}
    spotify_data = spotify.request_recently_played(params)
    with tempfile.TemporaryDirectory() as directory:
        df = pd.DataFrame.from_records(spotify_data)
        df.to_csv(f'{directory}/{file_name}')
        if bucket_status['status'] is True:
            upload_to_gcs(bucket_status['bucket_object'], file_name, f"{directory}/{file_name}")
        blob_link = f'https://storage.cloud.google.com/{spotify_bucket}/{file_name}'
        return blob_link


csv_blob_link = get_spotify_data()

load_to_bq(csv_blob_link)

load_to_mongo(file_name, spotify_bucket, mongo_connection_str, mongo_db_name)
