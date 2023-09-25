from google.cloud import bigquery


def create_table_dataset(table_name, dataset_name):
    bq_client = bigquery.Client()

    dataset_id = f"{bq_client.project}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = bq_client.create_dataset(dataset, timeout=30, exists_ok=True)  # Make an API request.
    print("Created dataset {}.{}".format(bq_client.project, dataset.dataset_id))

    table_id = f"{bq_client.project}.{dataset_name}.{table_name}"



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

    return table.table_id
