import pymongo
import tempfile
import json
import pandas as pd
from google.cloud import storage


def load_to_mongo(filename, bucket_name, mongo_conn_string, db_name):
    try:
        with tempfile.NamedTemporaryFile("w") as directory:
            destination_uri = directory.name

            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # download csv and read it
            blob = bucket.blob(filename)
            blob.download_to_filename(destination_uri)
            print("File downloaded from Google Cloud Storage.")

            df = pd.read_csv(destination_uri)
            doc_json = df.to_json(orient='index')
            data = json.loads(doc_json)
            print(f"Dataframe successfully loaded into JSON object")

            # Create a MongoClient instance
            client = pymongo.MongoClient(mongo_conn_string)

            # Access a database
            db = client.get_database(mongo_db)

            # Define the list of documents to insert
            documents = []
            for key, value in data.items():
                documents.append(value)

            # Access the desired collection
            collection = db.get_collection(db_name)

            # Insert each document into the collection
            inserted_ids = []
            for document in documents:
                result = collection.insert_one(document)
                inserted_ids.append(result.inserted_id)

            # Close the connection
            client.close()

            # Return the inserted document IDs
            return inserted_ids

    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")

