# SPOTIFY-ELT-PIPELINE
This includes an endpoint used to get access to a user's account which can be seen in the app.py file and a script used to request for their listening data and pushed to an etl pipeline for further analysis.

For now, the completed parts of the project are the implementation of Spotify's API, the code to push the CSV file to GCS, code to push the CSV file from GCS to BigQuery.
The incomplete parts of the project for now include the transformation script to clean the data, the shell scripts used to deploy a cloud function as well and the cloud functions implementation.

This project consists of two parts: a flask backend service used to make requests to Spotify's API (plans to shift the backend service to HTTP-triggered Google serverless functions are in progress), and an ELT pipeline that retrieves the listening history of the user who has given access, push this listening history as a CSV file to object storage of which a cloud function will be triggered by the creation of the CSV file, the cloud function would perform the necessary transformation and load to BigQuery
