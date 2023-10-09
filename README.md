# data-engineering-repo
This project is currently being updated but should be complete by 17th of October 2023 (lack of time to finish up the project)

For now, the completed parts of the project are: implementation of spotify's API, code to push the csv file to GCS, code to push the csv file from GCS to BigQuery.
The incomplete parts of the project for now includes: the transformation script to clean the data, the shell scripts used to deploy a cloud function as well as the cloud functions implementation.

This project consists of two parts: a flask backend service used to make request to spotify's API (plans to shift the backend service to HTTP triggered google serverless functions are in progress), and an ELT pipeline which retrieves the listening history of the user who has given access, push this listening history as a csv file to an object storage of which a cloud function will be triggered by the creation of the csv file, the cloud function would perform the necessary transformation and loading to BigQuery
