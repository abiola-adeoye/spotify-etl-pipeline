from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

query = """
    SELECT airline_code, airline_name
    FROM `hazel-pillar-384107.altschool_flight.altschool_flight_1_semester`
    LIMIT 20
"""
query_job = client.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    airline_name = row[1]
    airline_code = row[0]
    print(f"airline_code: {airline_code}, airline_name:{airline_name}")