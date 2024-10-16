from google.cloud import bigquery
import pandas as pd

# Path to your service account key JSON file
service_account_json = "credits\\peaceful-seeker-431916-t5-243cdf247bde.json"

# Initialize the BigQuery client
client = bigquery.Client.from_service_account_json(service_account_json)

# Define your query string - Replace with your actual query
query = """
    SELECT
        *
    FROM
        `measurement-lab.ndt.unified_downloads`
    WHERE
        DATE BETWEEN '2024-01-01' AND '2024-06-30'
    LIMIT 1000
"""

# Execute the query
query_job = client.query(query)

# Convert the result to a Pandas DataFrame
df = query_job.to_dataframe()

# Show the first few rows of the dataframe
print(df.head())

# Optionally, save the data to a CSV file
df.to_csv('mlab_ndt_data.csv', index=False)
