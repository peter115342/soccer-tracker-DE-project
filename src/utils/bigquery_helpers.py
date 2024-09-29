from google.cloud import bigquery
import polars as pl

def insert_data_into_bigquery(data):
    client = bigquery.Client()
    #TODO
    table_id = 'project.dataset.table'
    
    if isinstance(data, pl.DataFrame):
        data = data.to_dicts()
    
    errors = client.insert_rows_json(table_id, data)
    if errors:
        print(f"Errors occurred: {errors}")
    else:
        print("Data inserted successfully.")
