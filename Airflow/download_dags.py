from google.cloud import storage
import os

def download_dags_from_gcs(bucket_name, prefix, local_dag_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.name.endswith('.py'):
            destination_file_name = os.path.join(local_dag_folder, os.path.basename(blob.name))
            blob.download_to_filename(destination_file_name)
            print(f"Downloaded: {blob.name} to {destination_file_name}")

if __name__ == "__main__":
    download_dags_from_gcs('soccer-tracker-bucket', 'dags/', '/app/airflow/dags')
