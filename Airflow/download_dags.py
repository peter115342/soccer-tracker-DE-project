from google.cloud import storage
import os
import shutil

def download_dags_from_gcs(bucket_name, prefix, local_dag_folder):
    shutil.rmtree(local_dag_folder, ignore_errors=True)
    os.makedirs(local_dag_folder, exist_ok=True)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.name.endswith('.py'):
            destination_file_name = os.path.join(local_dag_folder, os.path.basename(blob.name))
            blob.download_to_filename(destination_file_name)
            print(f"Downloaded: {blob.name} to {destination_file_name}")

if __name__ == "__main__":
    bucket_name = os.environ.get('GCS_BUCKET_NAME', 'soccer-tracker-bucket')
    download_dags_from_gcs(bucket_name, 'dags/', '/app/airflow/dags')
