from google.cloud import storage
import os
import shutil

def download_from_gcs(bucket_name, prefix, local_folder):
    shutil.rmtree(local_folder, ignore_errors=True)
    os.makedirs(local_folder, exist_ok=True)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if blob.name.endswith('.py'):
            relative_path = os.path.relpath(blob.name, prefix)
            destination_file_name = os.path.join(local_folder, relative_path)
            os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
            blob.download_to_filename(destination_file_name)

if __name__ == "__main__":
    bucket_name = os.environ.get('BUCKET_NAME')
    download_from_gcs(bucket_name, 'dags/', '/app/airflow/dags')
    download_from_gcs(bucket_name, 'utils/', '/app/airflow/dags/utils')
