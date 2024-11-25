from google.cloud import bigquery, storage
import logging
from typing import List, Tuple
import tempfile
import polars as pl

def transform_match_parquet(parquet_path: str) -> pl.DataFrame:
    df = pl.read_parquet(parquet_path)
    
    if 'referees' in df.columns:
        try:
            df = df.with_columns([
                pl.col('referees').struct.field('list').list.eval(
                    pl.struct([
                        pl.element().struct.field('element').struct.field('id'),
                        pl.element().struct.field('element').struct.field('name'),
                        pl.element().struct.field('element').struct.field('type'),
                        pl.element().struct.field('element').struct.field('nationality').cast(pl.Utf8).fill_null('None')
                    ]).alias('element')
                ).alias('list')
            ])
            
            df = df.with_columns([
                pl.struct([
                    pl.col('referees').struct.field('id'),
                    pl.col('referees').struct.field('name'),
                    pl.col('referees').struct.field('type'),
                    pl.col('referees').struct.field('nationality').cast(pl.Utf8).fill_null('None'),
                    pl.col('referees').struct.field('list')
                ]).alias('referees')
            ])
        except Exception as e:
            logging.warning(f"Error processing referees: {str(e)}")
            pass
    
    return df





def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    loaded_count = 0
    processed_files: List[str] = []
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        if not file.endswith('.parquet'):
            continue

        match_id = file.split('/')[-1].replace('.parquet', '')

        query = f"""
            SELECT COUNT(*) as count 
            FROM `{table_ref}` 
            WHERE id = CAST({match_id} AS INT64)
        """

        if next(client.query(query).result()).count > 0:
            logging.info(f"Match {match_id} already exists in BigQuery, skipping...")
            continue

        blob = bucket.blob(file)
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
            blob.download_to_filename(temp_file.name)
            
            df = transform_match_parquet(temp_file.name)
            df.write_parquet(temp_file.name)
            
            uri = f"gs://{bucket_name}/temp_{match_id}.parquet"
            temp_blob = bucket.blob(f"temp_{match_id}.parquet")
            temp_blob.upload_from_filename(temp_file.name)

        try:
            load_job = client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()

            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Successfully loaded match {match_id} into {table_ref}")

        except Exception as e:
            error_msg = f"Error loading match ID {match_id} from file {file}: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)
        finally:
            temp_blob.delete()

    return loaded_count, processed_files
