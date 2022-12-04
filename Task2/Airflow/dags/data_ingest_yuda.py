import os
import logging
import requests
import json
import pandas as pd
import pyarrow
import pyarrow.csv as pv
import pyarrow.json as jsw
import pyarrow.parquet as pq


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# credential_path = "D:/Dwi/tes/.google/credentials/google_credentials.json"
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'yuda-datafellowship8')
BUCKET = os.environ.get('GCP_GCS_BUCKET', 'yuda-datafellowship8')
CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '.google/credentials/google_credentials.json')
# CREDENTIALS = credential_path

api_url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
api_result = 'result_data.json'
path_to_local_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
csv_file = api_result.replace('.json', '.csv')
parquet_file = csv_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'pc2_dataset_yudasatrias')

def call_dataset_api(url_json: str, local_json: str):
    r = requests.get(url = url_json)

    data = r.json()
    with open(local_json, 'w') as outfile:
        json.dump(data, outfile)

def format_to_csv(json_file: str):
    if not json_file.endswith('.json'):
        logging.error('Only JSON file type')
        return

    data = jsw.read_json(json_file)
    data_arr = data['data'].to_numpy()

    data_list = list(i for i in data_arr[0])
    df = pd.DataFrame.from_dict(data_list)
    df_columnar = pyarrow.Table.from_pandas(df)

    pv.write_csv(df_columnar, f"{path_to_local_home}/{csv_file}")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Only CSV file type")
        return
    data = pv.read_csv(src_file)
    pq.write_table(data, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    print("INI CLIENT:")
    client = storage.Client()
    print("AFTER client:")
    bucket = client.bucket(bucket)
    print("bucket:")

    blob = bucket.blob(object_name)
    print("init blob:")
    blob.upload_from_filename(local_file)
    print("UPLOADED:")


default_args = {
    "owner": "yudasatrias",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingest_yuda",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['pc2_dataset_yudasatrias'],
) as dag:

    call_dataset_task = PythonOperator(
        task_id="call_dataset_task",
        python_callable=call_dataset_api,
        op_kwargs={
            "url_json": api_url,
            "local_json": f"{path_to_local_home}/{api_result}"
        },
    )

    save_as_csv_task = PythonOperator(
        task_id="save_as_csv_task",
        python_callable=format_to_csv,
        op_kwargs={
            "json_file": f"{path_to_local_home}/{api_result}",
        }
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data_raw_yudasatrias/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/data_raw_yudasatrias/{parquet_file}"],
            },
        },
    )

    call_dataset_task >> save_as_csv_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task