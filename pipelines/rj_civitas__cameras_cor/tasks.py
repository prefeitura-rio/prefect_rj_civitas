# -*- coding: utf-8 -*-
import httpx
import csv
import io
from typing import Literal, List, Dict, Any
from google.cloud import bigquery, storage
from prefect import task
from iplanrio.pipelines_utils.prefect import log


@task(retries=3)
def fetch_cameras_task(
    tixxi_url: str,
    tixxi_key: str,
    tixxi_email: str,
    tixxi_password: str
) -> List[Dict[str, Any]]:
    body = {
        "api_key": tixxi_key,
        "email": tixxi_email,
        "password": tixxi_password
    }
    log(f"Fetching data from TIXXI", level="info")
    try:
        response = httpx.post(url=tixxi_url, json=body)
        response.raise_for_status()
        data = response.json()["cameras"]
        cameras = [{
                "CameraCode": camera["code"],
                "CameraName": camera["name"],
                "CameraZone": None,
                "Latitude": camera["latitude"],
                "Longitude": camera["longitude"],
                "Streamming": camera["stream_url"]
            } for camera in data]
        log("Data obtained successfully.", level="info")
        return cameras

    except Exception as e:
        log(f"Error obtaining data: {e}", level="error")
        raise

@task
def upload_data_to_storage_task(
    project_id: str,
    bucket_name: str,
    blob_full_name: str,
    data: List[Dict[str, Any]],
    column_names: List[str]
):
    log(f"Uploading data to {f"{bucket_name}/{blob_full_name}"}")
    output = io.StringIO()

    writer = csv.DictWriter(
        output,
        fieldnames=column_names,
        extrasaction="ignore",
    )

    writer.writeheader()
    writer.writerows(data)

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_full_name)

    blob.upload_from_string(
        output.getvalue(),
        content_type="text/csv",
    )

    print(f"Uploaded gs://{bucket_name}/{blob_full_name}")
    return

@task
def create_external_storage_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_path: str,
    schema: list[bigquery.SchemaField],
    file_format: Literal["PARQUET", "CSV"]
):
    """
    Cria uma tabela externa no BigQuery apontando para um bucket GCS.

    Args:
        project_id: ID do projeto GCP
        dataset_id: dataset do BigQuery
        table_id: nome da tabela
        gcs_path: caminho gs://bucket/pasta/*
        schema: lista de bigquery.SchemaField
        file_format: PARQUET ou CSV
    """

    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    external_config = bigquery.ExternalConfig(file_format)

    external_config.source_uris = [gcs_path]

    external_config.schema = schema

    if file_format.upper() == "CSV":
        external_config.options.skip_leading_rows = 1
        external_config.options.field_delimiter = ","

    external_config.autodetect = False

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config

    table = client.create_table(table, exists_ok=True)

    print(f"Tabela externa criada: {table.full_table_id}")
    return table