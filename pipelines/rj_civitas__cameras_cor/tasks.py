# -*- coding: utf-8 -*-
import httpx
import csv
from typing import Literal
from google.cloud import bigquery
from prefect import task
from iplanrio.pipelines_utils.prefect import log


@task
def fetch_cameras_task(
    tixxi_url: str,
    tixxi_key: str,
    tixxi_email: str,
    tixxi_password: str
):
    body = {
        "api_key": tixxi_key,
        "email": tixxi_email,
        "password": tixxi_password
    }
    log(f"Fetching data from TIXXI", level="info")
    try:
        response = httpx.post(url=tixxi_url, json=body)

        response.raise_for_status()
        log("Data obtained successfully.", level="info")

        data = response.json()["cameras"]

        cameras = [{
            "CameraCode": camera["code"],
            "CameraName": camera["name"],
            "CameraZone": None,
            "Latitude": camera["latitude"],
            "Longitude": camera["longitude"],
            "Streamming": camera["stream_url"]
        } for camera in data]

        if isinstance(cameras, list) and len(cameras) > 0:
            headers = cameras[0].keys()

            with open("temp_tixxi_cameras.csv", "w", newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(cameras)

            return True

        else:
            log("Format returned is not JSON or it is empty", level="error")
            return None

    except Exception as e:
        log(f"Error obtaining data: {e}", level="error")
        raise

@task
def upload_to_data_storage_task(
    project_id: str,
    bucket_id: str,
    file_name: str,
    data: list[dict]
):
    #TODO
    return

@task
def create_external_storage_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    gcs_path: str,
    schema: list[bigquery.SchemaField],
    file_format: Literal["PARQUET", "CSV"] = "CSV"
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

    client = bigquery.Client()

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