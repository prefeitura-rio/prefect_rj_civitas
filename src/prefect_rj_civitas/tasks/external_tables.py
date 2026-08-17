# -*- coding: utf-8 -*-
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from prefect import task
from typing import Literal, List, Dict, Any
import csv
import io

from iplanrio.pipelines_utils.prefect import log


@task
def upload_data_to_storage_task(
    project_id: str,
    bucket_name: str,
    blob_full_name: str,
    data: List[Dict[str, Any]],
    column_names: List[str]
):
    """
    Sobe dados no formato CSV para um bucket GCS a partir de uma lista de dicionários.

    Args:
        project_id: ID do projeto GCP
        bucket_name: nome do bucket
        blob_full_name: caminho do arquivo dentro do bucket (ex: 'pasta/subpasta/arquivo.csv')
        data: dados no fromato de lista de dicionários
        column_names: lista com os nomes das colunas dos dados no CSV
    """
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

    log(f"Uploaded gs://{bucket_name}/{blob_full_name}", level="info")
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

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    log(f"Creating external table {full_table_id}", level="info")

    external_config = bigquery.ExternalConfig(file_format)

    external_config.source_uris = [gcs_path]

    external_config.schema = schema

    if file_format.upper() == "CSV":
        external_config.options.skip_leading_rows = 1
        external_config.options.field_delimiter = ","

    external_config.autodetect = False

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    try:
        existing_table = client.get_table(full_table_id)
        log(f"Table {full_table_id} already exists. Skipping table creation", level="info")
        return existing_table

    except NotFound:
        table = client.create_table(table, exists_ok=True)
        log(f"External table {full_table_id} successfully created", level="info")
        return table