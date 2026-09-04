# -*- coding: utf-8 -*-
from google.cloud import bigquery
from datetime import datetime
from typing import Literal, Any
import pytz


PARTITION_TYPES = {
    "HOUR": bigquery.TimePartitioningType.HOUR,
    "DAY": bigquery.TimePartitioningType.DAY,
    "MONTH": bigquery.TimePartitioningType.MONTH,
    "YEAR": bigquery.TimePartitioningType.YEAR,
}

def save_data_in_bq_table(
        data: list[dict[str, Any]],
        schema: list[bigquery.SchemaField],
        project_id: str,
        dataset_id: str,
        table_id: str,
        table_description: str | None = None,
        write_disposition: str = "WRITE_APPEND",
        allow_field_addition: bool = False,
        ignore_unknown_values: bool = True,
        insert_timestamp_field: str | None = None,
        clustering_fields: list[str] | None = None,
        partition_field: str | None = None,
        partition_granularity: Literal["HOUR", "DAY", "MONTH", "YEAR"] = "MONTH"
) -> None:
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"
    client = bigquery.Client(project=project_id)

    if insert_timestamp_field:
        timestamp_now = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        data = [{**row, insert_timestamp_field: timestamp_now} for row in data]

    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=schema,
        write_disposition=write_disposition
    )

    if clustering_fields:
        job_config.clustering_fields = clustering_fields

    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=PARTITION_TYPES[partition_granularity],
            field=partition_field
        )

    if ignore_unknown_values:
        job_config.ignore_unknown_values = True

    if allow_field_addition and write_disposition=="WRITE_APPEND":
        job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]

    try:
        job = client.load_table_from_json(data, table_full_name, job_config=job_config)
        job.result()

        if table_description is not None:
            table = client.get_table(table_full_name)
            table.description = table_description
            client.update_table(table, ["description"])
    except Exception as e:
        raise Exception(e)