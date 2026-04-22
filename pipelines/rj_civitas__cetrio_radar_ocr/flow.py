# -*- coding: utf-8 -*-
"""
Flow for extracting CETRIO radar data and loading into BigQuery.
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_civitas__cetrio_radar_ocr.tasks import (
    extract_radar_data,
    get_cetrio_radar_api_credentials,
)


@flow(log_prints=True)
def rj_civitas__cetrio_radar_ocr(
    dataset_id: str = "cerco_digital",
    table_id: str = "radar",
    dump_mode: str = "overwrite",
    infisical_secret_path: str = "/cetrio_radar",
    filename: str = "radar_data.csv",
    environment: str = "prod",
):
    """
    Extracts radar data from CETRIO API and writes it to BigQuery.
    """
    rename_current_flow_run_task(new_name=f"ELT_{dataset_id}_{table_id}")
    inject_bd_credentials_task(environment=environment)

    secrets = get_cetrio_radar_api_credentials(secret_path=infisical_secret_path)
    filepath = extract_radar_data(secrets=secrets, filename=filename)

    create_table_and_upload_to_gcs_task(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
    )
