# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake do Disque Denúncia (Prefect 3).

Migrado de pipelines_rj_civitas Prefect 1.4 (disque_denuncia/extract/flows.py):
- Flow/Parameter/case → @flow + if; schedule em prefect.yaml.
- create_flow_run/wait_for_flow_run → execute_dbt_task (iplanrio).
- check_report_qty ENDRUN+Skipped → return Completed(..., name="Skipped") quando não há XML.
"""

from pathlib import Path
from typing import Any, Literal, Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.dbt import execute_dbt_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect.states import Completed
from prefect_rj_civitas import verify_secrets_task
from pipelines.rj_civitas__disque_denuncia.tasks import (
    get_reports_from_start_date,
    loop_transform_report_data,
    task_get_date_execution,
    update_missing_coordinates_in_bigquery,
)

DBT_GIT_REPOSITORY = "https://github.com/prefeitura-rio/queries-rj-civitas.git"
RAW_DIR = Path("/tmp/pipelines/disque_denuncia/data/raw")
PARTITION_DIR = Path("/tmp/pipelines/disque_denuncia/data/partition_directory")


@flow(log_prints=True)
def rj_civitas__disque_denuncia(
    project_id: str,
    dataset_id: str,
    table_id: str,
    start_date: str,
    tipo_difusao: str,
    loop_limiter: bool,
    dump_mode: Literal["append", "overwrite"],
    biglake_table: bool,
    mod: int,
    materialize_after_dump: bool,
    materialize_reports_dd_after_dump: bool,
    georeference_reports: bool,
    mode: Literal["prod", "staging"],
    address_columns: list[str],
    lat_lon_columns: dict[str, str],
    id_column_name: str,
    timestamp_creation_column_name: str,
    start_date_geocoding: str | None = None,
    date_column_name_geocoding: str | None = None,
    required_secrets: tuple[str, ...] | None = None,
) -> Any:
    rename_current_flow_run_task(new_name=f"ELT_{dataset_id}_{table_id}")
    verify_secrets_task(secrets=required_secrets)
    inject_bd_credentials_task(environment="prod")

    date_execution = task_get_date_execution(utc=False)

    reports_response = get_reports_from_start_date(
        start_date=start_date,
        file_dir=RAW_DIR,
        tipo_difusao=tipo_difusao,
        dataset_id=dataset_id,
        table_id=table_id,
        loop_limiter=loop_limiter,
        mod=mod,
        mode=mode,
    )

    if not reports_response["xml_file_path_list"]:
        return Completed(
            message="No data returned by the API, finishing the flow.",
            name="Skipped",
        )

    loop_transform_report_data(
        source_file_path_list=reports_response["xml_file_path_list"],
        final_file_dir=PARTITION_DIR,
        mod=mod,
    )

    create_table_and_upload_to_gcs_task(
        data_path=PARTITION_DIR,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    if materialize_after_dump:
        execute_dbt_task(
            command="build",
            target="prod",
            select=table_id,
            git_repository_path=DBT_GIT_REPOSITORY,
        )
        if materialize_reports_dd_after_dump:
            execute_dbt_task(
                command="build",
                target="prod",
                select="reports_disque_denuncia",
                git_repository_path=DBT_GIT_REPOSITORY,
            )

    if georeference_reports:
        update_missing_coordinates_in_bigquery(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            id_column_name=id_column_name,
            address_columns_names=address_columns,
            lat_lon_columns_names=lat_lon_columns,
            mode=mode if mode in ("prod", "staging") else "prod",
            date_execution=date_execution,
            start_date=start_date_geocoding,
            date_column_name=date_column_name_geocoding,
            timestamp_creation_column_name=timestamp_creation_column_name,
        )
