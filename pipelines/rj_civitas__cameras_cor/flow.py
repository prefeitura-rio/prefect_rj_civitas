# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake dos dados de câmeras do COR (Prefect 3).
"""

from typing import Literal, Any

from iplanrio.pipelines_utils.env import inject_bd_credentials_task, getenv_or_action
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task, log
from prefect import flow
from prefect.states import Completed
from prefect_rj_civitas import (
    config,
    run_deployment_task,
    skip_if_already_running,
    verify_secrets_task,
)

from pipelines.rj_civitas__cameras_cor.tasks import (
    fetch_cameras_task,
    upload_data_to_storage_task,
    create_external_storage_table_task
)


@flow(log_prints=True)
def rj_civitas__cameras_civitas(
    project_id: str = "rj-civitas",
    dataset_id: str = "cerco_digital",
    table_id: str = "cameras",
    gcs_path: str = "/cameras.csv",
    dbt_select: str = "cameras",
    materialize_after_dump: bool = True,
    mode: Literal["dev", "prod", "staging"] = "staging",
    github_repo: str = "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    gcs_buckets: dict[str, str] | None = {
            "prod": "rj-civitas_dbt",
            "dev": "rj-civitas-dev_dbt"
        },
    required_secrets: tuple[str, ...] = (
        "TIXXI_API_KEY",
        "TIXXI_API_EMAIL",
        "TIXXI_API_PASSWORD",
        "TIXXI_API_URL"
    )
):
    rename_current_flow_run_task(new_name=f"{table_id}-{mode}")

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    verify_secrets_task(secrets=required_secrets)

    tixxi_email = getenv_or_action("TIXXI_API_EMAIL", action="raise")
    tixxi_password = getenv_or_action("TIXXI_API_PASSWORD", action="raise")
    tixxi_url = getenv_or_action("TIXXI_API_URL", action="raise")
    tixxi_key = getenv_or_action("TIXXI_API_KEY", action="raise")

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    data = fetch_cameras_task(
            tixxi_url=tixxi_url,
            tixxi_key=tixxi_key,
            tixxi_email=tixxi_email,
            tixxi_password=tixxi_password
        )

    if not data:
        return Completed(
            message="No data returned by the API, finishing the flow.",
            name="Skipped",
        )

    upload_data_to_storage_task(
        project_id=project_id,
        bucket_id=bucket_id,
        gcs_path=gcs_path,
        data=data
    )

    create_external_storage_table_task(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        gcs_path=gcs_path,
        schema=bq_schema,
        file_format=file_format
    )

    if materialize_after_dump:
        dbt_target = "dev" if mode == "prod" else "staging"
        materialize_after_dump_parameters: dict[str, Any] = {
            "command": "build",
            "select": dbt_select,
            "send_discord_report": True,
            "github_repo": github_repo,
            "bigquery_project": project_id,
            "target": dbt_target,
            "gcs_buckets": gcs_buckets
        }

        materialize_after_dump_future = run_deployment_task.submit(
            name=config.run_dbt_deployment_name + "--" + mode,
            parameters=materialize_after_dump_parameters,
            timeout=None,
            as_subflow=False,
        )
        materialize_after_dump_run = materialize_after_dump_future.result()
        log(
            f"Materialize after dump deployment run: {materialize_after_dump_run.id}",
            level="info",
        )
