# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake dos dados de câmeras da CIVITAS (Prefect 3).
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

from pipelines.rj_civitas__cameras_civitas.tasks import (
    get_smart_token_task,
    fetch_cameras_task,
    load_to_table_task
)


@flow(log_prints=True)
def rj_civitas__cameras_civitas(
    project_id: str = "rj-civitas",
    dataset_id: str = "cerco_digital",
    table_id: str = "cameras_civitas",
    dbt_select: str = "cameras_civitas cameras",
    materialize_after_dump: bool = False,
    mode: Literal["dev", "prod", "staging"] = "staging",
    github_repo: str = "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    gcs_buckets: dict[str, str] | None = None,
    required_secrets: tuple[str, ...] = (
        "SMART_USERNAME",
        "SMART_PASSWORD",
        "SMART_URL"
    )
):
    rename_current_flow_run_task(new_name=f"{table_id}-{mode}")

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    verify_secrets_task(secrets=required_secrets)

    smart_email = getenv_or_action("SMART_USERNAME", action="raise")
    smart_password = getenv_or_action("SMART_PASSWORD", action="raise")
    smart_url = getenv_or_action("SMART_URL", action="raise")
    smart_token = get_smart_token_task(smart_url=smart_url, smart_email=smart_email, smart_password=smart_password)

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    data = fetch_cameras_task(
            smart_url=smart_url,
            smart_token=smart_token
        )

    if not data:
        return Completed(
            message="No data returned by the API, finishing the flow.",
            name="Skipped",
        )

    load_to_table_task(
            project_id=project_id,
            dataset_id=f"{dataset_id}_staging",
            table_id=table_id,
            data=data,
            write_disposition="WRITE_TRUNCATE"
        )

    if materialize_after_dump:
        materialize_after_dump_parameters: dict[str, Any] = {
            "command": "build",
            "select": dbt_select,
            "send_discord_report": True,
            "github_repo": github_repo,
            "bigquery_project": project_id,
            "target": "dev",
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
