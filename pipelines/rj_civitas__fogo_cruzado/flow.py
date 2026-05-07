# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake do Fogo Cruzado (Prefect 3).

Migrado de pipelines_rj_civitas Prefect 1.4 (fogo_cruzado/extract_load):
- Flow/Parameter/case → @flow + if; schedule em prefect.yaml.
- state_handlers (inject_bd / skip_if_running) → chamadas explícitas.
- create_flow_run/wait_for_flow_run → run_deployment_task.
- check_report_qty (ENDRUN+Skipped) → return Completed(name="Skipped") quando lista vazia.
"""


from typing import Any, Literal

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import log, rename_current_flow_run_task
from prefect import flow
from prefect.states import Completed
from prefect_rj_civitas import (
    config,
    run_deployment_task,
    skip_if_already_running,
    verify_secrets_task,
)

from pipelines.rj_civitas__fogo_cruzado.tasks import (
    fetch_occurrences_task,
    load_to_table_task,
    resolve_start_date_task,
)


@flow(log_prints=True)
def rj_civitas__fogo_cruzado(
    project_id: str = "rj-civitas",
    dataset_id: str = "fogo_cruzado",
    table_id: str = "ocorrencias",
    prefix: str = "PARTIAL_REFRESH_",
    take: int = 100,
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    materialize_after_dump: bool = True,
    materialize_reports_fc_after_dump: bool = True,
    mode: Literal["dev", "prod", "staging"] = "prod",
    github_repo: str = "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    gcs_buckets: dict[str, str] | None = None,
    required_secrets: tuple[str, ...] = (
        "FOGOCRUZADO_USERNAME",
        "FOGOCRUZADO_PASSWORD",
        "REDIS_HOST",
    ),
) -> Any:
    rename_current_flow_run_task(new_name=f"{prefix}{dataset_id}_{table_id}")

    if skip := skip_if_already_running():
        return skip

    verify_secrets_task(secrets=required_secrets)
    inject_bd_credentials_task(environment="prod")

    resolved_start_date = resolve_start_date_task(days_offset=30)

    occurrences = fetch_occurrences_task(
        start_date=resolved_start_date,
        take=take,
    )

    if not occurrences:
        return Completed(
            message="No data returned by the API, finishing the flow.",
            name="Skipped",
        )

    load_to_table_task(
        project_id=project_id,
        dataset_id=f"{dataset_id}_staging",
        table_id=table_id,
        occurrences=occurrences,
        write_disposition=write_disposition,
        mode=mode,
    )

    if materialize_after_dump:
        dbt_select = "+reports_fogo_cruzado" if materialize_reports_fc_after_dump else dataset_id
        materialize_after_dump_parameters: dict[str, Any] = {
            "command": "build",
            "select": dbt_select,
            "send_discord_report": True,
            "github_repo": github_repo,
            "bigquery_project": project_id,
            "target": "dev",
            "gcs_buckets": gcs_buckets,
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
