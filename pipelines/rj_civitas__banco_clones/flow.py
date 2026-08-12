# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
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
    verify_secrets_task
)

from pipelines.rj_civitas__banco_clones.tasks import get_readings_task, get_tracks_task, upload_to_table_task


@flow(log_prints=True)
def rj_civitas__banco_clones(
    project_id: str = "rj-civitas",
    clones_dataset_id: str = "banco_clones",
    readings_dataset_id: str = "cerco_digital",
    readings_table_id: str = "vw_all_readings",
    banco_clones_table_id: str = "banco_clones_dia",
    pares_suspeitos_table_id: str = "pares_suspeitos",
    trilhas_table_id: str = "trilhas_clones_dia",
    dbt_select: str = "banco_clones_staging banco_clones",
    mode: Literal["dev", "prod", "staging"] = "staging",
    start_date: str = "2026-08-01",
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    github_repo: str = "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    gcs_buckets: dict[str, str] | None = {
        "prod": "rj-civitas_dbt",
        "dev": "rj-civitas-dev_dbt"
    },
    required_secrets: tuple[str, ...] = (
    )
):
    rename_current_flow_run_task(new_name=f"banco_clones-{mode}")

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    verify_secrets_task(secrets=required_secrets)

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"
#        dbt_target = "staging"
#    else:
#        dbt_target = "dev"
#
#    dbt_run_parameters: dict[str, Any] = {
#        "command": "build",
#        "select": dbt_select,
#        "send_discord_report": True,
#        "github_repo": github_repo,
#        "bigquery_project": project_id,
#        "target": dbt_target,
#        "flag": f"--vars '{{\"start_date\": \"{start_date}\"}}'",
#        "gcs_buckets": gcs_buckets
#    }
#
#    dbt_run = run_deployment_task.submit(
#        name=config.run_dbt_deployment_name + "--" + mode,
#        parameters=dbt_run_parameters,
#        timeout=None,
#        as_subflow=False,
#    )
#    materialize_after_dump_run = dbt_run.result()
#    log(
#        f"Materialize after dump deployment run: {materialize_after_dump_run.id}",
#        level="info",
#    )

    readings_full_table_id = f"rj-civitas.{readings_dataset_id}.{readings_table_id}"
    trilhas_full_table_id = f"{project_id}.{clones_dataset_id}.{trilhas_table_id}"
    banco_clones_full_table_id = f"{project_id}.{clones_dataset_id}.{banco_clones_table_id}"
    pares_suspeitos_full_table_id = f"{project_id}.{clones_dataset_id}_staging.{pares_suspeitos_table_id}"
    plate_readings_day = get_readings_task(
        start_date=start_date,
        readings_full_table_id=readings_full_table_id,
        banco_clones_full_table_id=banco_clones_full_table_id,
        trilhas_full_table_id=trilhas_full_table_id,
        pares_suspeitos_full_table_id=pares_suspeitos_full_table_id
    )

    if not plate_readings_day:
        return Completed(
                message="No fresh clone suspects detected, finishing the flow.",
                name="Skipped",
            )

    plate_tracks_day = get_tracks_task(plate_readings_day)

    upload_to_table_task(
        project_id=project_id,
        dataset_id=f"{clones_dataset_id}",
        table_id=trilhas_table_id,
        data=plate_tracks_day,
        write_disposition=write_disposition
    )

