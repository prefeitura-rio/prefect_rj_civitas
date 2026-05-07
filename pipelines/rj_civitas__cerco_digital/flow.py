# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

from typing import Literal, Optional

from iplanrio.pipelines_templates.dump_db.tasks import (
    dump_upload_batch_task,
    format_partitioned_query_task,
    get_database_username_and_password_from_secret_task,
    parse_comma_separated_string_to_list_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import log, rename_current_flow_run_task
from prefect import flow
from pipelines.rj_civitas__cerco_digital.tasks import add_default_start_date_var_to_dbt_flags
from prefect_rj_civitas import config, run_deployment_task, skip_if_already_running


@flow(log_prints=True)
def rj_civitas__cerco_digital(
    # GCP parameters
    gcs_buckets: dict[str, str],
    # DBT parameters
    github_repo: str,
    command: str,
    bigquery_project: str,
    select: str | None = None,
    exclude: str | None = None,
    flag: str | None = None,
    target: str = "dev",
    # Flow parameters
    send_discord_report: bool = False,
    flow_run_name: str | None = None,
    mode: Literal["dev", "prod", "staging"] = "prod",
):
    rename_current_flow_run_task(new_name="ELT_cerco_digital")

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    dbt_flags = add_default_start_date_var_to_dbt_flags(flag=flag)

    dbt_run = run_deployment_task(
        name=config.run_dbt_deployment_name + "--" + mode,
        parameters={
            "gcs_buckets": gcs_buckets,
            "github_repo": github_repo,
            "command": command,
            "bigquery_project": bigquery_project,
            "select": select,
            "exclude": exclude,
            "flag": dbt_flags,
            "target": target,
            "send_discord_report": send_discord_report,
        },
        timeout=0,
        as_subflow=False,
    )
    log(f"Cerco Digital deployment run: {dbt_run.id}", level="info")