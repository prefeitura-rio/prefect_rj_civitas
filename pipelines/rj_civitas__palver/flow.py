# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake dos dados da Palver (Prefect 3).
"""

from dotenv import load_dotenv
from os import environ
from typing import Literal

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

from pipelines.rj_civitas__palver.tasks import (
    fetch_messages_task,
    load_to_table_task,
    resolve_start_date_task,
)


@flow(log_prints=True)
def rj_civitas__palver(
    project_id: str = "rj-civitas",
    dataset_id: str = "palver",
    sources: list[Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]] = ["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    docs_per_page: int = 100,
    start_date: str | None = None,
    days_offset: int = 30,
    query: str = "tiroteio",
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    materialize_after_dump: bool = True,
    mode: Literal["dev", "prod", "staging"] = "prod",
    github_repo: str = "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    gcs_buckets: dict[str, str] | None = None,
    required_secrets: tuple[str, ...] = (
        "PALVER_BASE_URL",
        "PALVER_TOKEN"
    )
):
    rename_current_flow_run_task(new_name=f"{write_disposition}_{dataset_id}_messages-{mode}")

    if skip := skip_if_already_running():
        return skip
    
    if mode == "dev":
        load_dotenv()
        environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"
        log("INJECTED: GCP credentials from service account")
    else:
        inject_bd_credentials_task(environment="prod")

    verify_secrets_task(secrets=required_secrets)

    resolved_start_date = resolve_start_date_task(start_date, days_offset)

    for source in sources:
        table_id = f"palver_{source.replace('.', '_')}_messages"
        data = fetch_messages_task(
            start_date=resolved_start_date,
            docs_per_page=docs_per_page,
            source=source,
            query=query
        )

        if not data:
            log(f"No data from {source} returned by the API.")
            continue

        load_to_table_task(
            project_id=project_id,
            dataset_id=f"{dataset_id}_staging",
            table_id=table_id,
            source=source,
            data=data,
            write_disposition=write_disposition,
            mode=mode,
        )
