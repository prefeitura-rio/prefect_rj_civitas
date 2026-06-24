# -*- coding: utf-8 -*-
"""
CIVITAS — Extração e carga no datalake dos dados da Palver (Prefect 3).
"""

from dotenv import load_dotenv
from os import environ
from typing import Literal, Any

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import log, rename_current_flow_run_task
from prefect import flow
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
    resolve_incremental_date_task,
    clean_text_task,
    enrich_with_tags_task,
    llm_enrich_task,
    get_geolocation_task
)


@flow(log_prints=True)
def rj_civitas__palver(
    project_id: str = "rj-civitas",
    dataset_id: str = "palver",
    sources: list[Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]] = ["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    docs_per_page: int = 100,
    incremental: bool = True,
    start_date: str | None = None,
    end_date: str | None = None,
    minutes_offset: int = 1440,
    query: str = "tiroteio~ OR assalt* OR tr?fic* OR mil?ci* OR furtou OR furtaram OR homicídio~ OR furtou OR furtaram OR feminicídio OR latrocínio",
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    llm_model: str = "gemini-2.5-flash",
    materialize_after_dump: bool = True,
    mode: Literal["dev", "prod", "staging"] = "staging",
    github_repo: str = "https://github.com/prefeitura-rio/pipelines_rj_civitas",
    required_secrets: tuple[str, ...] = (
        "PALVER_BASE_URL",
        "PALVER_TOKEN"
    )
):
    rename_current_flow_run_task(new_name=f"{write_disposition}_{dataset_id}_messages-{mode}")

    if skip := skip_if_already_running():
        return skip
    
    inject_bd_credentials_task(environment="prod")

    verify_secrets_task(secrets=required_secrets)

    google_maps_api_key = environ["GOOGLE_MAPS_API_KEY"]

    resolved_start_date = resolve_start_date_task(start_date, minutes_offset)

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    for source in sources:
        table_id = f"palver_{source.replace('.', '_')}_messages"

        if incremental:
            incremental_date = resolve_incremental_date_task(
                project_id=project_id,
                dataset_id=f"{dataset_id}_staging",
                table_id=table_id
            )
            if incremental_date:
                resolved_start_date = incremental_date

        data = fetch_messages_task(
            start_date=resolved_start_date,
            end_date=end_date,
            docs_per_page=docs_per_page,
            source=source,
            query=query
        )

        if not data:
            log(f"No data from {source} returned by the API.")
            continue
        
        data = clean_text_task(source=source, data=data)

        data = enrich_with_tags_task(source=source, data=data)

        data = llm_enrich_task(source=source, data=data, model=llm_model)

        data = get_geolocation_task(source=source, data=data, google_maps_api_key=google_maps_api_key)

        load_to_table_task(
            project_id=project_id,
            dataset_id=f"{dataset_id}_staging",
            table_id=table_id,
            source=source,
            data=data,
            write_disposition=write_disposition
        )

    if materialize_after_dump:
        dbt_select = dataset_id
        materialize_after_dump_parameters: dict[str, Any] = {
            "command": "build",
            "select": dbt_select,
            "send_discord_report": True,
            "github_repo": github_repo,
            "bigquery_project": project_id,
            "target": "dev"
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

