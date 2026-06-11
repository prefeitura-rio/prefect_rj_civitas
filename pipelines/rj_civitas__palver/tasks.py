"""
Tasks da pipeline Fogo Cruzado.
"""
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal

import pytz
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_civitas__palver.utils import (
    get_data,
    save_data_in_bq
)
from pipelines.rj_civitas__palver.schemas import get_source_schema

tz = pytz.timezone("America/Sao_Paulo")


@task
def resolve_start_date_task(start_date: str | None, days_offset: int) -> str:
    """
    Resolves the start_date used for the API call.

    If `start_date` is provided, returns it unchanged. Otherwise, computes
    `(now in America/Sao_Paulo - days_offset days)` formatted as `YYYY-MM-DD`.

    This is evaluated at flow run time so the schedule does not need to
    embed a concrete date.
    """
    if start_date:
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").strftime('%Y-%m-%d')
            return start_date
        except ValueError as e:
            log(f"Value Error: start_date is in the wrong format. Expected: YYYY-mm-dd.")
            raise e

    resolved = (datetime.now(tz=tz) - timedelta(days=days_offset)).strftime("%Y-%m-%d")
    log(f"Resolved start_date dynamically: {resolved}", level="info")
    return resolved
    

@task(retries=5, retry_delay_seconds=30)
def fetch_messages_task(
    start_date: str,
    docs_per_page: int,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    query: str
) -> List[Dict[str, Any]]:
    """
    Task that fetches messages from the Palver API.

    Reads `PALVER_BASE_URL`, `PALVER_TOKEN`
    from environment variables. 
    """
    host = getenv_or_action("PALVER_BASE_URL", action="raise")
    token = getenv_or_action("PALVER_TOKEN", action="raise")

    log("Fetching data...", level="info")
    data = asyncio.run(
        get_data(
            host=host,
            token=token,
            source=source,
            initial_date=start_date,
            query=query,
            docs_per_page=docs_per_page
        )
    )

    log("Data fetched successfully.", level="info")
    return data


@task(retries=5, retry_delay_seconds=30)
def load_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    source: str,
    data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    mode: Literal["dev", "prod", "staging"] = "prod",
) -> None:
    """
    Loads occurrences to a BigQuery table using the canonical schema.

    In `dev`/`staging` mode the destination project is suffixed with `-dev`.
    """
    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")

    schema = get_source_schema(source)
    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=schema,
        json_data=data,
        write_disposition=write_disposition,
    )
    log(f"{len(data)} occurrences written to {project_id}.{dataset_id}.{table_id}")
