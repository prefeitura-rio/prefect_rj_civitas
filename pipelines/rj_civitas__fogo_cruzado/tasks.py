# -*- coding: utf-8 -*-
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

from pipelines.rj_civitas__fogo_cruzado.utils import (
    get_occurrences,
    get_valid_token,
    safe_float_conversion,
    save_data_in_bq,
)

tz = pytz.timezone("America/Sao_Paulo")


@task
def resolve_start_date_task(start_date: str | None = None, days_offset: int = 30) -> str:
    """
    Resolves the start_date used for the API call.

    If `start_date` is provided, returns it unchanged. Otherwise, computes
    `(now in America/Sao_Paulo - days_offset days)` formatted as `YYYY-MM-DD`.

    This is evaluated at flow run time so the schedule does not need to
    embed a concrete date.
    """
    if start_date:
        return start_date
    resolved = (datetime.now(tz=tz) - timedelta(days=days_offset)).strftime("%Y-%m-%d")
    log(f"Resolved start_date dynamically: {resolved}", level="info")
    return resolved


@task(retries=5, retry_delay_seconds=30)
def fetch_occurrences_task(
    start_date: str | None = None,
    take: int = 100,
) -> List[Dict[str, Any]]:
    """
    Task that fetches occurrences from the Fogo Cruzado API.

    Reads `FOGOCRUZADO_USERNAME`, `FOGOCRUZADO_PASSWORD`
    from environment variables. Latitude/longitude are coerced to floats.
    """
    email = getenv_or_action("FOGOCRUZADO_USERNAME", action="raise")
    password = getenv_or_action("FOGOCRUZADO_PASSWORD", action="raise")

    token = get_valid_token(email=email, password=password)

    log("Fetching data...", level="info")
    occurrences = asyncio.run(
        get_occurrences(
            token=token,
            initial_date=start_date,
            take=take,
        )
    )

    for row in occurrences:
        for key in ["latitude", "longitude"]:
            row[key] = safe_float_conversion(row[key])

    log("Data fetched successfully.", level="info")
    return occurrences


_OCCURRENCES_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
    bigquery.SchemaField(name="documentNumber", field_type="STRING", mode="NULLABLE"),
    bigquery.SchemaField(name="address", field_type="STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        name="state",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=(
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        name="region",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=(
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="region", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="state", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="enabled", field_type="STRING", mode="NULLABLE"),
        ),
    ),
    bigquery.SchemaField(
        name="city",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        name="neighborhood",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        name="subNeighborhood",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        name="locality",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(name="latitude", field_type="FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField(name="longitude", field_type="FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField(name="date", field_type="TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField(name="policeAction", field_type="STRING", mode="NULLABLE"),
    bigquery.SchemaField(name="agentPresence", field_type="STRING", mode="NULLABLE"),
    bigquery.SchemaField(name="relatedRecord", field_type="STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        name="contextInfo",
        field_type="STRUCT",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField(
                name="mainReason",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="complementaryReasons",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="clippings",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(name="massacre", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="policeUnit", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        name="transports",
        field_type="STRUCT",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="occurrenceId", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="transport",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="interruptedTransport", field_type="STRING", mode="NULLABLE"
            ),
            bigquery.SchemaField(name="dateInterruption", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="releaseDate", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="transportDescription", field_type="STRING", mode="NULLABLE"
            ),
        ],
    ),
    bigquery.SchemaField(
        name="victims",
        field_type="STRUCT",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="occurrenceId", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="situation", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="circumstances",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(name="deathDate", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="personType", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="age", field_type="INTEGER", mode="NULLABLE"),
            bigquery.SchemaField(
                name="ageGroup",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="genre",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(name="race", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="place",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="serviceStatus",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="qualifications",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="politicalPosition",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="politicalStatus",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="partie",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="coorporation",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="agentPosition",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                name="agentStatus",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(name="unit", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        name="animalVictims",
        field_type="STRUCT",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="occurrenceId", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="animalType",
                field_type="STRUCT",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(name="situation", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                name="circumstances",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="name", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="type", field_type="STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(name="deathDate", field_type="STRING", mode="NULLABLE"),
        ],
    ),
    bigquery.SchemaField(
        name="timestamp_insercao",
        field_type="DATETIME",
        mode="NULLABLE",
        description="Data e hora de inserção no BD em GTM-3",
    ),
]


@task(retries=5, retry_delay_seconds=30)
def load_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    occurrences: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
    mode: Literal["dev", "prod", "staging"] = "prod",
) -> None:
    """
    Loads occurrences to a BigQuery table using the canonical schema.

    In `dev`/`staging` mode the destination project is suffixed with `-dev`
    to mirror the v1 behaviour driven by `get_flow_run_mode()`.
    """
    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")
    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=_OCCURRENCES_SCHEMA,
        json_data=occurrences,
        write_disposition=write_disposition,
    )
    log(f"{len(occurrences)} occurrences written to {project_id}.{dataset_id}.{table_id}")
