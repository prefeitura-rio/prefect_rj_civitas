# -*- coding: utf-8 -*-
"""
Tasks da pipeline Palver.
"""
import asyncio
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, List, Literal
import pytz
from zoneinfo import ZoneInfo

import re
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect_rj_civitas import (
    save_data_in_bq_table
)
from pipelines.rj_civitas__palver_eleicoes.utils import (
    is_token_valid,
    get_on_redis,
    auth,
    update_token_on_redis,
    get_data
)
from pipelines.rj_civitas__palver_eleicoes.schemas import get_source_schema

tz = pytz.timezone("America/Sao_Paulo")

@task
def resolve_incremental_date_task(
    project_id: str,
    dataset_id: str,
    table_id: str
    ):
    log(f"Getting the last charge datetime from {table_id}")
    try:
        table_full_name = f"{project_id}.{dataset_id}.{table_id}"

        client = bigquery.Client(project=project_id)

        query = f"""
            SELECT MAX(datetime) AS max_value
            FROM `{table_full_name}`
        """

        query_job = client.query(query)
        result = query_job.result()

        row = next(result)
        if row.max_value is None:
            return None

        ts_utc = row.max_value.astimezone(UTC) + timedelta(seconds=1)
        resolved_start_date = ts_utc.isoformat().replace("+00:00", "Z")
        log(f"Start date redefined to: {resolved_start_date}")
        return resolved_start_date
    except Exception as e:
        log(f"Error while searching for last charge datetime. Using start date predefined.\nDetails: {e}")
        return None


@task
def resolve_start_date_task(start_date: str | None, minutes_offset: int) -> str:
    """
    Resolves the start_date used for the API call.

    If `start_date` is provided, returns it unchanged. Otherwise, computes
    `(now in America/Sao_Paulo - minutes_offset minutes)` formatted as `YYYY-MM-DDTHH:`.

    This is evaluated at flow run time so the schedule does not need to
    embed a concrete date.
    """
    if start_date:
        try:
            dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))

            start_date = dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
            return start_date
        except ValueError as e:
            log(f"Value Error: start_date is in the wrong format. Expected: YYYY-MM-DD HH:mm:ss.")
            raise e

    resolved = (datetime.now(tz=UTC) - timedelta(minutes=minutes_offset)).strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"Resolved start_date dynamically: {resolved}", level="info")
    return resolved

@task
def get_palver_token_task(
    palver_email: str,
    palver_password: str,
    redis_password:  str | None = None
    ) -> str:
    """Returns a valid auth token, using cache when possible."""
    try:
        token_data = get_on_redis(
            dataset_id="palver",
            name="api_token",
            redis_password=redis_password,
        )

        if is_token_valid(token_data):
            log("Using cached token", level="info")
            return token_data["token"]

        log("Token expired or invalid. Requesting new token...", level="info")
    except Exception as e:
        log(f"Error accessing Redis: {e}\nRequesting new token...", level="warning")

    try:
        response = auth(palver_email, palver_password)
        log("Token obtained successfully", level="info")
    except Exception as e:
        log(f"Error obtaining valid token: {e}", level="error")
        raise

    try:
        update_token_on_redis(response, redis_password=redis_password)
        log("Token updated in Redis", level="info")
    except Exception as e:
        log(f"Failed to update token in Redis: {e}", level="warning")

    return response.json().get("token")


@task(retries=5, retry_delay_seconds=30)
def fetch_messages_task(
    start_date: str,
    end_date: str | None,
    docs_per_page: int,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter", "telegram"],
    query: str,
    palver_token: str
) -> List[Dict[str, Any]]:
    """
    Task that fetches messages from the Palver API.

    Reads `PALVER_BASE_URL`, `PALVER_TOKEN`
    from environment variables.
    """
    host = getenv_or_action("PALVER_BASE_URL", action="raise")

    if end_date:
        try:
            dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))

            end_date = dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            log(f"Value Error: end_date is in the wrong format. Expected: YYYY-MM-DD HH:mm:ss")
            raise e

    else:
        if source=="press":
            # The Press source truncates timestamps to the day. Set the end date to the end
            # of yesterday (UTC-3) to prevent today's data from being skipped by the incremental logic.
            end_date = datetime.now(tz=UTC).strftime("%Y-%m-%dT02:59:59Z")
        else:
            end_date = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    log(f"Fetching data from {source}\nStart Date: {start_date}\nEnd date: {end_date}", level="info")
    data = asyncio.run(
        get_data(
            host=host,
            token=palver_token,
            source=source,
            start_date=start_date,
            end_date=end_date,
            query=query,
            docs_per_page=docs_per_page
        )
    )

    log(f"Data from {source} fetched successfully.", level="info")
    return data


@task
def clean_text_task(
    data: List[Dict[str, Any]],
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter", "telegram"]
) -> List[Dict[str, Any]]:
    if source in ("radio.medias", "whatsapp", "television", "twitter", "telegram"):
        log("Cleaning transcription texts")
        for doc in data:
            if doc.get("transcript", ""):
                cleaned_text  = re.sub(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}|\n\d+\n|^\d+\n', '', doc["transcript"])
                doc["transcript"] = cleaned_text
        log("Transcriptions successfully cleaned")

    return data


@task(retries=5, retry_delay_seconds=30)
def load_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter", "telegram"],
    data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND"
) -> None:
    """
    Loads occurrences to a BigQuery table using the canonical schema.

    In `dev`/`staging` mode the destination project is suffixed with `-dev`.
    """
    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")

    schema = get_source_schema(source)
    partition_field = "c_processed_at" if source=="press" else "datetime"
    save_data_in_bq_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            schema=schema,
            data=data,
            write_disposition=write_disposition,
            partition_field=partition_field,
            partition_granularity="MONTH",
            clustering_fields=["id"],
            ignore_unknown_values=True,
            allow_field_addition=True,
            insert_timestamp_field="timestamp_insercao"
        )
    log(f"{len(data)} occurrences written to {project_id}.{dataset_id}.{table_id}")


@task
def get_dd_data(start_date: str):
    client = bigquery.Client()
    query_last_date = "SELECT MAX(data_denuncia) as ultima_data FROM `rj-civitas-dev.alerta_contexto_eleicoes.alerta_eleicoes_disque_denuncia`"
    last_date = start_date
    try:
        last_date_query_job = client.query(query_last_date)
        last_date_result = last_date_query_job.result()

        row = next(last_date_result, None)

        if row and row.ultima_data:
            last_date = row.ultima_data.strftime("%Y-%m-%d %H:%M:%S")
            log(f"Data de início do Disque Denúncia incremental: {last_date}")

        else:
            log(
                f"Não foi possível encontrar a data do último registro do "
                f"Disque Denúncia. Usando start_date: {start_date}."
            )

    except Exception as e:
        log(
            f"Não foi possível encontrar a data do último registro do "
            f"Disque Denúncia. Usando start_date: {start_date}. Erro: {e}"
        )

    query_data = f"""
SELECT
    id_denuncia,
    numero_denuncia,
    TIMESTAMP(data_denuncia) AS data_denuncia,
    relato,
    tipo_logradouro,
    logradouro,
    numero_logradouro,
    complemento_logradouro,
    referencia_logradouro,
    municipio,
    bairro_logradouro,
    estado,
    latitude,
    longitude,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(
          COALESCE(assunto.classe, ''), ': ', COALESCE(tipo.tipo, ''),
          IF(tipo.assunto_principal = 1, ' (principal)', '')
        )
        FROM UNNEST(assuntos) AS assunto, UNNEST(assunto.tipos) AS tipo
      ),
      ' | '
    ) AS assuntos_tipos
  FROM `rj-civitas.disque_denuncia.denuncias`
  WHERE data_denuncia > '{last_date}'
    AND relato IS NOT NULL
"""

    try:
        data_query_job = client.query(query_data)
        data_result = data_query_job.result()

        data = []

        for row in data_result:
            item = dict(row)

            if item.get("data_denuncia"):
                item["data_denuncia"] = item["data_denuncia"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

            data.append(item)
        log(f"Foram retornados {len(data)} registros do Disque Denúncia")


        return data

    except Exception as e:
        log(
            f"Erro ao consultar dados do Disque Denúncia. Erro: {e}"
        )
        return None
