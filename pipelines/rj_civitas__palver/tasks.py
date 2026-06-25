"""
Tasks da pipeline Fogo Cruzado.
"""
import asyncio
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, List, Literal
from zoneinfo import ZoneInfo
import os

import re
from google import genai
from google.oauth2 import service_account
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_civitas__palver.utils import (
    is_token_valid,
    get_on_redis,
    auth,
    update_token_on_redis,
    get_data,
    get_geolocation,
    save_data_in_bq,
    llm_extract_relevance_and_locations_from_text
)
from pipelines.rj_civitas__palver.schemas import get_source_schema, get_source_text_fields


@task
def resolve_incremental_date_task(
    project_id: str, 
    dataset_id: str, 
    table_id: str
    ):    
    log(f"Getting the last charge datetime from {table_id}")
    try:
        client = bigquery.Client()
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
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    query: str,
    palver_token: str
) -> List[Dict[str, Any]]:
    """
    Task that fetches messages from the Palver API.

    Reads `PALVER_BASE_URL`, `PALVER_TOKEN`
    from environment variables. 
    """
    host = getenv_or_action("PALVER_BASE_URL", action="raise")

    try:
        dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))

        end_date = dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    except:
        if source=="press":
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


def clean_text_task(
    data: List[Dict[str, Any]],
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]
) -> List[Dict[str, Any]]:
    if source in ("radio.medias", "whatsapp", "television"):
        log("Cleaning transcription texts")
        for doc in data:
            if doc.get("transcript", ""):
                cleaned_text  = re.sub(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}|\n\d+\n|^\d+\n', '', doc["transcript"])
                doc["transcript"] = cleaned_text 
        log("Transcriptions successfully cleaned")

    if source=="whatsapp":
        for doc in data:
            if not doc.get("text", "") and doc.get("transcript", ""):
                doc["text"] = doc["transcript"]

    return data


def enrich_with_tags_task(
    data: List[Dict[str, Any]],
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]
) -> List[Dict[str, Any]]:
    """
    Task that extracts the main topics from the text 
    """
    brute_tags = [
    (
        "tiroteio", 
        r"\btiroteios?\b|\btroca de tiros\b|\bbalead[ao]s?\b|\bdispar(?:os?|ou|aram)\b"
    ),
    (
        "assalto", 
        r"\bassalt(?:os?|as?|antes?|ados?|adas?|ou|aram)\b"
    ),
    (
        "homicídio", 
        r"\bassassin(?:atos?|ou|aram)\b|\bhomic[ií]d(?:ios?|as?)\b|\bmat(?:ou|aram)\b"
    ),
    (
        "roubo", 
        r"\broub(?:os?|as?|ou|aram|ar)\b|\bladr(?:ão|ões)\b|\bfurt(?:os?|ados?|adas?|ou|aram|ar)\b"
    ),
    (
        "milícia", 
        r"\bmil[ií]cias?\b|\bmilicianos?\b"
    ),
    (
        "tráfico", 
        r"\btr[aá]fic[oa]s?\b|\btraficantes?\b|\bentorpecentes?\b"
    )
]
    compiled_tags = [(name, re.compile(pattern, re.IGNORECASE)) for name, pattern in brute_tags]

    log(f"Extracting tags from {source} messages text...")
    
    text_fields = get_source_text_fields(source)
    for doc in data:
        found_tags = set()

        lines = []
        for field in text_fields:
            value = doc.get(field, "")
            if value:
                lines.append(value)        
        text = "\n".join(lines)

        if not text:
            doc["tags"] = []
            continue

        for tag in compiled_tags:
            if tag[1].search(text):
                found_tags.add(tag[0])
        doc["tags"] = list(found_tags)        

    log(f"Tags from {source} messages successfully extracted")
    return data


@task(retries=2, retry_delay_seconds=60)
def llm_enrich_task(
    model: str,
    data: List[Dict[str, Any]],
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
) -> List[Dict[str, Any]]:
    credentials = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    
    client = genai.Client(
        vertexai=True,
        project=credentials.project_id,
        location="us-central1",
        credentials=credentials
    )

    results = asyncio.run(
        llm_extract_relevance_and_locations_from_text(client, model, source, data)
    )
    
    return results


@task(retries=2, retry_delay_seconds=60)
def get_geolocation_task(
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    data: List[Dict[str, Any]],
    google_maps_api_key: str,
) -> List[Dict[str, Any]]:
    """
    Task that fetches the geolocation of the main location from the Google Maps API.
    """
    log(f"Getting the best geolocation from each message of {source}...")
    for doc in data:
        main_location = doc.get("main_location", "")
        if not main_location:
            continue

        geolocation= get_geolocation(search_text=main_location, google_maps_api_key=google_maps_api_key)
        if not geolocation:
            continue

        doc["main_location_full_address"] = geolocation.get("full_address", "")
        doc["city"] = geolocation.get("city", "")
        doc["latitude"] = geolocation.get("latitude", "")
        doc["longitude"] = geolocation.get("longitude", "")

    log(f"Got all geolocations from {source}")
    return data


@task(retries=5, retry_delay_seconds=30)
def load_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND"
) -> None:
    """
    Loads occurrences to a BigQuery table using the canonical schema.

    In `dev`/`staging` mode the destination project is suffixed with `-dev`.
    """
    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")

    schema = get_source_schema(source)
    save_data_in_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema=schema,
        json_data=data,
        write_disposition=write_disposition,
        source=source
    )
    log(f"{len(data)} occurrences written to {project_id}.{dataset_id}.{table_id}")
