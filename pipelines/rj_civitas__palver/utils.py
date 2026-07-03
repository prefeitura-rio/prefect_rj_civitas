# -*- coding: utf-8 -*-
"""
Helpers para a pipeline Palver.

Inclui fetch assíncrono de ocorrências e escrita em BigQuery.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

import aiohttp
import googlemaps
import pandas as pd
import pytz
import requests
import urllib3
from redis_pal import RedisPal
from google.cloud import bigquery
from google import genai
from google.genai import types
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log, log_mod

from pipelines.rj_civitas__palver.schemas import get_source_parameters, get_source_text_fields, LLMGeoSchema

tz = pytz.timezone("America/Sao_Paulo")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_redis_client(
    host: str | None = None,
    port: int = 6379,
    db: int = 0,
    password: str | None = None,
) -> RedisPal:
    """
    Returns a Redis client.

    Host must be provided either explicitly or via the REDIS_HOST env var.
    """
    host = host or getenv_or_action("REDIS_HOST", action="raise")
    return RedisPal(host=host, port=port, db=db, password=password)


def build_redis_key(
    dataset_id: str,
    name: str | None = None,
    mode: Literal["dev", "prod"] = "prod",
) -> str:
    """Constructs a Redis key from dataset, table and optional name."""
    key = dataset_id
    if name:
        key = f"{key}.{name}"
    if mode == "dev":
        key = f"{mode}.{key}"
    return key


def get_on_redis(
    dataset_id: str,
    name: str | None = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str | None = None,
) -> Any:
    """Retrieves a value from Redis based on dataset/table/name."""
    redis_client = get_redis_client(password=redis_password)
    key = build_redis_key(dataset_id, name, mode)
    return redis_client.get(key)


def save_on_redis(
    data: Any,
    dataset_id: str,
    name: str | None = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str | None = None,
) -> None:
    """Saves a value to Redis based on dataset/table/name."""
    redis_client = get_redis_client(password=redis_password)
    key = build_redis_key(dataset_id, name, mode)
    redis_client.set(key, data)


def update_token_on_redis(data: requests.Response, redis_password: str | None = None) -> None:
    """Updates the cached token in Redis with its expiration date."""
    request_date_str: str = data.headers.get("date")
    request_date_obj: datetime = pd.to_datetime(request_date_str)

    expires_at: datetime = request_date_obj + timedelta(
        seconds=data.json().get("expiry", {})
    )
    expires_at_str: str = expires_at.strftime("%Y-%m-%d %H:%M:%S")

    payload: dict = data.json()
    payload.update({"expiresAt": expires_at_str})

    save_on_redis(
        dataset_id="palver",
        name="api_token",
        data=payload,
        redis_password=redis_password
    )


def is_token_valid(token_data: Optional[Dict[str, Any]]) -> bool:
    """Checks if the API token cached in Redis is still valid."""
    if not token_data:
        return False

    access_token = token_data.get("token")
    expires_at_str = token_data.get("expiresAt")
    if not all([access_token, expires_at_str]):
        return False

    try:
        expires_at = datetime.strptime(expires_at_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        return expires_at > datetime.now(tz=timezone.utc)
    except ValueError as e:
        raise Exception(f"Error parsing expiration date: {e}")


def auth(email: str, password: str) -> requests.Response:
    """Authenticates against the Fogo Cruzado API."""
    host = getenv_or_action("PALVER_BASE_URL", action="raise")
    endpoint = "/auth"
    payload = {"email": email, "password": password}
    headers = {"Content-Type": "application/json"}

    response = requests.post(host + endpoint, json=payload, headers=headers, verify=False)
    response.raise_for_status()
    return response


async def get_data(
    host: str,
    token: str,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    start_date: str,
    end_date: str,
    query: str,
    docs_per_page: int,
    max_concurrent: int = 5,
    delay_between_requests: float = 5,
) -> List[Dict]:
    """Fetches occurrences from the Palver API asynchronously with rate limiting."""
    params = get_source_parameters(source)
    params["query"] = query
    params["perPage"] = docs_per_page
    params["startDate"] = f"{start_date}"
    params["endDate"] = f"{end_date}"

    headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8"
        }
    api_url = f"{host}/{source}/messages"

    async with aiohttp.ClientSession() as session:
        log("Getting total pages from API...", level="info")
        params["page"] = 1
        async with session.get(api_url, headers=headers, params=params) as response:
            if response.status == 429:
                log("Rate limited on first request. Waiting 5 seconds...", level="info")
                await asyncio.sleep(5)
                async with session.get(
                    api_url, headers=headers, params=params, ssl=False
                ) as response:
                    response.raise_for_status()
                    initial_data = await response.json()
            else:
                response.raise_for_status()
                initial_data = await response.json()

            total_pages = initial_data["meta"]["totalPages"]
            docs = initial_data["data"]

        log(f"Total pages to fetch: {total_pages}", level="info")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_page_with_retry(session, page, retries=3):
            async with semaphore:
                for attempt in range(retries):
                    try:
                        params["page"] = page
                        async with session.get(
                            api_url, headers=headers, params=params
                        ) as response:
                            if response.status == 429:
                                wait_time = 2**attempt
                                log(
                                    f"Rate limited on page {page}, attempt {attempt + 1}. "
                                    f"Waiting {wait_time}s...",
                                    level="info",
                                )
                                await asyncio.sleep(wait_time)
                                continue

                            response.raise_for_status()
                            data = await response.json()

                            if delay_between_requests > 0:
                                await asyncio.sleep(delay_between_requests)

                            log_mod(
                                f"Page {page} fetched successfully.",
                                level="info",
                                index=page,
                                mod=10,
                            )
                            return data["data"]

                    except Exception as e:
                        if attempt == retries - 1:
                            log(
                                f"Failed to fetch page {page} after {retries} attempts: {e}",
                                level="error",
                            )
                            return []
                        log(
                            f"Error on page {page}, attempt {attempt + 1}: {e}",
                            level="warning",
                        )
                        await asyncio.sleep(2**attempt)

        tasks = [fetch_page_with_retry(session, page) for page in range(2, total_pages + 1)]
        log(
            f"Fetching {len(tasks)} pages with max {max_concurrent} concurrent requests...",
            level="info",
        )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_pages: list[tuple[int, str]] = []
        successful_pages = 0

        for i, page_data in enumerate(results):
            page_num = i + 2
            if isinstance(page_data, list):
                docs.extend(page_data)
                successful_pages += 1
            else:
                failed_pages.append((page_num, str(page_data)))

        if failed_pages:
            error_msg = f"Failed to fetch {len(failed_pages)} pages after retries: {failed_pages}"
            log(f"ERROR: {error_msg}", level="error")
            raise Exception(error_msg)

        log(
            f"Data collected from API successfully. {successful_pages} pages loaded.",
            level="info",
        )
        return docs


async def llm_extract_single_text(
        semaphore: asyncio.Semaphore,
        client: genai.Client,
        model: str,
        source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
        text: str,
        doc: Dict[str, Any]) -> Optional[LLMGeoSchema]:
    """Função assíncrona que analisa a relevância do texto e extrai suas informações geográficas"""
    text_type = "o post de rede social" if source in ("whatsapp", "twitter") else "a notícia"
    prompt = f"Analise {text_type} sobre segurança pública abaixo. Extraia as informações geográficas exigidas estritamente de acordo com o esquema JSON fornecido.\n\nTexto:\n{text}"

    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=LLMGeoSchema.model_json_schema(),
        temperature=0.1
    )
    async with semaphore:
        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=prompt,
                config=config,
            )
            result = LLMGeoSchema.model_validate_json(response.text)
            if result:
                doc["is_relevant"] = result.is_relevant
                doc["locations"] = result.locations
                doc["main_location"] = result.main_location
            return doc
        except Exception as e:
            log(f"Error while proccessing text: {e}")
            return doc



async def llm_extract_relevance_and_locations_from_text(
        client: genai.Client,
        model: str,
        source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
        data: List[Dict[str, Any]]):
    """Envelopa a chamada da API usando o semáforo para limitar acessos simultâneos"""
    print(f"Starting geographic data extraction of  {len(data)} texts from {source}...")
    semaphore = asyncio.Semaphore(5)

    text_fields = get_source_text_fields(source)
    extractions = []
    for doc in data:
        lines = []
        for field in text_fields:
            value = doc.get(field, "")
            if value:
                lines.append(value)

        text = "\n".join(lines)

        if not text:
            extractions.append(asyncio.sleep(0, result=doc))
            continue

        task = llm_extract_single_text(semaphore, client, model, source, text, doc)
        extractions.append(task)

    results = await asyncio.gather(*extractions)
    return results


def get_geolocation_from_cache(
        key: str,
        local_geolocation_cache: dict,
        bq_geolocation_cache_table: str):
    local_cached_data = local_geolocation_cache.get(key, None)
    if local_cached_data:
        return local_cached_data["data"]

    client = bigquery.Client()

    query = f"""
        SELECT
            geolocation_details
        FROM `{bq_geolocation_cache_table}`
        WHERE key = @key
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("key", "STRING", key)
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
        result = query_job.result()

        row = next(result, None)

        if row is None:
            return None
    except Exception as error:
        log(f"Problems while communicating with BigQuery cache table: {error}", level="warning")
        return None

    data = json.loads(row.geolocation_details) if isinstance(row.geolocation_details, str) else dict(row.geolocation_details)
    set_geolocation_to_local_cache(key=key, value=data, is_new=False, local_geolocation_cache=local_geolocation_cache)
    return data

def set_geolocation_to_local_cache(key: str, value: dict, is_new: bool, local_geolocation_cache: dict):
    local_geolocation_cache.setdefault(key, {})
    local_geolocation_cache[key]["data"] = value
    local_geolocation_cache[key]["isNew"] = is_new
    return

def get_geolocation(
        search_text: str,
        google_maps_api_key: str,
        local_geolocation_cache: dict,
        bq_geolocation_cache_table: str):
    cache_key = " ".join(search_text.lower().split())
    cached = get_geolocation_from_cache(cache_key, local_geolocation_cache, bq_geolocation_cache_table)
    if cached:
        return cached

    client = googlemaps.Client(key=google_maps_api_key)
    try:
        geocode_result = client.geocode(
            address=search_text,
            region="br",
        )

        if not geocode_result:
            return None

        result = None
        city = ""
        for partial_result in geocode_result:
            state = next(
                (
                    c["short_name"]
                    for c in partial_result["address_components"]
                    if "administrative_area_level_1" in c["types"]
                ),
                None,
            )
            if state == "RJ":
                result = partial_result
                city = (
                    next(
                        (
                            c["long_name"]
                            for c in result["address_components"]
                            if "locality" in c["types"]
                        ),
                        None,
                    )
                    or next(
                        (
                            c["long_name"]
                            for c in result["address_components"]
                            if "administrative_area_level_2" in c["types"]
                        ),
                        "",
                    )
                )
                break

        if not result:
            return None

        location = result["geometry"]["location"]

        details = {
            "full_address": result["formatted_address"],
            "city": city,
            "latitude": location["lat"],
            "longitude": location["lng"],
            }

        set_geolocation_to_local_cache(key=cache_key, value=details, is_new=True, local_geolocation_cache=local_geolocation_cache)
        return details

    except Exception as e:
        log(f"Error geocoding locality {search_text}: {str(e)}")
        return None


def save_data_in_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    json_data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"],
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]
) -> None:
    """Saves a list of dictionaries to a BigQuery table partitioned monthly."""
    client = bigquery.Client()
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    partition_field = "c_processed_at" if source=="press" else "datetime"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        ignore_unknown_values=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=partition_field,
        ),
        clustering_fields=["id"],
    )

    timestamp_now = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    json_data = [{**row, "timestamp_insercao": timestamp_now} for row in json_data]

    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)
