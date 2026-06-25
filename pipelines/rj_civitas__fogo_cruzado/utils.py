# -*- coding: utf-8 -*-
"""
Helpers para a pipeline Fogo Cruzado.

Inclui autenticação contra a API, cache de token via Redis, fetch
assíncrono de ocorrências, e escrita em BigQuery.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

import aiohttp
import pandas as pd
import pytz
import requests
import urllib3
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log, log_mod
from redis_pal import RedisPal

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
    table_id: str,
    name: str | None = None,
    mode: Literal["dev", "prod"] = "prod",
) -> str:
    """Constructs a Redis key from dataset, table and optional name."""
    key = f"{dataset_id}.{table_id}"
    if name:
        key = f"{key}.{name}"
    if mode == "dev":
        key = f"{mode}.{key}"
    return key


def get_on_redis(
    dataset_id: str,
    table_id: str,
    name: str | None = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str | None = None,
) -> Any:
    """Retrieves a value from Redis based on dataset/table/name."""
    redis_client = get_redis_client(password=redis_password)
    key = build_redis_key(dataset_id, table_id, name, mode)
    return redis_client.get(key)


def save_on_redis(
    data: Any,
    dataset_id: str,
    table_id: str,
    name: str | None = None,
    mode: Literal["dev", "prod"] = "prod",
    redis_password: str | None = None,
) -> None:
    """Saves a value to Redis based on dataset/table/name."""
    redis_client = get_redis_client(password=redis_password)
    key = build_redis_key(dataset_id, table_id, name, mode)
    redis_client.set(key, data)


def safe_float_conversion(str_value: Any) -> float | None:
    """Converts a value to float, tolerating duplicated negative signs."""
    if isinstance(str_value, float):
        return str_value

    negative_sign_count = str(str_value).count("-")
    if negative_sign_count > 1:
        str_value = str(str_value).replace("-", "", negative_sign_count - 1)

    try:
        return float(str_value)
    except (TypeError, ValueError):
        return None


def is_token_valid(token_data: Optional[Dict[str, Any]]) -> bool:
    """Checks if the API token cached in Redis is still valid."""
    if not token_data:
        return False

    access_token = token_data.get("accessToken")
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
    host = "https://api-service.fogocruzado.org.br/api/v2"
    endpoint = "/auth/login"
    payload = {"email": email, "password": password}
    headers = {"Content-Type": "application/json"}

    response = requests.post(host + endpoint, json=payload, headers=headers, verify=False)
    response.raise_for_status()
    return response


def update_token_on_redis(data: requests.Response, redis_password: str | None = None) -> None:
    """Updates the cached token in Redis with its expiration date."""
    request_date_str: str = data.headers.get("Date")
    request_date_obj: datetime = pd.to_datetime(request_date_str)

    expires_at: datetime = request_date_obj + timedelta(
        seconds=data.json().get("data", {}).get("expiresIn", 0)
    )
    expires_at_str: str = expires_at.strftime("%Y-%m-%d %H:%M:%S")

    payload: dict = data.json().get("data", {})
    payload.update({"expiresAt": expires_at_str})

    save_on_redis(
        dataset_id="fogo_cruzado",
        table_id="ocorrencias",
        name="api_token",
        data=payload,
        redis_password=redis_password,
    )


def get_valid_token(email: str, password: str, redis_password: str | None = None) -> str:
    """Returns a valid auth token, using cache when possible."""
    try:
        token_data = get_on_redis(
            dataset_id="fogo_cruzado",
            table_id="ocorrencias",
            name="api_token",
            redis_password=redis_password,
        )

        if is_token_valid(token_data):
            log("Using cached token", level="info")
            return token_data["accessToken"]

        log("Token expired or invalid.", level="info")
    except Exception as e:
        log(f"Error accessing Redis: {e}", level="warning")
    
    try:
        log("Requesting new token...", level="info")
        response = auth(email, password)
        log("Token obtained successfully", level="info")
    except Exception as e:
        log(f"Error obtaining valid token: {e}", level="error")
        raise
    
    try:
        update_token_on_redis(response, redis_password=redis_password)
        log("Token updated in Redis", level="info")
    except Exception as e:
        log(f"Error accessing Redis: {e}", level="warning")

    return response.json().get("data", {}).get("accessToken")


async def get_occurrences(
    token: str,
    initial_date: Optional[str] = None,
    take: int = 200,
    id_state: str = "b112ffbe-17b3-4ad0-8f2a-2038745d1d14",
    id_city: str = "d1bf56cc-6d85-4e6a-a5f5-0ab3f4074be3",
    max_concurrent: int = 5,
    delay_between_requests: float = 5,
) -> List[Dict]:
    """Fetches occurrences from the Fogo Cruzado API asynchronously with rate limiting."""
    params_dict = {
        "initialdate": initial_date,
        "idState": id_state,
        "idCities": id_city,
        "take": take,
    }
    params = {k: v for k, v in params_dict.items() if v is not None}

    headers = {"Authorization": f"Bearer {token}"}
    base_url = "https://api-service.fogocruzado.org.br/api/v2/occurrences?page={page}"

    async with aiohttp.ClientSession() as session:
        initial_url = base_url.format(page=1)
        log("Getting total pages from API...", level="info")

        async with session.get(initial_url, headers=headers, params=params, ssl=False) as response:
            if response.status == 429:
                log("Rate limited on first request. Waiting 5 seconds...", level="info")
                await asyncio.sleep(5)
                async with session.get(
                    initial_url, headers=headers, params=params, ssl=False
                ) as response:
                    response.raise_for_status()
                    initial_data = await response.json()
            else:
                response.raise_for_status()
                initial_data = await response.json()

            total_pages = initial_data["pageMeta"]["pageCount"]
            occurrences = initial_data["data"]

        log(f"Total pages to fetch: {total_pages}", level="info")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_page_with_retry(session, page, retries=3):
            async with semaphore:
                for attempt in range(retries):
                    try:
                        url = base_url.format(page=page)
                        async with session.get(
                            url, headers=headers, params=params, ssl=False
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
                occurrences.extend(page_data)
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
        return occurrences


def save_data_in_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
    json_data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND",
) -> None:
    """Saves a list of dictionaries to a BigQuery table partitioned monthly."""
    client = bigquery.Client()
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="timestamp_insercao",
        ),
        clustering_fields=["timestamp_insercao"],
    )

    timestamp_now = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    json_data = [{**row, "timestamp_insercao": timestamp_now} for row in json_data]

    try:
        job = client.load_table_from_json(json_data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)
