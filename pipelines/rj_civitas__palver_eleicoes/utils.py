# -*- coding: utf-8 -*-
"""
Helpers para a pipeline Palver.

Inclui fetch assíncrono de ocorrências e escrita em BigQuery.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

import aiohttp
import pandas as pd
import pytz
import requests
import urllib3
from redis_pal import RedisPal
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log, log_mod

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

    response = requests.post(host + endpoint, json=payload, headers=headers, verify=False, timeout=10)
    response.raise_for_status()
    return response


async def get_data(
    host: str,
    token: str,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter", "telegram"],
    start_date: str,
    end_date: str,
    query: str,
    docs_per_page: int,
    max_concurrent: int = 5,
    delay_between_requests: float = 5,
) -> List[Dict]:
    """Fetches occurrences from the Palver API asynchronously with rate limiting."""
    params = {
            "sortOrder": "desc",
            "sortField": "datetime",
            "query": query,
            "perPage": docs_per_page,
            "startDate": f"{start_date}",
            "endDate": f"{end_date}"
        }
    if source not in ("telegram", "twitter"):
        params["country"] = "BR"
        params["region"] = "RJ"

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