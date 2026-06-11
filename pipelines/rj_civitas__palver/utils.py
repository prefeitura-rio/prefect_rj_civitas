# -*- coding: utf-8 -*-
"""
Helpers para a pipeline Palver.

Inclui fetch assíncrono de ocorrências e escrita em BigQuery.
"""
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import aiohttp
import pandas as pd
import pytz
import urllib3
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log, log_mod

from pipelines.rj_civitas__palver.schemas import get_source_parameters

tz = pytz.timezone("America/Sao_Paulo")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



async def get_data(
    host: str,
    token: str,
    source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"],
    initial_date: str,        
    query: str,
    docs_per_page: int,
    max_concurrent: int = 5,
    delay_between_requests: float = 5,
) -> List[Dict]:
    """Fetches occurrences from the Palver API asynchronously with rate limiting."""
    params = get_source_parameters(source)
    params["query"] = query
    params["perPage"] = docs_per_page
    params["startDate"] = f"{initial_date}T03:00:00Z"
    params["endDate"] = f"{datetime.now(tz=tz).strftime('%Y-%m-%d')}T03:00:00Z"

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
        ignore_unknown_values=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
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
