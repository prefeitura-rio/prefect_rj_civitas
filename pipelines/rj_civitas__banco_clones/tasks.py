# -*- coding: utf-8 -*-
"""
Tasks da pipeline banco_clones.
"""
import httpx
from datetime import datetime
from typing import Any, Dict, List, Literal
import pytz
import unicodedata

from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_civitas__banco_clones.utils import resolve_start_date, fetch_new_clonned_plates_and_days, get_plates_readings

tz = pytz.timezone("America/Sao_Paulo")

@task
def get_tracks_task(
    start_date: str,
    readings_full_table_id: str,
    banco_clones_full_table_id: str,
    auditoria_full_table_id: str = ""
) -> list[dict] | None:
    log("Conecting to BigQuery")
    try:
        bq_client = bigquery.Client()
    except Exception as e:
        log("Cannection error", level="error")
        raise(e)

    log("Fetching new cloned plate registers...")
    resolved_start_date = resolve_start_date(
        bq_client=bq_client,
        auditoria_table_id=auditoria_full_table_id,
        start_date=start_date
        )

    plates_days = fetch_new_clonned_plates_and_days(
        bq_client=bq_client,
        banco_clones_table_id=banco_clones_full_table_id,
        start_date=resolved_start_date
    )

    if not plates_days:
        log("No new clonned plates found")
        return None

    readings = get_plates_readings(
        bq_client=bq_client,
        plates_days=plates_days,
        readings_table_id=readings_full_table_id,
        start_date=resolved_start_date
        )

    tracks = separate_tracks(readings)

    return tracks