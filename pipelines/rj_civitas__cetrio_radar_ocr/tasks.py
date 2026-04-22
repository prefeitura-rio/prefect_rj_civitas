# -*- coding: utf-8 -*-
"""
Tasks for rj_civitas__cetrio_radar_ocr pipeline.
"""

import csv
from pathlib import Path
from uuid import uuid4

import pandas as pd
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task


@task
def get_cetrio_radar_api_credentials(secret_path: str = "/cetrio_radar") -> dict:
    """
    Resolves radar API credentials from environment variables.

    Expected format:
    - <SECRET_PATH>__URL
    - <SECRET_PATH>__TOKEN
    """
    normalized_secret_path = secret_path.upper().replace("-", "_").replace("/", "")
    return {
        "URL": getenv_or_action(f"{normalized_secret_path}__URL"),
        "TOKEN": getenv_or_action(f"{normalized_secret_path}__TOKEN"),
    }


@task
def extract_radar_data(secrets: dict, filename: str = "radar_data.csv") -> Path:
    """
    Extracts radar data from CETRIO API and saves it to a local CSV file.
    """
    path = Path(f"/tmp/{uuid4()}")
    path.mkdir(parents=True, exist_ok=True)
    filepath = path / filename

    url = secrets.get("URL")
    token = secrets.get("TOKEN")

    if not url:
        raise ValueError("Radar API URL not found in secrets")
    if not token:
        raise ValueError("Radar API token not found in secrets")

    log(f"Starting download from URL: {url}")
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()

        dataframe = pd.DataFrame(data)
        dataframe.to_csv(
            filepath,
            index=False,
            sep=",",
            quoting=csv.QUOTE_NONNUMERIC,
            quotechar='"',
        )
        return filepath
    except Exception as exc:
        log(f"Error extracting radar data from CETRIO API: {exc}", level="error")
        raise
