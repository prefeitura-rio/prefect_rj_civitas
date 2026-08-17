# -*- coding: utf-8 -*-
import httpx
from typing import List, Dict, Any
from prefect import task
from iplanrio.pipelines_utils.prefect import log


@task(retries=3)
def fetch_cameras_task(
    tixxi_url: str,
    tixxi_key: str,
    tixxi_email: str,
    tixxi_password: str
) -> List[Dict[str, Any]]:
    body = {
        "api_key": tixxi_key,
        "email": tixxi_email,
        "password": tixxi_password
    }
    log(f"Fetching data from TIXXI", level="info")
    try:
        response = httpx.post(url=tixxi_url, json=body)
        response.raise_for_status()
        data = response.json()["cameras"]
        cameras = [{
                "CameraCode": camera["code"],
                "CameraName": camera["name"],
                "CameraZone": None,
                "Latitude": camera["latitude"],
                "Longitude": camera["longitude"],
                "Streamming": camera["stream_url"]
            } for camera in data]
        log("Data obtained successfully.", level="info")
        return cameras

    except Exception as e:
        log(f"Error obtaining data: {e}", level="error")
        raise
