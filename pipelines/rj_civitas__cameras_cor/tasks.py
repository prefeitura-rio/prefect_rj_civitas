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
        response = httpx.post(url=tixxi_url, json=body, verify=False)
        response.raise_for_status()
        data = response.json()["cameras"]
        cameras = [{
                "code": camera["code"],
                "name": camera["name"],
                "latitude": camera["latitude"],
                "longitude": camera["longitude"],
                "status_code": camera["CameraStatus"]
            } for camera in data]
        log("Data obtained successfully.", level="info")
        return cameras

    except Exception as e:
        log(f"Error obtaining data: {e}", level="error")
        raise


@task
def enrich_data_task(
    data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    status_map = {
        "0": "OFFLINE",
        "1": "ONLINE",
        "3": "DESLIGADA",
        "5": "TERCEIROS"
    }
    enriched_data = [
        {
            **register,
            "status_description": status_map.get(register.get("status_code")),
            "is_third_party": register.get("status_code") == '5'
            } for register in data]
    return enriched_data