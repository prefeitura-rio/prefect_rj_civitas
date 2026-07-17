# -*- coding: utf-8 -*-
"""
Tasks da pipeline cameras_civitas.
"""
import requests
from datetime import datetime
from typing import Any, Dict, List, Literal
import pytz
import unicodedata

from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

tz = pytz.timezone("America/Sao_Paulo")

@task
def get_smart_token_task(
    smart_url: str,
    smart_email: str,
    smart_password: str
    ) -> str:
    """Returns a valid auth token from SMART"""
    endpoint = "/auth/login"

    headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Origin": "http://smartrackcloud.com.br",
    "Referer": "http://smartrackcloud.com.br/login",
    "Content-Type": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "application/json, text/plain, */*"
    }

    body = {
        "email": f"{smart_email}",
        "password": f"{smart_password}"
    }

    try:
        response = requests.post(url=smart_url + endpoint, headers=headers, json=body)

        response.raise_for_status()
        log("Token obtained successfully", level="info")
        return response.json().get("accessToken")

    except Exception as e:
        log(f"Error obtaining valid token: {e}", level="error")
        raise


@task(retries=3, retry_delay_seconds=60)
def fetch_cameras_task(
    smart_url: str,
    smart_token: str
) -> List[Dict[str, Any]]:
    """Task that fetches messages from the SMART API."""
    def normalize_column_names(texto):
        texto = texto.lower().replace(' ', '_')
        texto_normalizado = unicodedata.normalize('NFKD', texto)
        return "".join([c for c in texto_normalizado if not unicodedata.combining(c)])

    endpoint = "/queries/run"

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {smart_token}",
        "Origin": "http://smartrackcloud.com.br",
        "Referer": "http://smartrackcloud.com.br/consultas",
    }

    body = {
        "projectId":"cmorqe7tq001bkbjwlrhf7c37",
        "name":"",
        "search":"",
        "columns":[
            "record_id",
            "updated_at",
            "Poligono",
            "Modelo",
            "Id_do_Ponto",
            "IP_da_camera",
            "Caixa_Tecnica",
            "IP_da_caixa_tecnica",
            "Numero_de_serie_da_camera",
            "Mascara",
            "Latitude",
            "Longitude",
            "Bairro",
            "Endereço",
            "Camera_Furtada",
            "Data_da_Identificação_do_Furto",
            "Numero_do_BO",
            "Status Zabbix",
            "Status_da_camera",
            "Data_NOC",
            "Numero_do_Sim_card",
            "ICCID",
            "Numero_de_serie_Roteador",
            "IP_do_roteador",
            "Tipo_de_conectividade",
            "Reposicionamento",
            "Data_do_repocisionamento",
            "Data_de_implantacao_da_camera",
            "Status_RDO",
            "Data_do_RDO",
            "Numero_do_RDO",
            "VMS_ou_SENTRY",
            "Data_de_inclusão_VMS_ou_Sentry",
            "Energização",
            "Data_de_energização",
            "Status_da_fibra",
            "Data_Fibra",
            "Validação_da_visada",
            "Data_da_validação_da_visada",
            "Data_da_aprovação_RDO",
            "Comissionada"
            ],
        "filters":[],
        "filterCombineMode":"auto",
        "isShared": False
        }

    log(f"Fetching data from SMART", level="info")
    try:
        response = requests.post(url=smart_url + endpoint, headers=headers, json=body)

        response.raise_for_status()
        log("Data obtained successfully. Normalizing column names...", level="info")

        data = response.json().get("rows")
        normalized_data = [
            {normalize_column_names(key): value for key, value in row.items()}
            for row in data
        ]
        return normalized_data

    except Exception as e:
        log(f"Error obtaining valid data: {e}", level="error")
        raise


@task(retries=3, retry_delay_seconds=30)
def load_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_TRUNCATE"
) -> None:
    """
    Loads occurrences to a BigQuery table using the canonical schema.
    """
    log(f"Writing occurrences to {project_id}.{dataset_id}.{table_id}")

    schema = [
        bigquery.SchemaField(name="record_id", field_type="STRING", mode="REQUIRED"),
        bigquery.SchemaField(name="updated_at", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="poligono", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="modelo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="id_do_ponto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="ip_da_camera", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="caixa_tecnica", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="ip_da_caixa_tecnica", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_de_serie_da_camera", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="mascara", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="bairro", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="endereco", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="camera_furtada", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_da_identificacao_do_furto", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_do_bo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="status_zabbix", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="status_da_camera", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_noc", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_do_sim_card", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="iccid", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_de_serie_roteador", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="ip_do_roteador", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="tipo_de_conectividade", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="reposicionamento", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_do_repocisionamento", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_de_implantacao_da_camera", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="status_rdo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_do_rdo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="numero_do_rdo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="vms_ou_sentry", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_de_inclusao_vms_ou_sentry", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="energizacao", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_de_energizacao", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="status_da_fibra", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_fibra", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="validacao_da_visada", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_da_validacao_da_visada", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="data_da_aprovacao_rdo", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="comissionada", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField(name="timestamp_insercao", field_type="TIMESTAMP", mode="REQUIRED")
        ]

    client = bigquery.Client()
    table_full_name = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        ignore_unknown_values=True,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=write_disposition
    )

    timestamp_now = datetime.now(tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    data = [{**row, "timestamp_insercao": timestamp_now} for row in data]

    try:
        job = client.load_table_from_json(data, table_full_name, job_config=job_config)
        job.result()
    except Exception as e:
        raise Exception(e)

    log(f"{len(data)} registers written to {project_id}.{dataset_id}.{table_id}")