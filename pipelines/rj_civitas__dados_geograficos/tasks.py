# -*- coding: utf-8 -*-
"""
Tasks for rj_civitas__dados_geograficos pipeline.
"""

import geobr
import geopandas as gdp
import requests
from typing import Any, Literal, List, Dict
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_civitas__dados_geograficos.utils import corrigir_geometria, format_isp_data
from prefect_rj_civitas import (
    save_data_in_bq_table
)


@task
def get_bairros_rj_data(
    year: int = 2022
):
    log("Baixando dados geográficos de todo o Estado do RJ...")
    rj_setores = geobr.read_census_tract(code_tract="RJ", year=year)

    log("Dados baixados com sucesso. Limpando e padronizando os códigos do IBGE...")
    rj_setores['code_muni'] = rj_setores['code_muni'].astype(float).astype(int).astype(str)

    rj_setores = rj_setores.dropna(subset=['name_neighborhood'])
    rj_setores = rj_setores[rj_setores['name_neighborhood'].astype(str).str.strip() != ""]
    rj_setores['code_neighborhood'] = rj_setores['code_neighborhood'].fillna("0")

    bairros_rj = rj_setores.dissolve(
        by=['code_muni', 'name_muni', 'code_neighborhood', 'name_neighborhood']
    ).reset_index()

    bairros_rj = bairros_rj[[
        'code_muni',
        'name_muni',
        'code_neighborhood',
        'name_neighborhood',
        'geometry'
    ]]

    bairros_rj.columns = ['id_municipio', 'nome_municipio', 'id_bairro', 'nome_bairro', 'geometria']

    log(f"{len(bairros_rj)} bairros mapeados com sucesso.")

    log("Formatando dados em memória para subir para BigQuery...")

    dados_para_bq = []

    for _, row in bairros_rj.iterrows():
        try:
            id_bairro_int = int(float(row["id_bairro"]))
        except (ValueError, TypeError):
            id_bairro_int = 0

        geometria_pronta = corrigir_geometria(row["geometria"])

        if geometria_pronta is None or geometria_pronta.is_empty:
            continue

        linha = {
            "id_municipio": str(row["id_municipio"]),
            "nome_municipio": str(row["nome_municipio"]),
            "id_bairro": id_bairro_int,
            "nome_bairro": str(row["nome_bairro"]),
            "geometria": geometria_pronta.wkt
        }
        dados_para_bq.append(linha)

    return dados_para_bq

@task
def load_bairros_rj_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_TRUNCATE"
) -> None:
    log(f"Carregando dados geográficos em {project_id}.{dataset_id}.{table_id}")

    bq_descriptions = {
        "bairros_rj": "Dados geográficos de todos os bairros do estado do Rio de Janeiro. Fonte: IBGE",
        "areas_aisp": "Dados geográficos das AISPs (Áreas Integradas de Segurança Pública) e os seus respectivos batalhões de polícia militar. Fonte: ISP (por meio do site do Ministério Público)",
        "areas_cisp": "Dados geográficos das CISPs (Circunscrições Integradas de Segurança Pública) e as suas respectivas delegacias de polícia. Fonte: ISP (por meio do site do Ministério Público)",
    }

    bq_schemas = {
        "bairros_rj": [
            bigquery.SchemaField("id_municipio", "STRING", mode="NULLABLE", description="ID do município"),
            bigquery.SchemaField("nome_municipio", "STRING", mode="REQUIRED", description="Nome do município"),
            bigquery.SchemaField("id_bairro", "STRING", mode="NULLABLE", description="ID do bairro"),
            bigquery.SchemaField("nome_bairro", "STRING", mode="REQUIRED", description="Nome do município"),
            bigquery.SchemaField("geometria", "GEOGRAPHY", mode="REQUIRED", description="Dados geográficos de delimitação do bairro"),
            bigquery.SchemaField("timestamp_insercao", "TIMESTAMP", mode="REQUIRED", description="Timestamp UTC de inserção no banco de dados"),
        ],

        "areas_aisp": [
            bigquery.SchemaField("aisp", "STRING", mode="REQUIRED", description="Número da AISP"),
            bigquery.SchemaField("unidade", "STRING", mode="REQUIRED", description="Batalhão de polícia militar relativo à AISP"),
            bigquery.SchemaField("geometria", "GEOGRAPHY", mode="REQUIRED", description="Dados geográficos de delimitação da AISP"),
            bigquery.SchemaField("timestamp_insercao", "TIMESTAMP", mode="REQUIRED", description="Timestamp UTC de inserção no banco de dados"),
        ],

        "areas_cisp": [
            bigquery.SchemaField("cisp", "STRING", mode="REQUIRED", description="Número da CISP"),
            bigquery.SchemaField("dp_nome", "STRING", mode="REQUIRED", description="Delegacia de polícia civil relativo à CISP"),
            bigquery.SchemaField("geometria", "GEOGRAPHY", mode="REQUIRED", description="Dados geográficos de delimitação da CISP"),
            bigquery.SchemaField("timestamp_insercao", "TIMESTAMP", mode="REQUIRED", description="Timestamp UTC de inserção no banco de dados"),
        ]
    }

    save_data_in_bq_table(
        data =data,
        schema=bq_schemas[table_id],
        table_description=bq_descriptions[table_id],
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        insert_timestamp_field="timestamp_insercao",
        write_disposition=write_disposition,
        max_bad_records=100 if table_id=="bairros_rj" else 0
    )
    log(f"Dados carregados com sucesso em {project_id}.{dataset_id}.{table_id}")


@task
def get_isp_geojson_data_from_mp(type: str, name_field: str, base_url: str):
    log(f"Solicitando os dados georreferenciados de {type} ao servidor do MPRJ...")
    params = {
        "f": "geojson",
        "where": "1=1",
        "outFields": "*",
        "orderByFields": type,
        "returnGeometry": "true",
        "outSR": 4326
    }
    attempts = 5
    for attempt in range(attempts):
        try:
            response = requests.get(base_url, params=params, timeout=15)
            response.raise_for_status()
            dados_geojson = response.json()
            dados_formatados = format_isp_data(type, name_field, dados_geojson)
            return dados_formatados

        except requests.exceptions.Timeout as e:
            if attempt + 1 == attempts:
                log(f"Timeout error. Todas as {attempts} tentativas falharam")
                raise e
            else:
                log(f"Timeout error. Tentativa {attempt + 1}/{attempts}", level="warning")
                continue

    log(f"Problemas ao fazer o download dos dados de {type}")
    return None