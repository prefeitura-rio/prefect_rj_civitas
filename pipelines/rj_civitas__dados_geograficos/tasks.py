# -*- coding: utf-8 -*-
"""
Tasks for rj_civitas__dados_geograficos pipeline.
"""

import geobr
from typing import Any, Literal, List, Dict
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_civitas__dados_geograficos.utils import corrigir_geometria
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

    print(f"{len(bairros_rj)} bairros mapeados com sucesso.")

    print("Formatando dados em memória para subir para BigQuery...")

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
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND"
) -> None:
    log(f"Carregando dados geográficos de bairrs em {project_id}.{dataset_id}.{table_id}")
    schema_bairros = [
        bigquery.SchemaField("id_municipio", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nome_municipio", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("id_bairro", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nome_bairro", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("geometria", "GEOGRAPHY", mode="REQUIRED"),
        bigquery.SchemaField("timestamp_insercao", "TIMESTAMP", mode="REQUIRED"),
    ]

    save_data_in_bq_table(
        data =data,
        schema=schema_bairros,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        insert_timestamp_field="timestamp_insercao",
        write_disposition=write_disposition,
        max_bad_records=100
    )
    log(f"Dados carregados com sucesso em {project_id}.{dataset_id}.{table_id}")