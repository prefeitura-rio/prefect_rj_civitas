# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

from prefect import flow
from prefect_rj_civitas import skip_if_already_running
from typing import Literal, List

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task

from pipelines.rj_civitas__dados_geograficos.tasks import (
    get_bairros_rj_data,
    load_bairros_rj_to_table_task,
    get_isp_geojson_data_from_mp
)



@flow(log_prints=True)
def rj_civitas__dados_geograficos(
    project_id: str = "rj-civitas",
    dataset_id: str = "dados_geograficos",
    tables: List[Literal["bairros_rj", "areas_isp"]] = ["bairros_rj", "areas_isp"],
    mode: Literal["dev", "prod", "staging"] = "staging"
):
    rename_current_flow_run_task(new_name=dataset_id)

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"


    if "bairros_rj" in tables:
        bairros_rj_data = get_bairros_rj_data(year=2022)

        load_bairros_rj_to_table_task(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id="bairros_rj",
            data=bairros_rj_data,
            write_disposition="WRITE_TRUNCATE"
        )


    if "areas_isp" in tables:
        isp_metadata = {
            "CISP": {
                "url": "https://geo.mprj.mp.br/arcgis/rest/services/Seguranca_Publica/Circunscri%C3%A7%C3%B5es_Integradas_de_Seguran%C3%A7a_P%C3%BAblica__CISP_/FeatureServer/0/query",
                "name_field": "dp_nome"
            },
            "AISP": {
                "url": "https://geo.mprj.mp.br/arcgis/rest/services/Seguranca_Publica/%C3%81reas_Integradas_de_Seguran%C3%A7a_P%C3%BAblica__AISP_/FeatureServer/0/query",
                "name_field": "unidade"
            }
        }

        for key, value in isp_metadata.items():
            data = get_isp_geojson_data_from_mp(key, value["name_field"], value["url"])
            if data:
                load_bairros_rj_to_table_task(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=f"areas_{key.lower()}",
                    data=data,
                    write_disposition="WRITE_TRUNCATE"
                )