# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

from prefect import flow
from prefect_rj_civitas import skip_if_already_running
from typing import Literal

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task

from pipelines.rj_civitas__dados_geograficos.tasks import get_bairros_rj_data, load_bairros_rj_to_table_task



@flow(log_prints=True)
def rj_civitas__dados_geograficos(
    project_id: str = "rj-civitas",
    dataset_id: str = "dados_geograficos",
    table_id: str = "bairros_rj",
    mode: Literal["dev", "prod", "staging"] = "staging"
):
    rename_current_flow_run_task(new_name=table_id)

    if skip := skip_if_already_running():
        return skip

    inject_bd_credentials_task(environment="prod")

    if mode in ("dev", "staging"):
        project_id = f"{project_id}-dev"

    bairros_rj_data = get_bairros_rj_data(year=2022)

    load_bairros_rj_to_table_task(
        project_id=project_id,
        dataset_id=f"{dataset_id}",
        table_id=table_id,
        data=bairros_rj_data,
        write_disposition="WRITE_TRUNCATE"
    )