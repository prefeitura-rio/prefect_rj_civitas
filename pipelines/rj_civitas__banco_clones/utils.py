# -*- coding: utf-8 -*-
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from iplanrio.pipelines_utils.logging import log


def resolve_start_date(
        bq_client: bigquery.Client,
        auditoria_table_id:str,
        start_date: str
):
    """
    Pegar último dia da tabela de auditoria ṕara implementar lógica incremental
    """
    log("Resolving start date for nincremental logic...")
    query_max_date = f"""
        SELECT MAX(dia) FROM `{auditoria_table_id}`
    """
    try:
        result_query_incremental = bq_client.query_and_wait(query=query_max_date)
        first_row = next(iter(result_query_incremental), None)
        max_date = first_row[0] if first_row else None
    except NotFound:
        log(f"Table {auditoria_table_id} does not exist yet. Start date will be: {start_date}")
        max_date = start_date

    if not max_date:
        log(f"No registers in {auditoria_table_id}. Cannot use incremental logic. Start date will be: {start_date}")
        max_date = start_date
    else:
        max_date = max_date.strftime("%Y-%m-%d") if hasattr(max_date, "strftime") else str(max_date)
        log(f"Incremental logic succeded -> Start date: {max_date}")

    return max_date


def fetch_new_clonned_plates_and_days(
        bq_client: bigquery.Client,
        banco_clones_table_id: str,
        start_date: str
    ):
    """"
    Selecionar placas e dias a serem auditados.
    Aplicação de lógica incremental, excluindo tabelas do dia atual,
    para criar trilhas apenas sobre dias completos
    """
    log("Fetching new clonned plates...")

    query_new_clonned_plates = f"""
        SELECT DISTINCT placa, ultimo_dia_suspeito FROM `{banco_clones_table_id}`
        WHERE timestamp_insercao >= TIMESTAMP(@start_date, 'America/Sao_Paulo')
          AND ultimo_dia_suspeito >= CAST(@start_date AS DATE)
          AND ultimo_dia_suspeito < CURRENT_DATE()
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date)
        ]
    )

    returned_plates_days = {}
    rows = bq_client.query_and_wait(query=query_new_clonned_plates, job_config=job_config)

    for row in rows:
        placa = row.placa
        dia = row.ultimo_dia_suspeito
        if returned_plates_days.get(placa, None) is None:
            returned_plates_days[placa] = set([dia])
        else:
            returned_plates_days[placa].add(dia)

    log(f"{len(returned_plates_days)} new clonned plates found")
    return returned_plates_days


def get_plates_readings(
        bq_client: bigquery.Client,
        plates_days: dict,
        readings_table_id: str,
        start_date: str
):
    log("Getting plates readings per suspect day...")
    string_plates_days_list = ", ".join(
        f"'{plate}{day.isoformat()}'"
        for plate, days in plates_days.items()
        for day in days
    )
    query_plates_readings = f"""
        SELECT
            placa,
            DATE(datahora, 'America/Sao_Paulo') AS dia,
            ARRAY_AGG(
                STRUCT(
                    datahora,
                    empresa,
                    camera_latitude,
                    camera_longitude,
                    sentido,
                    id_ponto_coleta,
                    camera_numero
                )
            ) AS leituras
        FROM `{readings_table_id}`
        WHERE datahora >= TIMESTAMP('{start_date}', 'America/Sao_Paulo')
          AND datahora < TIMESTAMP(CURRENT_DATE(), 'America/Sao_Paulo')
          AND CONCAT(placa, CAST(DATE(datahora, 'America/Sao_Paulo') AS STRING)) IN ({string_plates_days_list})
        GROUP BY placa, DATE(datahora, 'America/Sao_Paulo')
        """

    readings = bq_client.query_and_wait(query=query_plates_readings)

    filtered_readings = []
    for row in readings:
        if row.dia in plates_days.get(row.placa, set()):
            filtered_readings.append({
                "placa": row.placa,
                "dia": row.dia.isoformat(),
                "leituras": [
                    {
                        "datahora": reading["datahora"].isoformat(),
                        "empresa": reading["empresa"],
                        "latitude": reading["camera_latitude"],
                        "longitude": reading["camera_longitude"],
                        "sentido": reading["sentido"],
                        "id_ponto_coleta": reading["id_ponto_coleta"],
                        "camera_numero": reading["camera_numero"]
                    }
                    for reading in row.leituras]
                })
    log("Readings successfully retrieved")
    return filtered_readings