# -*- coding: utf-8 -*-
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from iplanrio.pipelines_utils.logging import log

from utils import (
    haversine_km,
    custo_trecho,
    is_spike,
    get_detection_track
)
from constants import CUSTO_INVIAVEL, DIST_MAX_ALTERNATIVA_KM


def resolve_start_date(
        bq_client: bigquery.Client,
        trilhas_table_id:str,
        start_date: str
):
    """
    Pegar último dia da tabela de trilhas ṕara implementar lógica incremental
    """
    log("Resolving start date for incremental logic...")
    query_max_date = f"""
        SELECT DATE_ADD(MAX(dia), INTERVAL 1 DAY) FROM `{trilhas_table_id}`
    """
    try:
        result_query_incremental = bq_client.query_and_wait(query=query_max_date)
        first_row = next(iter(result_query_incremental), None)
        max_date = first_row[0] if first_row else None
    except NotFound:
        log(f"Table {trilhas_table_id} does not exist yet. Start date will be: {start_date}")
        max_date = start_date

    if not max_date:
        log(f"No registers in {trilhas_table_id}. Cannot use incremental logic. Start date will be: {start_date}")
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
        SELECT DISTINCT placa, dia FROM `{banco_clones_table_id}`
        WHERE dia >= CAST('{start_date}' AS DATE)
          AND dia < CURRENT_DATE()
    """

    returned_plates_days = {}
    rows = bq_client.query_and_wait(query=query_new_clonned_plates)

    for row in rows:
        placa = row.placa
        dia = row.dia
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
        pares_suspeitos_table_id: str,
        start_date: str
):
    log("Getting plates readings per suspect day...")
    string_plates_days_list = ", ".join(
        f"'{plate}{day.isoformat()}'"
        for plate, days in plates_days.items()
        for day in days
    )
    query_plates_readings = f"""
        WITH leituras_validas AS (
        SELECT
            placa,
            DATE(datahora, 'America/Sao_Paulo') AS data_dia,
            CONCAT(id_ponto_coleta, CAST(datahora AS STRING)) AS id,
            datahora,
            empresa,
            sentido,
            bairro,
            localidade,
            velocidade,
            camera_latitude,
            camera_longitude,
            id_ponto_coleta,
            camera_numero
        FROM `{readings_table_id}`
            WHERE datahora >= TIMESTAMP('{start_date}', 'America/Sao_Paulo')
            AND datahora < TIMESTAMP(CURRENT_DATE(), 'America/Sao_Paulo')
            AND CONCAT(placa, CAST(DATE(datahora, 'America/Sao_Paulo') AS STRING)) IN ({string_plates_days_list})
            QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY placa,
                                        DATE(datahora, 'America/Sao_Paulo'),
                                        CONCAT(id_ponto_coleta, CAST(datahora AS STRING))
                            ORDER BY camera_numero ASC
                        ) = 1
        ),

        leituras_validas_struct AS (
        SELECT
            placa,
            data_dia,
            ARRAY_AGG(
                STRUCT(
                    id,
                    datahora,
                    empresa,
                    sentido,
                    bairro,
                    localidade,
                    velocidade,
                    camera_latitude,
                    camera_longitude,
                    id_ponto_coleta,
                    camera_numero
                )
                ORDER BY datahora
            ) AS leituras
        FROM leituras_validas
        GROUP BY placa, data_dia
        ),

        pares_suspeitos AS (
        SELECT
            placa,
            data_dia,
            datahora_anterior,
            datahora_posterior,
            ponto_anterior,
            ponto_posterior,
            geolocation_anterior,
            geolocation_posterior,
            IF(st_distance(geolocation_anterior, LAG(geolocation_posterior) OVER (PARTITION BY placa, data_dia ORDER BY datahora_anterior)) +
            st_distance(geolocation_posterior, LAG(geolocation_anterior) OVER (PARTITION BY placa, data_dia ORDER BY datahora_anterior)) <
            st_distance(geolocation_anterior, LAG(geolocation_anterior) OVER (PARTITION BY placa, data_dia ORDER BY datahora_anterior)) +
            st_distance(geolocation_posterior, LAG(geolocation_posterior) OVER (PARTITION BY placa, data_dia ORDER BY datahora_anterior)),
            1, 0
            ) AS flag_trilha   --troca de trilha com relação ao anterior? Se sim, flag 1
        FROM `{pares_suspeitos_table_id}`
            WHERE datahora_posterior >= TIMESTAMP('{start_date}', 'America/Sao_Paulo')
            AND CONCAT(placa, CAST(DATE(datahora_posterior, 'America/Sao_Paulo') AS STRING)) IN ({string_plates_days_list})
        ),

        pares_com_trilha AS (
        SELECT
            *,
            MOD(
            SUM(flag_trilha) OVER (
                PARTITION BY placa, data_dia
                ORDER BY datahora_anterior
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            2
            ) AS trilha  --Ímpar=lados mantidos, par=inverte lados
        FROM pares_suspeitos
        ),

        pares_finais AS (
        SELECT
            *,
            CONCAT(ponto_anterior, CAST(datahora_anterior AS STRING)) AS id_anterior,
            CONCAT(ponto_posterior, CAST(datahora_posterior AS STRING)) AS id_posterior,
            IF(trilha=0, 'A', 'B') AS trilha_anterior,
            IF(trilha=0, 'B', 'A') AS trilha_posterior
        FROM pares_com_trilha
        )

        SELECT
            lv.placa,
            lv.data_dia,
            lv.leituras,
            ARRAY_AGG(
                STRUCT(
                    pf.id_anterior,
                    pf.id_posterior,
                    pf.datahora_anterior,
                    pf.datahora_posterior,
                    pf.trilha_anterior,
                    pf.trilha_posterior
                )
                ORDER BY pf.datahora_anterior
            ) AS pares_suspeitos_trilhas
        FROM leituras_validas_struct lv
        JOIN pares_finais pf
        ON lv.placa = pf.placa AND lv.data_dia = pf.data_dia
        GROUP BY lv.placa, lv.data_dia, lv.leituras
        """

    readings = bq_client.query_and_wait(query=query_plates_readings)

    filtered_readings = []
    for row in readings:
        if row.data_dia in plates_days.get(row.placa, set()):
            filtered_readings.append({
                "placa": row.placa,
                "dia": row.data_dia.isoformat(),
                "leituras": [
                    {
                        "id": reading["id"],
                        "datahora": reading["datahora"].replace(tzinfo=None).isoformat(),
                        "empresa": reading["empresa"],
                        "latitude": reading["camera_latitude"],
                        "longitude": reading["camera_longitude"],
                        "sentido": reading["sentido"],
                        "bairro": reading["bairro"],
                        "localidade": reading["localidade"],
                        "velocidade": reading["velocidade"],
                        "id_ponto_coleta": reading["id_ponto_coleta"],
                        "camera_numero": reading["camera_numero"]
                    }
                    for reading in row.leituras],
                "pares_suspeitos_trilhas": [
                    {
                        "id_anterior": par["id_anterior"],
                        "id_posterior": par["id_posterior"],
                        "trilha_anterior": par["trilha_anterior"],
                        "trilha_posterior": par['trilha_posterior']
                    }
                    for par in row.pares_suspeitos_trilhas
                ]
                })
    log("Readings successfully retrieved")
    return filtered_readings


def separate_suspect_pairs_into_tracks(
    leituras: list[dict],
    pares_suspeitos: list[dict]
):
    trilha_a = []
    trilha_b = []


    suspeitos_trilhas = {}
    for par_suspeito in pares_suspeitos:
        #salvar os ids de detecções suspeitas com cada trilha para separação posterior
        suspeitos_trilhas[par_suspeito["id_anterior"]] = par_suspeito["trilha_anterior"]
        suspeitos_trilhas[par_suspeito["id_posterior"]] = par_suspeito["trilha_posterior"]


    for leitura in leituras:
        trilha_leitura = suspeitos_trilhas.get(leitura["id"])
        if trilha_leitura == 'A':
            leitura["suspeito"] = True
            trilha_a.append(leitura)
        elif trilha_leitura == 'B':
            leitura["suspeito"] = True
            trilha_b.append(leitura)

    return trilha_a, trilha_b


def create_anchors(
    leituras: list[dict],
    pares_suspeitos: list[dict]
):
    ancoras = []
    ancoras_final = []
    map_id_para_indice = {}
    for i, leitura in enumerate(leituras):
        map_id_para_indice[leitura["id"]] = i

    for par in pares_suspeitos:
        if par["trilha_anterior"] == "A":
            inicio_a = fim_a = map_id_para_indice[par["id_anterior"]]
            inicio_b = fim_b = map_id_para_indice[par["id_posterior"]]
        elif par["trilha_anterior"] == "B":
            inicio_a = fim_a = map_id_para_indice[par["id_posterior"]]
            inicio_b = fim_b = map_id_para_indice[par["id_anterior"]]

        ancoras.append({
            "inicio_a": inicio_a,
            "inicio_b": inicio_b,
            "fim_a": fim_a,
            "fim_b": fim_b
        })

    ancoras_length = len(ancoras)
    i=0
    while i+1 < ancoras_length:
        if ancoras[i]["fim_a"] == ancoras[i+1]["inicio_a"] \
            or ancoras[i]["fim_b"] == ancoras[i+1]["inicio_b"] \
            or max(ancoras[i]["fim_a"], ancoras[i]["fim_b"]) + 1 == \
                min(ancoras[i+1]["inicio_a"], ancoras[i+1]["inicio_b"]):
            ancoras[i+1]["inicio_b"] = ancoras[i]["inicio_b"]
            ancoras[i+1]["inicio_a"] = ancoras[i]["inicio_a"]
        else:
            ancoras_final.append(ancoras[i])

        i+=1

    ancoras_final.append(ancoras[i])

    return ancoras_final


def get_valid_segment(
    readings: list[dict],
    first_index: int,
    last_index: int
):
    segment = [readings[first_index]]
    outliers = []

    i = first_index + 1
    while i < last_index:
        if i + 1 < last_index and is_spike(segment[-1], readings[i], readings[i + 1]):
            outliers.append(readings[i])
            i += 1
            continue

        distancia = haversine_km(segment[-1], readings[i])
        if distancia <= DIST_MAX_ALTERNATIVA_KM and custo_trecho(segment[-1], readings[i]) < CUSTO_INVIAVEL:
            segment.append(readings[i])
        else:
            outliers.append(readings[i])
        i += 1

    return segment, outliers


def apply_intermediate_and_last_detections_tracks(leituras, ancoras, trilha_a, trilha_b, ambiguos):
        i = 0
        ancoras_length = len(ancoras)
        while i < ancoras_length:
            detection_index = max(ancoras[i]["fim_a"], ancoras[i]["fim_b"]) + 1
            if i + 1 < ancoras_length:
                last_index = min(ancoras[i+1]["inicio_a"], ancoras[i+1]["inicio_b"])
                next_anchor = ancoras[i+1]
            else:
                last_index = len(leituras)
                next_anchor = None
            while detection_index < last_index:
                detection = leituras[detection_index]
                detection_track = get_detection_track(leituras, detection, ancoras[i], next_anchor)
                if detection_track == "A":
                    trilha_a.append(detection)
                    ancoras[i]["fim_a"] = detection_index
                elif detection_track == "B":
                    trilha_b.append(detection)
                    ancoras[i]["fim_b"] = detection_index
                else:
                    ambiguos.append(detection)
                detection_index += 1
            i += 1

        return
