# -*- coding: utf-8 -*-
"""
Tasks da pipeline banco_clones.
"""
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task
from typing import Any, Dict, List, Literal

from prefect_rj_civitas import (
    save_data_in_bq_table
)
from pipelines.rj_civitas__banco_clones.subtasks import (
    resolve_start_date,
    fetch_new_clonned_plates_and_days,
    get_plates_readings,
    separate_suspect_pairs_into_tracks,
    create_anchors,
    get_valid_segment,
    apply_intermediate_and_last_detections_tracks
)
from pipelines.rj_civitas__banco_clones.utils import (
    get_detection_track
)

@task
def get_readings_task(
    start_date: str,
    readings_full_table_id: str,
    banco_clones_full_table_id: str,
    pares_suspeitos_full_table_id: str,
    trilhas_full_table_id: str
) -> list[dict] | None:
    log("Conecting to BigQuery")
    try:
        bq_client = bigquery.Client()
    except Exception as e:
        log("BigQuery connection error", level="error")
        raise(e)

    log("Fetching new cloned plate registers...")
    resolved_start_date = resolve_start_date(
        bq_client=bq_client,
        trilhas_table_id=trilhas_full_table_id,
        start_date=start_date
        )

    plates_days = fetch_new_clonned_plates_and_days(
        bq_client=bq_client,
        banco_clones_table_id=banco_clones_full_table_id,
        start_date=resolved_start_date
    )

    if not plates_days:
        log("No new clonned plates found")
        return None

    readings = get_plates_readings(
        bq_client=bq_client,
        plates_days=plates_days,
        readings_table_id=readings_full_table_id,
        pares_suspeitos_table_id=pares_suspeitos_full_table_id,
        start_date=resolved_start_date
        )

    return readings


@task
def get_tracks_task(
    readings: dict
):
    log("Separating tracks...")
    tracks_data = []
    for row in readings:
        placa = row["placa"]
        dia = row["dia"]
        leituras = row["leituras"]
        pares_suspeitos = row["pares_suspeitos_trilhas"]
        ambiguos = []

        trilha_a, trilha_b = separate_suspect_pairs_into_tracks(leituras, pares_suspeitos)

        ancoras = create_anchors(leituras, pares_suspeitos)

        first_anchor_index = min(ancoras[0]["inicio_a"], ancoras[0]["inicio_b"])
        if first_anchor_index >= 1:
            initial_segment, initial_outliers = get_valid_segment(leituras, 0, first_anchor_index)
            initial_segment_track = get_detection_track(leituras, initial_segment[-1], None, ancoras[0])
            if initial_segment_track == "A":
                trilha_a[:0] = initial_segment
                ancoras[0]["inicio_a"] = 0
                ambiguos = initial_outliers[:]
            elif initial_segment_track == "B":
                trilha_b[:0] = initial_segment
                ancoras[0]["inicio_b"] = 0
                ambiguos = initial_outliers[:]
            else:
                ambiguos = leituras[:first_anchor_index]

        apply_intermediate_and_last_detections_tracks(leituras, ancoras, trilha_a, trilha_b, ambiguos)

        civitas_in_track_a = civitas_in_track_b = False

        for detection in trilha_a:
            if not detection.get("suspeito"):
                detection["suspeito"] = False
            if detection["empresa"] == "CIVITAS":
                civitas_in_track_a = True

        for detection in trilha_b:
            if not detection.get("suspeito"):
                detection["suspeito"] = False
            if detection["empresa"] == "CIVITAS":
                civitas_in_track_b = True

        tracks_data.append({
            "placa": placa,
            "dia": dia,
            "trilha_a": trilha_a,
            "trilha_b": trilha_b,
            "deteccoes_ambiguas": ambiguos,
            "civitas_ambas_trilhas": civitas_in_track_a and civitas_in_track_b
        })
    log("Tracks successfully separated")
    return tracks_data


@task
def upload_to_table_task(
    project_id: str,
    dataset_id: str,
    table_id: str,
    data: List[Dict[str, Any]],
    write_disposition: Literal["WRITE_TRUNCATE", "WRITE_APPEND"] = "WRITE_APPEND"
):
    schema = schema = [
            bigquery.SchemaField(name="placa", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="dia", field_type="DATE", mode="REQUIRED"),
            bigquery.SchemaField(
                name="trilha_a",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="datahora", field_type="TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField(name="empresa", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="latitude", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="longitude", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="sentido", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="bairro", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="localidade", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="velocidade", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="id_ponto_coleta", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="camera_numero", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="suspeito", field_type="BOOLEAN", mode="NULLABLE")
                ],
            ),
            bigquery.SchemaField(
                name="trilha_b",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="datahora", field_type="TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField(name="empresa", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="latitude", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="longitude", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="sentido", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="bairro", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="localidade", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="velocidade", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="id_ponto_coleta", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="camera_numero", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="suspeito", field_type="BOOLEAN", mode="NULLABLE")
                ],
            ),
            bigquery.SchemaField(
                name="deteccoes_ambiguas",
                field_type="STRUCT",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="datahora", field_type="TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField(name="empresa", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="latitude", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="longitude", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="sentido", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="bairro", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="localidade", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="velocidade", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="id_ponto_coleta", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="camera_numero", field_type="STRING", mode="NULLABLE")
                ],
            ),
            bigquery.SchemaField(name="civitas_ambas_trilhas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="TIMESTAMP", mode="REQUIRED")
            ]

    log(f"Writing registers to {project_id}.{dataset_id}.{table_id}")

    save_data_in_bq_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            schema=schema,
            data=data,
            write_disposition=write_disposition,
            partition_field="dia",
            partition_granularity="MONTH",
            clustering_fields=["placa"],
            ignore_unknown_values=True,
            allow_field_addition=True,
            insert_timestamp_field="timestamp_insercao"
        )
    log(f"{len(data)} registers written to {project_id}.{dataset_id}.{table_id}")

