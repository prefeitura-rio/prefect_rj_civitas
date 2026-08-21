# -*- coding: utf-8 -*-
from iplanrio.pipelines_utils.logging import log
from datetime import datetime
from typing import Literal
import numpy as np

from constants import *


def haversine_km(reading_1: dict, reading_2: dict) -> float:
    """Distância aproximada em quilômetros (círculo máximo) entre duas coordenadas.
    """
    lat1 = reading_1["latitude"]
    lon1 = reading_1["longitude"]
    lat2 = reading_2["latitude"]
    lon2 = reading_2["longitude"]
    raio = 6371.0088
    lat1_r, lat2_r = np.radians(lat1), np.radians(lat2)
    dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2) ** 2
    return float(2 * raio * np.arcsin(np.sqrt(a)))


def intervalo_horas(reading_1: dict, reading_2: dict) -> float:
    datetime_1 = datetime.fromisoformat(reading_1["datahora"])
    datetime_2 = datetime.fromisoformat(reading_2["datahora"])
    return (datetime_2 - datetime_1).total_seconds() / 3600


def custo_trecho(reading_1: dict, reading_2: dict) -> float:
    distancia = haversine_km(reading_1, reading_2)
    horas = intervalo_horas(reading_1, reading_2)
    velocidade = distancia / horas if horas > 0 else VMAX_CURTA_KMH + 1
    if horas <= 0 or (velocidade > VMAX_KMH and distancia >= LIMITE_DIST_MIN_KM) or velocidade > VMAX_CURTA_KMH:
        return CUSTO_INVIAVEL

    return distancia


def is_spike(reading_1, reading_2, reading_3):
    dist_ab = haversine_km(reading_1, reading_2)
    dist_bc = haversine_km(reading_2, reading_3)
    dist_ac = haversine_km(reading_1, reading_3)
    delta_ac_min = (datetime.fromisoformat(reading_1["datahora"]) - datetime.fromisoformat(reading_3["datahora"])).total_seconds() / 60
    return (
        dist_ab > DIST_SPIKE_CAUDA_INICIAL_KM
        and dist_bc > DIST_SPIKE_CAUDA_INICIAL_KM
        and dist_ac <= DIST_RETORNO_SPIKE_CAUDA_INICIAL_KM
        and delta_ac_min <= JANELA_RETORNO_SPIKE_CAUDA_MIN
    )

def get_detection_track(
        plate_detections: list[dict],
        detection: dict,
        previous_anchor: dict | None,
        next_anchor: dict | None
) -> Literal["A", "B", None]:
    cost_prev_a = cost_prev_b = cost_next_a = cost_next_b = 0

    if previous_anchor:
        previous_a = plate_detections[previous_anchor["fim_a"]]
        previous_b = plate_detections[previous_anchor["fim_b"]]
        if detection["datahora"] == previous_a["datahora"] \
            and haversine_km(detection, previous_a) < 0.5:
            return "A"
        elif detection["datahora"] == previous_b["datahora"] \
            and haversine_km(detection, previous_b) < 0.5:
            return "B"

        cost_prev_a = custo_trecho(previous_a, detection)
        cost_prev_b = custo_trecho(previous_b, detection)

    if next_anchor:
        next_a = plate_detections[next_anchor["inicio_a"]]
        next_b = plate_detections[next_anchor["inicio_b"]]
        if detection["datahora"] == next_a["datahora"] \
            and haversine_km(detection, next_a) < 0.5:
            return "A"
        elif detection["datahora"] == next_b["datahora"] \
            and haversine_km(detection, next_b) < 0.5:
            return "B"

        cost_next_a = custo_trecho(detection, next_a)
        cost_next_b = custo_trecho(detection, next_b)


    cost_total_a = cost_prev_a + cost_next_a
    cost_total_b = cost_prev_b + cost_next_b

    if cost_total_a < cost_total_b:
        best_track = "A"
        best_cost = cost_total_a
        worst_cost = cost_total_b
    elif cost_total_a > cost_total_b:
        best_track = "B"
        best_cost = cost_total_b
        worst_cost = cost_total_a
    else:
        return None

    diff_cost = worst_cost - best_cost
    ratio_cost = worst_cost / 0.0001 if best_cost == 0 else worst_cost / best_cost
    if best_cost >= CUSTO_INVIAVEL \
        or (diff_cost < DIF_CLAREZA_INSERCAO_KM \
            and ratio_cost < RAZAO_CLAREZA_INSERCAO):
        return None

    return best_track