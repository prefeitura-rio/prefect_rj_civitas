# -*- coding: utf-8 -*-
import shapely
from shapely import force_2d
from shapely.geometry import MultiPolygon
from shapely.validation import make_valid
from iplanrio.pipelines_utils.logging import log

def corrigir_geometria(geom):
    if geom is None or geom.is_empty:
        return None

    try:
        # 1. Correção topológica básica inicial
        geom = force_2d(geom)
        geom = geom.simplify(tolerance=0.000001, preserve_topology=True)
        geom = make_valid(geom)

        # 2. Garante orientação correta (Mão Direita para o BigQuery)
        if geom.geom_type == 'MultiPolygon':
            poligonos = []
            for p in geom.geoms:
                if not p.is_empty:
                    poligonos.append(shapely.geometry.polygon.orient(p.buffer(0), sign=1.0))
            geom_final = MultiPolygon(poligonos)
        else:
            geom_final = shapely.geometry.polygon.orient(geom.buffer(0), sign=1.0)

        # Teste rápido de geração de WKT: se o próprio Shapely falhar aqui, o except captura
        _ = geom_final.wkt

        return geom_final

    except Exception as e:
        # Qualquer falha ou erro matemático retorna None imediatamente
        log(f"Falha na geração de geometria: {e}", level="warning")
        return None