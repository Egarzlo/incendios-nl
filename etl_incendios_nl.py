"""
Pipeline ETL diario — Sistema de Predicción de Incendios de Nuevo León
======================================================================
Ejecutar como cron job via GitHub Actions a las 6:00 AM CST.

Dependencias: pip install -r requirements.txt
Configuración: variables de entorno (secrets en GitHub Actions)
"""

import os
import json
import time
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

try:
    import numpy as np
    import joblib
    HAS_ML = True
except ImportError:
    HAS_ML = False

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_incendios.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── Configuración ──────────────────────────────────────────────────────────
FIRMS_MAP_KEY = os.getenv("FIRMS_MAP_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

NL_BBOX = {"west": -101.21, "south": 23.16, "east": -98.42, "north": 27.80}
NL_BBOX_STR = f"{NL_BBOX['west']},{NL_BBOX['south']},{NL_BBOX['east']},{NL_BBOX['north']}"

MUNICIPIOS_SHP = "data/2025_1_19_MUN/2025_1_19_MUN.shp"
MUNICIPIOS_SHP_CRS = (
    "+proj=lcc +lat_1=17.5 +lat_2=29.5 +lat_0=12 +lon_0=-102 "
    "+x_0=2500000 +y_0=0 +ellps=GRS80 +units=m +no_defs"
)

METEO_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]

# ─── Factor antropogenico: ecorregion SEMA NL + actividades por municipio ────
# La ecorregion (R1-R5) deriva del mapa oficial de ecorregiones de SEMA NL y
# funciona como proxy del comportamiento del fuego por tipo de combustible.
# Las actividades son capas humanas que modulan el riesgo de ignicion segun
# la temporada. Ver README / documentacion para la justificacion de pesos.
ZONA_BY_CVE = {
    "001": ("R1", []),
    "002": ("R1", []),
    "003": ("R1", []),
    "004": ("R3", ["citrica", "turismo_alto"]),
    "005": ("R1", []),
    "006": ("R1", ["amm_nucleo"]),
    "007": ("R5", ["ganaderia_ext"]),
    "008": ("R1", ["turismo_alto"]),
    "009": ("R3", ["citrica", "amm_periurbano", "turismo_medio"]),
    "010": ("R1", ["amm_periurbano"]),
    "011": ("R1", []),
    "012": ("R1", ["amm_periurbano"]),
    "013": ("R1", []),
    "014": ("R5", ["ganaderia_ext"]),
    "015": ("R1", []),
    "016": ("R1", []),
    "017": ("R4", ["ganaderia_ext", "turismo_alto"]),
    "018": ("R1", ["amm_nucleo", "frontera_sierra"]),
    "019": ("R3", ["amm_nucleo", "frontera_sierra"]),
    "020": ("R1", []),
    "021": ("R1", ["amm_nucleo"]),
    "022": ("R3", ["citrica"]),
    "023": ("R1", []),
    "024": ("R5", ["ganaderia_ext"]),
    "025": ("R1", ["amm_periurbano"]),
    "026": ("R1", ["amm_nucleo"]),
    "027": ("R1", []),
    "028": ("R1", []),
    "029": ("R3", ["citrica"]),
    "030": ("R4", ["turismo_medio"]),
    "031": ("R1", ["amm_nucleo"]),
    "032": ("R1", []),
    "033": ("R3", ["citrica", "turismo_medio"]),
    "034": ("R1", []),
    "035": ("R1", []),
    "036": ("R5", ["ganaderia_ext"]),
    "037": ("R2", []),
    "038": ("R3", ["citrica"]),
    "039": ("R3", ["amm_nucleo", "frontera_sierra"]),
    "040": ("R1", []),
    "041": ("R1", ["amm_periurbano"]),
    "042": ("R1", []),
    "043": ("R4", ["turismo_medio"]),
    "044": ("R1", []),
    "045": ("R1", ["amm_periurbano"]),
    "046": ("R1", ["amm_nucleo"]),
    "047": ("R1", []),
    "048": ("R3", ["amm_nucleo", "frontera_sierra"]),
    "049": ("R3", ["turismo_alto", "frontera_sierra"]),
    "050": ("R1", []),
    "051": ("R1", []),
}

# Pts base por ecorregion segun evento calendario
BASE_POR_REGION = {
    "semana_santa":      {"R1": 4, "R2": 2, "R3": 6,  "R4": 10, "R5": 5},
    "quemas_agricolas":  {"R1": 5, "R2": 2, "R3": 10, "R4": 4,  "R5": 8},
    "residuos_cosecha":  {"R1": 3, "R2": 1, "R3": 5,  "R4": 2,  "R5": 4},
    "vacaciones_verano": {"R1": 2, "R2": 1, "R3": 4,  "R4": 8,  "R5": 3},
    "navidad":           {"R1": 4, "R2": 3, "R3": 5,  "R4": 5,  "R5": 4},
}
FACTOR_CAP = 20  # cap total de puntos


# ─── Supabase Client (lightweight, no SDK dependency issues) ────────────────
class SupabaseClient:
    """Cliente ligero para Supabase REST API."""

    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    def select(self, table: str, params: dict = None) -> list:
        url = f"{self.url}/rest/v1/{table}"
        r = requests.get(url, headers=self.headers, params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def insert(self, table: str, rows: list) -> list:
        url = f"{self.url}/rest/v1/{table}"
        r = requests.post(url, headers=self.headers, json=rows, timeout=60)
        if not r.ok:
            log.error(f"Supabase INSERT {table} error {r.status_code}: {r.text[:500]}")
            r.raise_for_status()
        return r.json()

    def upsert(self, table: str, rows: list, on_conflict: str = "") -> list:
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates,return=representation"}
        url = f"{self.url}/rest/v1/{table}"
        if on_conflict:
            url += f"?on_conflict={on_conflict}"
        r = requests.post(url, headers=headers, json=rows, timeout=60)
        if not r.ok:
            log.error(f"Supabase UPSERT {table} error {r.status_code}: {r.text[:500]}")
            r.raise_for_status()
        return r.json()


# ─── Paso 1: Fetch hotspots de NASA FIRMS ───────────────────────────────────
# Cuatro fuentes NRT activas hoy (abril 2026):
#  - VIIRS_NOAA20_NRT : satelite NOAA-20, ~375m, desde 2018
#  - VIIRS_SNPP_NRT   : Suomi NPP, ~375m, desde 2012
#  - VIIRS_NOAA21_NRT : NOAA-21 (JPSS-2), ~375m, operacional desde 2023
#  - MODIS_NRT        : Terra + Aqua combinado, ~1km, legado (mayor cobertura
#                       historica, util para continuidad y redundancia)
FIRMS_SOURCES = ["VIIRS_NOAA20_NRT", "VIIRS_SNPP_NRT", "VIIRS_NOAA21_NRT", "MODIS_NRT"]


def _safe_parse_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (ValueError, TypeError):
        return default


def fetch_firms_hotspots(day_range: int = 2) -> list[dict]:
    import csv
    import io

    all_hotspots = []
    per_source_counts = {}

    for source in FIRMS_SOURCES:
        url = (
            f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            f"{FIRMS_MAP_KEY}/{source}/{NL_BBOX_STR}/{day_range}"
        )
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            count = 0
            errors = 0
            for row in reader:
                try:
                    lat = _safe_parse_float(row.get("latitude"))
                    lon = _safe_parse_float(row.get("longitude"))
                    if lat == 0.0 and lon == 0.0:
                        errors += 1
                        continue
                    # VIIRS usa bright_ti4, MODIS usa brightness
                    brightness = _safe_parse_float(row.get("bright_ti4")) or _safe_parse_float(row.get("brightness"))
                    all_hotspots.append({
                        "latitude": lat,
                        "longitude": lon,
                        "brightness": brightness,
                        "frp": _safe_parse_float(row.get("frp")),
                        "confidence": (row.get("confidence") or "")[:10],
                        "satellite": row.get("satellite") or source,
                        "source": source,
                        "detected_at": parse_firms_datetime(
                            row.get("acq_date", ""), row.get("acq_time", "0000")
                        ),
                    })
                    count += 1
                except Exception as e:
                    errors += 1
                    # No abortamos toda la fuente por una fila mal formada
                    continue
            per_source_counts[source] = count
            if count == 0 and errors == 0:
                log.info(f"FIRMS {source}: sin hotspots detectados")
            elif errors:
                log.info(f"FIRMS {source}: {count} hotspots descargados, {errors} filas ignoradas")
            else:
                log.info(f"FIRMS {source}: {count} hotspots descargados")
        except Exception as e:
            log.error(f"Error fetching FIRMS {source}: {e}")
            per_source_counts[source] = 0

    total = sum(per_source_counts.values())
    if total:
        detalle = ", ".join(f"{s.split('_')[-2] if len(s.split('_'))>2 else s}: {n}" for s, n in per_source_counts.items())
        log.info(f"FIRMS total: {total} hotspots de 4 fuentes ({detalle})")
    return all_hotspots


def parse_firms_datetime(acq_date: str, acq_time: str) -> str:
    """Parsea fecha/hora de FIRMS a formato ISO 8601 válido para PostgreSQL."""
    try:
        acq_time = str(acq_time).zfill(4)
        hh = acq_time[:2]
        mm = acq_time[2:4]
        return f"{acq_date}T{hh}:{mm}:00+00:00"
    except Exception:
        return f"{acq_date}T00:00:00+00:00"


# ─── Paso 2: Fetch meteorología de Open-Meteo (BATCH) ───────────────────────
def fetch_open_meteo(municipios: list[dict], days_back: int = 7) -> dict:
    """
    Usa la API de Open-Meteo con múltiples coordenadas en una sola llamada.
    Open-Meteo acepta listas de lat/lon separadas por coma.
    Hacemos lotes de 15 municipios para no saturar.
    """
    results = {}
    today = date.today()
    start = today - timedelta(days=days_back)
    batch_size = 15

    for i in range(0, len(municipios), batch_size):
        batch = municipios[i:i + batch_size]
        lats = ",".join(str(m["lat_centroide"]) for m in batch)
        lons = ",".join(str(m["lon_centroide"]) for m in batch)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lats,
            "longitude": lons,
            "daily": ",".join(METEO_DAILY_VARS),
            "start_date": start.isoformat(),
            "end_date": today.isoformat(),
            "timezone": "America/Monterrey",
        }

        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            # Si es un solo municipio, Open-Meteo retorna objeto; si son varios, retorna lista
            if isinstance(data, dict):
                data = [data]

            for j, muni_data_raw in enumerate(data):
                if j >= len(batch):
                    break
                cve = batch[j]["cve_muni"]
                daily = muni_data_raw.get("daily", {})
                times = daily.get("time", [])

                muni_data = []
                for k, fecha_str in enumerate(times):
                    muni_data.append({
                        "fecha": fecha_str,
                        "temp_max": safe_float(daily.get("temperature_2m_max", []), k),
                        "temp_min": safe_float(daily.get("temperature_2m_min", []), k),
                        "humedad_min": safe_float(daily.get("relative_humidity_2m_min", []), k),
                        "viento_max": safe_float(daily.get("wind_speed_10m_max", []), k),
                        "precipitacion": safe_float(daily.get("precipitation_sum", []), k),
                        "et0": safe_float(daily.get("et0_fao_evapotranspiration", []), k),
                    })
                results[cve] = muni_data

            log.info(f"Open-Meteo lote {i//batch_size + 1}: {len(batch)} municipios OK")

        except Exception as e:
            log.error(f"Error Open-Meteo lote {i//batch_size + 1}: {e}")
            # Fallback: intentar uno por uno con pausa
            for m in batch:
                cve = m["cve_muni"]
                if cve not in results:
                    try:
                        time.sleep(1)
                        resp2 = requests.get(url, params={
                            "latitude": m["lat_centroide"],
                            "longitude": m["lon_centroide"],
                            "daily": ",".join(METEO_DAILY_VARS),
                            "start_date": start.isoformat(),
                            "end_date": today.isoformat(),
                            "timezone": "America/Monterrey",
                        }, timeout=30)
                        resp2.raise_for_status()
                        d = resp2.json().get("daily", {})
                        ts = d.get("time", [])
                        results[cve] = [{
                            "fecha": ts[k],
                            "temp_max": safe_float(d.get("temperature_2m_max", []), k),
                            "temp_min": safe_float(d.get("temperature_2m_min", []), k),
                            "humedad_min": safe_float(d.get("relative_humidity_2m_min", []), k),
                            "viento_max": safe_float(d.get("wind_speed_10m_max", []), k),
                            "precipitacion": safe_float(d.get("precipitation_sum", []), k),
                            "et0": safe_float(d.get("et0_fao_evapotranspiration", []), k),
                        } for k in range(len(ts))]
                    except Exception as e2:
                        log.error(f"Error Open-Meteo individual {cve}: {e2}")
                        results[cve] = []

        # Pausa entre lotes para no saturar la API
        if i + batch_size < len(municipios):
            time.sleep(2)

    ok_count = sum(1 for v in results.values() if v)
    log.info(f"Open-Meteo: datos obtenidos para {ok_count}/{len(municipios)} municipios")
    return results


def safe_float(lst, idx):
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


# ─── Paso 3: Geocodificar hotspots a municipios ─────────────────────────────
def cargar_municipios_shapely() -> list[tuple]:
    """
    Carga el shapefile oficial INEGI (Marco Geoestadístico 2025) y reproyecta
    cada polígono de LCC México ITRF 2008 a WGS84 (EPSG:4326).

    Retorna: lista de (cve_muni, shapely.Polygon/MultiPolygon en WGS84).

    Aborta si el shapefile no se puede cargar — no hay fallback impreciso.
    """
    import shapefile  # pyshp
    from shapely.geometry import shape
    from shapely.ops import transform as shp_transform
    from pyproj import Transformer

    if not os.path.exists(MUNICIPIOS_SHP):
        raise FileNotFoundError(
            f"Shapefile municipal no encontrado en {MUNICIPIOS_SHP}. "
            "Requerido para geocodificación correcta — abortando ETL."
        )

    tr = Transformer.from_crs(MUNICIPIOS_SHP_CRS, "EPSG:4326", always_xy=True)
    reproject = lambda x, y, z=None: tr.transform(x, y)

    sf = shapefile.Reader(MUNICIPIOS_SHP, encoding="cp1252")
    field_names = [f[0] for f in sf.fields[1:]]
    idx_cve = field_names.index("CVE_MUN")
    idx_ent = field_names.index("CVE_ENT") if "CVE_ENT" in field_names else None

    municipios_geom = []
    for sr in sf.shapeRecords():
        rec = sr.record
        if idx_ent is not None and rec[idx_ent] != "19":
            continue  # defensivo: solo NL
        cve = str(rec[idx_cve]).zfill(3)
        geom_lcc = shape(sr.shape.__geo_interface__)
        geom_wgs = shp_transform(reproject, geom_lcc)
        municipios_geom.append((cve, geom_wgs))

    if len(municipios_geom) != 51:
        raise ValueError(
            f"Se esperaban 51 municipios de NL, se cargaron {len(municipios_geom)}. "
            "Revisa el shapefile."
        )

    log.info(f"Shapefile cargado: {len(municipios_geom)} municipios de NL (INEGI 2025.1)")
    return municipios_geom


def geocode_hotspots_shapely(hotspots: list[dict], municipios_geom: list[tuple]) -> list[dict]:
    """
    Asigna cada hotspot al municipio cuyo polígono lo contiene.
    Los hotspots fuera de NL se descartan (no se asignan al centroide más cercano).
    """
    from shapely.geometry import Point
    from shapely.strtree import STRtree

    geoms = [g for _, g in municipios_geom]
    cves = [c for c, _ in municipios_geom]
    tree = STRtree(geoms)

    geocoded = []
    fuera = 0
    for h in hotspots:
        pt = Point(h["longitude"], h["latitude"])
        candidatos = tree.query(pt)  # índices de polígonos cuyo bbox intersecta el punto
        asignado = False
        for idx in candidatos:
            if geoms[int(idx)].covers(pt):
                h2 = dict(h)
                h2["cve_muni"] = cves[int(idx)]
                geocoded.append(h2)
                asignado = True
                break
        if not asignado:
            fuera += 1

    total = len(hotspots)
    pct_fuera = (fuera / total * 100) if total else 0
    log.info(
        f"Geocodificación: {len(geocoded)}/{total} hotspots dentro de NL, "
        f"{fuera} descartados fuera del estado ({pct_fuera:.1f}%)"
    )
    if total >= 10 and pct_fuera > 50:
        log.warning(
            f"⚠️ Más del 50% de hotspots cayeron fuera de NL ({fuera}/{total}). "
            "Revisar cobertura del bbox FIRMS o calidad de datos."
        )
    return geocoded


# ─── Factor antropogenico ──────────────────────────────────────────────────
def _easter(year: int) -> date:
    """Domingo de Pascua (gregoriano) — algoritmo Butcher/Anonymous."""
    a = year % 19
    b, c = year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _viernes_santo(year: int) -> date:
    return _easter(year) - timedelta(days=2)


def calcular_factor_antropogenico(cve_muni: str, fecha: date) -> tuple[int, list[str]]:
    """
    Calcula el factor antropogenico para un municipio en una fecha dada.

    Retorna (pts, etiquetas) donde pts es 0-20 y etiquetas lista los eventos
    calendario activos. Los pts se suman luego a prob_base: 20 pts => +0.20
    en probabilidad. Cap total en FACTOR_CAP.
    """
    zona = ZONA_BY_CVE.get(cve_muni)
    if not zona:
        return 0, []
    ecoregion, activities = zona
    pts = 0
    etiquetas: list[str] = []

    def bono_turismo(ev_aplica_alto: bool, ev_aplica_medio: bool) -> int:
        b = 0
        if ev_aplica_alto and "turismo_alto" in activities:
            b += 3
        if ev_aplica_medio and "turismo_medio" in activities:
            b += 2
        return b

    def bono_amm_frontera() -> int:
        b = 0
        if "amm_nucleo" in activities:
            b += 2
        elif "amm_periurbano" in activities:
            b += 1
        if "frontera_sierra" in activities:
            b += 2
        return b

    mes, dia = fecha.month, fecha.day

    # Semana Santa (±7 dias alrededor de Viernes Santo)
    try:
        vs = _viernes_santo(fecha.year)
        if abs((fecha - vs).days) <= 7:
            pts += BASE_POR_REGION["semana_santa"][ecoregion]
            pts += bono_turismo(True, True) + bono_amm_frontera()
            etiquetas.append("Semana Santa")
    except Exception:
        pass

    # Quemas agricolas (15 ene - 31 may)
    if (mes == 1 and dia >= 15) or (2 <= mes <= 5):
        pts += BASE_POR_REGION["quemas_agricolas"][ecoregion]
        if "citrica" in activities:
            pts += 3
        if "ganaderia_ext" in activities:
            pts += 2
        etiquetas.append("Quemas agricolas")

    # Residuos de cosecha (octubre)
    if mes == 10:
        pts += BASE_POR_REGION["residuos_cosecha"][ecoregion]
        if "citrica" in activities:
            pts += 3
        if "ganaderia_ext" in activities:
            pts += 2
        etiquetas.append("Residuos de cosecha")

    # Vacaciones de verano (jul-ago)
    if mes in (7, 8):
        pts += BASE_POR_REGION["vacaciones_verano"][ecoregion]
        pts += bono_turismo(True, True) + bono_amm_frontera()
        etiquetas.append("Vacaciones verano")

    # Temporada navidena (22 dic - 6 ene)
    if (mes == 12 and dia >= 22) or (mes == 1 and dia <= 6):
        pts += BASE_POR_REGION["navidad"][ecoregion]
        if "turismo_alto" in activities:
            pts += 3
        etiquetas.append("Temporada navidena")

    return min(pts, FACTOR_CAP), etiquetas


def _nivel_desde_prob(prob: float) -> str:
    if prob >= 0.8: return "EXTREMO"
    if prob >= 0.6: return "MUY_ALTO"
    if prob >= 0.4: return "ALTO"
    if prob >= 0.2: return "MEDIO"
    return "BAJO"


# ─── Paso 4: Features y modelo de riesgo ────────────────────────────────────
def calcular_dias_sin_lluvia(clima_historico: list[dict]) -> int:
    dias = 0
    for dia in reversed(clima_historico):
        precip = dia.get("precipitacion") or 0
        if precip < 1.0:
            dias += 1
        else:
            break
    return dias


def calcular_riesgo(features: dict) -> tuple[float, str]:
    score = 0.0

    dsl = features.get("dias_sin_lluvia", 0)
    if dsl >= 14: score += 35
    elif dsl >= 7: score += 25
    elif dsl >= 3: score += 15
    else: score += 5

    temp = features.get("temp_max") or 0
    if temp >= 40: score += 20
    elif temp >= 35: score += 15
    elif temp >= 30: score += 10
    else: score += 3

    hum = features.get("humedad_min") or 100
    if hum <= 15: score += 20
    elif hum <= 25: score += 15
    elif hum <= 40: score += 10
    else: score += 3

    viento = features.get("viento_max") or 0
    if viento >= 50: score += 15
    elif viento >= 30: score += 10
    elif viento >= 15: score += 5

    n_hs = features.get("n_hotspots_24h", 0)
    if n_hs >= 3: score += 10
    elif n_hs >= 1: score += 7

    prob = min(score / 100.0, 1.0)

    if prob >= 0.8: nivel = "EXTREMO"
    elif prob >= 0.6: nivel = "MUY_ALTO"
    elif prob >= 0.4: nivel = "ALTO"
    elif prob >= 0.2: nivel = "MEDIO"
    else: nivel = "BAJO"

    return prob, nivel


# ─── Paso 4b: Modelo ML ─────────────────────────────────────────────────────
def cargar_modelo_ml(model_path: str = None) -> Optional[dict]:
    """Carga el modelo ML (.pkl) si está disponible."""
    if not HAS_ML:
        log.warning("numpy/joblib no instalados — modelo ML deshabilitado")
        return None

    if model_path is None:
        # Buscar en el mismo directorio que el script
        script_dir = Path(__file__).parent
        model_path = str(script_dir / "modelo_incendios_nl.pkl")

    if not os.path.exists(model_path):
        log.warning(f"Modelo ML no encontrado: {model_path}")
        return None

    try:
        model_data = joblib.load(model_path)
        log.info(f"Modelo ML cargado: {model_data.get('model_name', '?')} v{model_data.get('version', '?')}")
        return model_data
    except Exception as e:
        log.error(f"Error cargando modelo ML: {e}")
        return None


def predecir_ml(model_data: dict, features: dict, muni_info: dict) -> tuple[float, str]:
    """Genera predicción usando el modelo ML."""
    feature_names = model_data["features"]
    threshold = model_data.get("threshold", 0.5)
    model = model_data["model"]
    scaler = model_data.get("scaler")

    # Ecoregion: del catalogo ZONA_BY_CVE (fallback 1=llanos)
    cve_muni = muni_info.get("cve_muni", "")
    ecoregion_str = ZONA_BY_CVE.get(cve_muni, ("R1", []))[0]
    ecoregion_int = {"R1":1, "R2":2, "R3":3, "R4":4, "R5":5}.get(ecoregion_str, 1)

    # Mapear features del ETL a los features del modelo
    feature_map = {
        "temp_max": features.get("temp_max") or 0,
        "temp_min": features.get("temp_min") or 0,
        "humedad_min": features.get("humedad_min") or 50,
        "viento_max": features.get("viento_max") or 0,
        "precipitacion": features.get("precipitacion") or 0,
        "et0": features.get("et0") or 0,
        "dias_sin_lluvia": features.get("dias_sin_lluvia", 0),
        "dias_sin_lluvia_30d": features.get("dias_sin_lluvia_30d", 0),
        "mes": date.today().month,
        "dia_del_ano": date.today().timetuple().tm_yday,
        "lat": muni_info.get("lat_centroide", 25.5),
        "lon": muni_info.get("lon_centroide", -100.0),
        "elevacion": muni_info.get("elevacion_media", 500),
        "ecoregion": ecoregion_int,
    }

    X = np.array([[feature_map.get(f, 0) for f in feature_names]])

    if scaler is not None:
        X = scaler.transform(X)

    prob = model.predict_proba(X)[0, 1]

    if prob >= 0.8: nivel = "EXTREMO"
    elif prob >= 0.6: nivel = "MUY_ALTO"
    elif prob >= 0.4: nivel = "ALTO"
    elif prob >= 0.2: nivel = "MEDIO"
    else: nivel = "BAJO"

    return float(prob), nivel


# ─── Paso 5: Alertas ────────────────────────────────────────────────────────
def explicar_condiciones(f: dict) -> str:
    """Genera texto explicativo en lenguaje sencillo para personal operativo."""
    partes = []
    dsl = f.get("dias_sin_lluvia", 0)
    temp = f.get("temp_max") or 0
    hum = f.get("humedad_min") or 100
    viento = f.get("viento_max") or 0
    hs = f.get("n_hotspots_24h", 0)

    if dsl >= 14:
        partes.append(f"Llevan {dsl} dias sin lluvia, la vegetacion esta muy seca y puede arder con facilidad.")
    elif dsl >= 7:
        partes.append(f"Van {dsl} dias sin lluvia. La vegetacion ha perdido humedad y es mas vulnerable al fuego.")
    elif dsl >= 3:
        partes.append(f"Han pasado {dsl} dias sin lluvia, la vegetacion aun conserva algo de humedad.")
    else:
        partes.append(f"Ha llovido recientemente ({dsl} dias sin lluvia), lo que reduce el riesgo.")

    if temp >= 40:
        partes.append(f"La temperatura maxima es de {temp}°C, extremadamente alta, lo que facilita la propagacion del fuego.")
    elif temp >= 35:
        partes.append(f"Se esperan {temp}°C de maxima, lo cual seca aun mas la vegetacion.")
    elif temp >= 30:
        partes.append(f"La temperatura es de {temp}°C, moderadamente alta.")

    if hum <= 15:
        partes.append(f"La humedad es de solo {hum}%, criticamente baja. Cualquier chispa puede iniciar fuego.")
    elif hum <= 25:
        partes.append(f"La humedad es de {hum}%, lo que indica un ambiente seco que favorece la ignicion.")
    elif hum <= 40:
        partes.append(f"Humedad de {hum}%, por debajo de lo ideal.")

    if viento >= 50:
        partes.append(f"Vientos fuertes de {viento} km/h que pueden propagar un incendio rapidamente.")
    elif viento >= 30:
        partes.append(f"Vientos de {viento} km/h, suficientes para avivar y extender un incendio.")

    if hs >= 3:
        partes.append(f"Se detectaron {hs} puntos de calor por satelite en las ultimas 24h, lo que sugiere fuego activo.")
    elif hs >= 1:
        partes.append(f"Se detecto {hs} punto de calor satelital en las ultimas 24h.")

    return " ".join(partes)


def generar_mensaje(pred: dict, contacto: dict) -> str:
    f = pred["features"]
    explicacion = explicar_condiciones(f)
    muni = pred.get("muni_nombre", pred["cve_muni"])
    nivel = pred["nivel"].replace("_", " ")
    prob = pred["prob"]

    return (
        f"ALERTA DE INCENDIO — {muni}, Nuevo Leon\n\n"
        f"Estimado(a) {contacto.get('nombre', 'Funcionario')},\n\n"
        f"Nivel de riesgo: {nivel} (probabilidad: {prob:.0%})\n\n"
        f"¿Por que este nivel?\n"
        f"{explicacion}\n\n"
        f"Datos del dia:\n"
        f"- Temp. maxima: {f.get('temp_max', 'N/D')}°C\n"
        f"- Humedad minima: {f.get('humedad_min', 'N/D')}%\n"
        f"- Viento maximo: {f.get('viento_max', 'N/D')} km/h\n"
        f"- Dias sin lluvia: {f.get('dias_sin_lluvia', 'N/D')}\n"
        f"- Hotspots activos 24h: {f.get('n_hotspots_24h', 0)}\n\n"
        f"Se recomienda activar protocolos preventivos.\n"
        f"Dashboard: https://incendios-nl.netlify.app\n\n"
        f"— Sistema de Prediccion de Incendios, BIOIMPACT / SEMA NL"
    )


def generar_resumen_diario(predicciones: list) -> str:
    """Genera un resumen diario breve de todos los municipios para WhatsApp."""
    fecha = date.today().isoformat()
    niveles = {"EXTREMO": [], "MUY_ALTO": [], "ALTO": [], "MEDIO": [], "BAJO": []}
    for p in predicciones:
        niveles.setdefault(p["nivel"], []).append(p["muni_nombre"])

    lineas = [f"REPORTE DIARIO DE INCENDIOS — Nuevo Leon\nFecha: {fecha}\n"]

    for nivel in ["EXTREMO", "MUY_ALTO", "ALTO", "MEDIO", "BAJO"]:
        munis = niveles.get(nivel, [])
        if not munis:
            continue
        emoji = {"EXTREMO": "🔴", "MUY_ALTO": "🟠", "ALTO": "🟡", "MEDIO": "🔵", "BAJO": "🟢"}
        label = nivel.replace("_", " ")
        lineas.append(f"{emoji.get(nivel, '')} {label} ({len(munis)}): {', '.join(munis[:8])}")
        if len(munis) > 8:
            lineas[-1] += f" y {len(munis)-8} mas"

    # Top 3 con más riesgo
    top3 = sorted(predicciones, key=lambda p: p["prob"], reverse=True)[:3]
    if top3:
        lineas.append("\nMunicipios con mayor riesgo:")
        for p in top3:
            f = p["features"]
            lineas.append(
                f"  {p['muni_nombre']}: {p['prob']:.0%} — "
                f"{f.get('temp_max', '?')}°C, {f.get('humedad_min', '?')}% hum, "
                f"{f.get('dias_sin_lluvia', '?')}d sin lluvia"
            )

    lineas.append(f"\nDashboard: https://incendios-nl.netlify.app")
    lineas.append("— BIOIMPACT / SEMA NL")
    return "\n".join(lineas)


def enviar_email(destinatario: str, mensaje: str, pred: dict):
    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key:
        log.warning("SENDGRID_API_KEY no configurada")
        return False

    muni = pred.get("muni_nombre", pred["cve_muni"])
    payload = {
        "personalizations": [{"to": [{"email": destinatario}]}],
        "from": {"email": "alertas@bioimpact.mx", "name": "Alertas Incendios NL"},
        "subject": f"Alerta incendio {pred['nivel']} — {muni}, NL",
        "content": [{"type": "text/plain", "value": mensaje}],
    }
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        json=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    log.info(f"Email enviado a {destinatario}")
    return True


def enviar_whatsapp(telefono: str, mensaje: str):
    """Envía mensaje por WhatsApp usando Twilio."""
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    from_num = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")  # Sandbox default
    if not all([sid, token]):
        log.warning("Twilio no configurado para WhatsApp")
        return False

    # Asegurar formato whatsapp:
    to_num = telefono if telefono.startswith("whatsapp:") else f"whatsapp:{telefono}"

    # WhatsApp tiene límite de 1600 caracteres
    wa_msg = mensaje[:1550] + "..." if len(mensaje) > 1600 else mensaje

    resp = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
        data={"To": to_num, "From": from_num, "Body": wa_msg},
        auth=(sid, token),
        timeout=30,
    )
    if not resp.ok:
        log.error(f"Error WhatsApp a {telefono}: {resp.status_code} — {resp.text[:200]}")
        return False
    log.info(f"WhatsApp enviado a {telefono}")
    return True


def enviar_sms(telefono: str, mensaje: str):
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    from_num = os.getenv("TWILIO_FROM_NUMBER")
    if not all([sid, token, from_num]):
        log.warning("Twilio no configurado")
        return False

    sms_msg = mensaje[:155] + "..." if len(mensaje) > 160 else mensaje
    resp = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
        data={"To": telefono, "From": from_num, "Body": sms_msg},
        auth=(sid, token),
        timeout=30,
    )
    resp.raise_for_status()
    log.info(f"SMS enviado a {telefono}")
    return True


# ─── Suscripciones publicas via Mailjet ────────────────────────────────────
MAILJET_API_KEY    = os.getenv("MAILJET_API_KEY")
MAILJET_API_SECRET = os.getenv("MAILJET_API_SECRET")
MAILJET_FROM_EMAIL = os.getenv("MAILJET_FROM_EMAIL")
MAILJET_FROM_NAME  = os.getenv("MAILJET_FROM_NAME", "Alertas Incendios Nuevo Leon")
DASHBOARD_URL      = os.getenv("DASHBOARD_URL", "https://incendios-nl.netlify.app").rstrip("/")

NIVEL_ORDER = {"BAJO": 0, "MEDIO": 1, "ALTO": 2, "MUY_ALTO": 3, "EXTREMO": 4}


def _nivel_ge(n1: str, n2: str) -> bool:
    """True si n1 >= n2 en escala de nivel."""
    return NIVEL_ORDER.get(n1, 0) >= NIVEL_ORDER.get(n2, 0)


def _html_escape(s):
    if s is None:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def enviar_email_mailjet(to_email: str, subject: str, html_body: str, text_body: str) -> tuple[bool, str]:
    """Envia un correo via Mailjet API v3.1. Retorna (ok, error_msg)."""
    if not all([MAILJET_API_KEY, MAILJET_API_SECRET, MAILJET_FROM_EMAIL]):
        return False, "Mailjet no configurado (faltan env vars)"
    payload = {"Messages": [{
        "From": {"Email": MAILJET_FROM_EMAIL, "Name": MAILJET_FROM_NAME},
        "To": [{"Email": to_email}],
        "Subject": subject,
        "TextPart": text_body,
        "HTMLPart": html_body,
    }]}
    try:
        resp = requests.post(
            "https://api.mailjet.com/v3.1/send",
            auth=(MAILJET_API_KEY, MAILJET_API_SECRET),
            json=payload,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, ""
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, str(e)


def _bloque_muni(p: dict, predicciones_ml_por_cve: dict) -> tuple[str, str]:
    """Genera (html, text) para un municipio combinando prediccion reglas + ML."""
    nom = p.get("muni_nombre", p.get("cve_muni", ""))
    f = p.get("features", {})
    prob_reglas = p.get("prob", 0) * 100
    nivel_reglas = p.get("nivel", "BAJO").replace("_", " ")

    # ML correspondiente
    ml = predicciones_ml_por_cve.get(p["cve_muni"])
    if ml:
        prob_ml = ml["prob"] * 100
        nivel_ml = ml["nivel"].replace("_", " ")
    else:
        prob_ml = None
        nivel_ml = None

    # Explicacion operativa: causas climaticas dominantes + factor
    razones = []
    dsl = f.get("dias_sin_lluvia")
    if dsl and dsl >= 14: razones.append(f"{dsl} dias sin lluvia")
    elif dsl and dsl >= 7: razones.append(f"{dsl} dias sin lluvia")
    temp = f.get("temp_max")
    if temp and temp >= 35: razones.append(f"{temp}°C maxima")
    hum = f.get("humedad_min")
    if hum is not None and hum <= 25: razones.append(f"{hum}% humedad")
    viento = f.get("viento_max")
    if viento and viento >= 30: razones.append(f"viento {viento} km/h")
    hs = f.get("n_hotspots_24h", 0)
    if hs >= 1: razones.append(f"{hs} hotspot{'s' if hs>1 else ''} satelital{'es' if hs>1 else ''} 24h")
    etiquetas_fa = p.get("factor_etiquetas", []) or []
    pts_fa = p.get("factor_pts", 0) or 0
    if pts_fa > 0 and etiquetas_fa:
        razones.append(f"+{pts_fa} pts por {', '.join(etiquetas_fa).lower()}")
    razon_txt = "; ".join(razones) if razones else "patron estacional y geografia"

    # HTML
    ml_html = f"<br/><strong>Prediccion ML:</strong> {nivel_ml} ({prob_ml:.0f}%)" if ml else ""
    html = (
        f"<div style='border:1px solid #e0e0d8;border-radius:8px;padding:12px;margin:10px 0;background:#fff'>"
        f"<div style='font-weight:600;color:#1A7A6E;font-size:15px'>{_html_escape(nom)}</div>"
        f"<div style='font-size:13px;margin-top:6px'>"
        f"<strong>Condiciones climaticas:</strong> {_html_escape(nivel_reglas)} ({prob_reglas:.0f}%){ml_html}"
        f"</div>"
        f"<div style='font-size:12px;color:#555;margin-top:6px;line-height:1.5'>"
        f"<em>Por que:</em> {_html_escape(razon_txt)}"
        f"</div></div>"
    )
    # Text
    text = f"{nom}\n  Condiciones climaticas: {nivel_reglas} ({prob_reglas:.0f}%)"
    if ml:
        text += f"\n  Prediccion ML: {nivel_ml} ({prob_ml:.0f}%)"
    text += f"\n  Por que: {razon_txt}\n"
    return html, text


def generar_email_suscriptor(sub: dict, predicciones_reglas: list, predicciones_ml: list, fecha_iso: str) -> tuple[str, str, str, int, int]:
    """
    Arma (asunto, html, text, n_munis_reportados, n_alertas) para un suscriptor.
    Primera parte: sus municipios seleccionados con detalle.
    Segunda parte: panorama general del estado.
    """
    munis_pref = set(sub.get("municipios_cve") or [])
    todos = "*" in munis_pref

    ml_por_cve = {p["cve_muni"]: p for p in predicciones_ml}
    reglas_por_cve = {p["cve_muni"]: p for p in predicciones_reglas}

    # Seleccionados: si el user eligio "*" o nada, mostramos los que superan nivel_minimo
    if todos or not munis_pref:
        # Default: solo los que superen nivel_minimo, top 10
        nivel_min = sub.get("nivel_minimo", "ALTO")
        munis_mostrar = sorted(
            [p for p in predicciones_reglas if _nivel_ge(p["nivel"], nivel_min)],
            key=lambda x: -x["prob"]
        )[:10]
        titulo_primer_bloque = f"Municipios en nivel {nivel_min.replace('_',' ')} o superior hoy"
    else:
        munis_mostrar = [reglas_por_cve[c] for c in munis_pref if c in reglas_por_cve]
        munis_mostrar.sort(key=lambda x: -x["prob"])
        titulo_primer_bloque = "Tus municipios seleccionados"

    # Panorama general
    niveles_counts = {n: 0 for n in ["EXTREMO", "MUY_ALTO", "ALTO", "MEDIO", "BAJO"]}
    nombres_por_nivel = {n: [] for n in niveles_counts}
    for p in predicciones_reglas:
        n = p["nivel"]
        niveles_counts[n] += 1
        nombres_por_nivel[n].append(p.get("muni_nombre", p["cve_muni"]))

    total_alertas = sum(niveles_counts[n] for n in ["ALTO", "MUY_ALTO", "EXTREMO"])
    nivel_min = sub.get("nivel_minimo", "ALTO")
    alertas_personales = sum(1 for p in munis_mostrar if _nivel_ge(p["nivel"], nivel_min))

    # Asunto
    nombre_saludo = sub.get("nombre") or "Equipo operativo"
    if alertas_personales > 0:
        subject = f"[INCENDIOS NL] {alertas_personales} alerta(s) en tus municipios — {fecha_iso}"
    elif total_alertas > 0:
        subject = f"[INCENDIOS NL] Reporte diario — {total_alertas} municipios del estado en alerta"
    else:
        subject = f"[INCENDIOS NL] Reporte diario — sin alertas {fecha_iso}"

    # === HTML ===
    html_parts = [
        f"<body style='font-family:system-ui,-apple-system,Segoe UI,sans-serif;background:#f5f5f0;margin:0;padding:20px;color:#2c2c2a'>",
        f"<div style='max-width:640px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;border:1px solid #e0e0d8'>",
        f"<div style='background:#1A7A6E;color:#fff;padding:18px 24px'>"
        f"<div style='font-size:18px;font-weight:600'>Incendios NL — Reporte {fecha_iso}</div>"
        f"<div style='font-size:13px;opacity:0.85;margin-top:3px'>Prediccion diaria de riesgo para los 51 municipios de Nuevo Leon</div>"
        f"</div>",
        f"<div style='padding:20px 24px'>",
        f"<p style='margin:0 0 16px'>Hola {_html_escape(nombre_saludo)},</p>",
        f"<h2 style='font-size:15px;font-weight:600;color:#1A7A6E;margin:16px 0 6px;border-bottom:1px solid #e0e0d8;padding-bottom:6px'>{_html_escape(titulo_primer_bloque)}</h2>",
    ]

    if munis_mostrar:
        for p in munis_mostrar:
            h, _ = _bloque_muni(p, ml_por_cve)
            html_parts.append(h)
    else:
        html_parts.append("<p style='color:#666;font-size:13px'>Sin municipios con riesgo elevado en tu seleccion hoy.</p>")

    # Panorama
    html_parts.append(f"<h2 style='font-size:15px;font-weight:600;color:#1A7A6E;margin:24px 0 8px;border-bottom:1px solid #e0e0d8;padding-bottom:6px'>Panorama general del estado</h2>")
    for nivel, emoji, color in [("EXTREMO","🔴","#D90429"),("MUY_ALTO","🟠","#E8600A"),("ALTO","🟡","#E5A100"),("MEDIO","🔵","#2B9348")]:
        if niveles_counts[nivel]:
            lista = ", ".join(_html_escape(x) for x in nombres_por_nivel[nivel])
            html_parts.append(
                f"<div style='margin:6px 0;font-size:13px'>"
                f"<span style='color:{color};font-weight:600'>{emoji} {nivel.replace('_',' ')}</span> "
                f"({niveles_counts[nivel]}): {lista}</div>"
            )
    if not any(niveles_counts[n] for n in ["EXTREMO","MUY_ALTO","ALTO","MEDIO"]):
        html_parts.append("<p style='color:#1B4332;font-size:13px'>Todos los municipios del estado en nivel BAJO hoy.</p>")

    html_parts.extend([
        f"<p style='margin-top:20px;font-size:13px'>Ver dashboard completo: <a href='{DASHBOARD_URL}' style='color:#1A7A6E'>{DASHBOARD_URL}</a></p>",
        f"</div>",
        f"<div style='background:#fafaf5;padding:16px 24px;border-top:1px solid #e0e0d8;font-size:11px;color:#73726c;line-height:1.6'>",
        f"Recibes este correo porque te suscribiste en <a href='{DASHBOARD_URL}' style='color:#1A7A6E'>{DASHBOARD_URL}</a>. ",
        f"Puedes <a href='{DASHBOARD_URL}/preferencias.html' style='color:#1A7A6E'>cambiar tus preferencias</a> o ",
        f"<a href='{DASHBOARD_URL}/desuscribir.html?token={sub['unsubscribe_token']}' style='color:#1A7A6E'>darte de baja</a> con un solo click.<br/>",
        f"Sistema de Prediccion de Incendios — BIOIMPACT / SEMA NL",
        f"</div>",
        f"</div></body>",
    ])
    html = "".join(html_parts)

    # === Text ===
    text_parts = [
        f"INCENDIOS NL — Reporte {fecha_iso}",
        "=" * 50,
        f"Hola {nombre_saludo},",
        "",
        titulo_primer_bloque.upper(),
        "-" * 50,
    ]
    if munis_mostrar:
        for p in munis_mostrar:
            _, t = _bloque_muni(p, ml_por_cve)
            text_parts.append(t)
    else:
        text_parts.append("Sin municipios con riesgo elevado en tu seleccion hoy.\n")

    text_parts += [
        "",
        "PANORAMA GENERAL DEL ESTADO",
        "-" * 50,
    ]
    for nivel in ["EXTREMO", "MUY_ALTO", "ALTO", "MEDIO"]:
        if niveles_counts[nivel]:
            text_parts.append(f"  {nivel.replace('_',' ')} ({niveles_counts[nivel]}): {', '.join(nombres_por_nivel[nivel])}")
    text_parts += [
        "",
        f"Dashboard: {DASHBOARD_URL}",
        "",
        f"Baja: {DASHBOARD_URL}/desuscribir.html?token={sub['unsubscribe_token']}",
    ]
    text = "\n".join(text_parts)

    return subject, html, text, len(munis_mostrar), alertas_personales


def enviar_resumen_suscriptores(sb: "SupabaseClient", predicciones: list, predicciones_ml: list, fecha_iso: str) -> None:
    """Consulta suscriptores activos y envia correo personalizado via Mailjet."""
    if not all([MAILJET_API_KEY, MAILJET_API_SECRET, MAILJET_FROM_EMAIL]):
        log.info("Mailjet no configurado; se omite envio a suscriptores")
        return

    try:
        subs = sb.select("v_suscriptores_activos", {"select": "*"})
    except Exception as e:
        log.error(f"Error consultando suscriptores: {e}")
        return

    if not subs:
        log.info("Sin suscriptores activos")
        return

    log.info(f"Procesando {len(subs)} suscriptores...")
    enviados = 0
    omitidos = 0
    fallidos = 0

    for sub in subs:
        try:
            subject, html, text, n_munis, n_alertas = generar_email_suscriptor(
                sub, predicciones, predicciones_ml, fecha_iso
            )

            # Si cadencia es solo_alertas y no hay alertas personales relevantes, omitir
            if sub.get("cadencia") == "solo_alertas" and n_alertas == 0:
                omitidos += 1
                continue

            ok, err = enviar_email_mailjet(sub["email"], subject, html, text)
            status = "sent" if ok else "failed"
            if ok:
                enviados += 1
            else:
                fallidos += 1
                log.error(f"  Fallo envio a {sub['email']}: {err}")

            # Log en BD
            try:
                sb.insert("correos_enviados", [{
                    "suscriptor_id": sub["id"],
                    "email": sub["email"],
                    "fecha": fecha_iso,
                    "asunto": subject[:200],
                    "n_municipios": n_munis,
                    "n_alertas": n_alertas,
                    "status": status,
                    "error_msg": err[:500] if err else None,
                }])
            except Exception:
                pass
        except Exception as e:
            log.error(f"  Error procesando suscriptor {sub.get('email','?')}: {e}")
            fallidos += 1

    log.info(f"Suscriptores: {enviados} enviados, {omitidos} omitidos (solo_alertas sin alertas), {fallidos} fallidos")


# ─── Orquestador principal ──────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("ETL Predicción de Incendios — Nuevo León")
    log.info(f"Fecha: {date.today().isoformat()}")
    log.info("=" * 60)

    if not FIRMS_MAP_KEY:
        log.error("FIRMS_MAP_KEY no configurada.")
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL o SUPABASE_KEY no configuradas.")
        return

    sb = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    municipios_db = sb.select("municipios", {"select": "id,cve_muni,lat_centroide,lon_centroide,nombre,elevacion_media,pendiente_media"})
    if not municipios_db:
        log.error("No hay municipios en la BD. Ejecuta schema.sql primero.")
        return

    municipios_map = {m["cve_muni"]: m["id"] for m in municipios_db}
    municipios_info = {m["cve_muni"]: m for m in municipios_db}
    log.info(f"Municipios en BD: {len(municipios_map)}")

    # Paso 1
    hotspots_raw = fetch_firms_hotspots(day_range=2)

    # Paso 2
    meteo_data = fetch_open_meteo(municipios_db, days_back=7)

    # Paso 3: Geocodificación estricta con shapefile INEGI oficial
    try:
        municipios_geom = cargar_municipios_shapely()
    except Exception as e:
        log.error(f"No se pudo cargar shapefile municipal: {e}")
        return

    hotspots_geo = []
    if hotspots_raw:
        hotspots_geo = geocode_hotspots_shapely(hotspots_raw, municipios_geom)

    # Indexar hotspots por municipio, separando los de las últimas 24h
    # (el fetch cubre 48h para robustez; el feature sólo cuenta las últimas 24h)
    from datetime import timezone as _tz
    corte_24h = datetime.now(_tz.utc) - timedelta(hours=24)
    hotspots_por_muni = {}       # TODOS los hotspots (para upsert a BD)
    hotspots_24h_por_muni = {}   # SÓLO últimas 24h (para feature n_hotspots_24h)
    for h in hotspots_geo:
        cve = h["cve_muni"]
        hotspots_por_muni.setdefault(cve, []).append(h)
        try:
            ts = datetime.fromisoformat(h["detected_at"].replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz.utc)
            if ts >= corte_24h:
                hotspots_24h_por_muni.setdefault(cve, []).append(h)
        except Exception:
            pass  # si no parsea, no lo contamos en 24h

    # Paso 4: calcular dias_sin_lluvia y dias_sin_lluvia_30d desde BD
    # (usa historico; dsl es racha actual, dsl30 es conteo en ventana 30d)
    dias_sin_lluvia_db = {}
    dias_sin_lluvia_30d_db = {}
    for cve, info in municipios_info.items():
        mid = municipios_map.get(cve)
        if not mid:
            continue
        try:
            clima_hist = sb.select("clima_diario", {
                "select": "fecha,precipitacion",
                "municipio_id": f"eq.{mid}",
                "order": "fecha.desc",
                "limit": "60",
            })
            # Racha actual (cuenta hasta primera lluvia >=1mm)
            dias = 0
            for dia in clima_hist:
                precip = dia.get("precipitacion") or 0
                if precip < 1.0:
                    dias += 1
                else:
                    break
            dias_sin_lluvia_db[cve] = dias
            # Ventana 30d: total de dias secos entre los ultimos 30 registros
            ultimos_30 = clima_hist[:30]
            dsl30 = sum(1 for d in ultimos_30 if (d.get("precipitacion") or 0) < 1.0)
            dias_sin_lluvia_30d_db[cve] = dsl30
        except Exception:
            dias_sin_lluvia_db[cve] = 0
            dias_sin_lluvia_30d_db[cve] = 0

    # Cargar modelo ML
    modelo_ml = cargar_modelo_ml()

    predicciones = []       # Reglas v1 (condiciones climáticas)
    predicciones_ml = []    # ML v2

    for cve, info in municipios_info.items():
        clima_muni = meteo_data.get(cve, [])
        if not clima_muni:
            continue

        clima_hoy = clima_muni[-1]
        dsl = dias_sin_lluvia_db.get(cve, calcular_dias_sin_lluvia(clima_muni))
        dsl30 = dias_sin_lluvia_30d_db.get(cve, 0)
        hs_muni_24h = hotspots_24h_por_muni.get(cve, [])

        features = {
            "cve_muni": cve,
            "fecha": clima_hoy["fecha"],
            "temp_max": clima_hoy.get("temp_max"),
            "temp_min": clima_hoy.get("temp_min"),
            "humedad_min": clima_hoy.get("humedad_min"),
            "viento_max": clima_hoy.get("viento_max"),
            "precipitacion": clima_hoy.get("precipitacion"),
            "et0": clima_hoy.get("et0"),
            "dias_sin_lluvia": dsl,
            "dias_sin_lluvia_30d": dsl30,
            "n_hotspots_24h": len(hs_muni_24h),
            "frp_max": max((h["frp"] for h in hs_muni_24h), default=0),
            "elevacion_media": info.get("elevacion_media", 0),
            "pendiente_media": info.get("pendiente_media", 0),
        }

        # Factor antropogenico para la fecha predicha (se comparte entre modelos)
        pts_fa, etiquetas_fa = calcular_factor_antropogenico(
            cve, date.fromisoformat(clima_hoy["fecha"])
        )
        delta = pts_fa / 100.0

        # Prediccion por reglas (condiciones climaticas)
        prob_base_r, _ = calcular_riesgo(features)
        prob_ajust_r = min(prob_base_r + delta, 1.0)
        predicciones.append({
            "cve_muni": cve,
            "fecha": clima_hoy["fecha"],
            "prob": prob_ajust_r,
            "prob_base": prob_base_r,
            "nivel": _nivel_desde_prob(prob_ajust_r),
            "factor_pts": pts_fa,
            "factor_etiquetas": etiquetas_fa,
            "features": features,
            "muni_nombre": info.get("nombre", cve),
            "modelo_version": "rules_v1",
        })

        # Prediccion ML: la version viene del pkl (ml_v2 entrenado con ERA5 real)
        if modelo_ml:
            try:
                prob_base_ml, _ = predecir_ml(modelo_ml, features, info)
                prob_ajust_ml = min(prob_base_ml + delta, 1.0)
                predicciones_ml.append({
                    "cve_muni": cve,
                    "fecha": clima_hoy["fecha"],
                    "prob": prob_ajust_ml,
                    "prob_base": prob_base_ml,
                    "nivel": _nivel_desde_prob(prob_ajust_ml),
                    "factor_pts": pts_fa,
                    "factor_etiquetas": etiquetas_fa,
                    "features": features,
                    "muni_nombre": info.get("nombre", cve),
                    "modelo_version": modelo_ml.get("version", "ml_v2"),
                })
            except Exception as e:
                log.error(f"Error ML para {cve}: {e}")

    log.info(f"Predicciones reglas: {len(predicciones)}")
    por_nivel = {}
    for p in predicciones:
        por_nivel.setdefault(p["nivel"], []).append(p["muni_nombre"])
    for nivel, munis in sorted(por_nivel.items()):
        log.info(f"  {nivel}: {len(munis)} municipios — {', '.join(munis[:5])}")

    if predicciones_ml:
        log.info(f"Predicciones ML: {len(predicciones_ml)}")
        por_nivel_ml = {}
        for p in predicciones_ml:
            por_nivel_ml.setdefault(p["nivel"], []).append(p["muni_nombre"])
        for nivel, munis in sorted(por_nivel_ml.items()):
            log.info(f"  ML {nivel}: {len(munis)} municipios — {', '.join(munis[:5])}")

    # Paso 5: Upsert a Supabase
    if hotspots_geo:
        hs_rows = []
        for h in hotspots_geo:
            mid = municipios_map.get(h["cve_muni"])
            if mid:
                hs_rows.append({
                    "municipio_id": mid, "latitude": h["latitude"],
                    "longitude": h["longitude"], "brightness": h["brightness"],
                    "frp": h["frp"], "source": h["source"],
                    "detected_at": h["detected_at"], "satellite": h["satellite"],
                    "confidence": str(h.get("confidence", ""))[:10],
                })
        if hs_rows:
            try:
                for i in range(0, len(hs_rows), 50):
                    batch = hs_rows[i:i+50]
                    sb.upsert("hotspots", batch, on_conflict="latitude,longitude,detected_at,source")
                log.info(f"Upsert {len(hs_rows)} hotspots (sin duplicados)")
            except Exception as e:
                log.error(f"Error upsert hotspots: {e}")

    clima_rows = []
    for cve, dias in meteo_data.items():
        mid = municipios_map.get(cve)
        if not mid:
            continue
        for dia in dias:
            clima_rows.append({
                "municipio_id": mid, "fecha": dia["fecha"],
                "temp_max": dia["temp_max"], "temp_min": dia["temp_min"],
                "humedad_min": dia["humedad_min"], "viento_max": dia["viento_max"],
                "precipitacion": dia["precipitacion"], "et0": dia["et0"],
            })
    if clima_rows:
        try:
            for i in range(0, len(clima_rows), 100):
                batch = clima_rows[i:i+100]
                sb.upsert("clima_diario", batch, on_conflict="municipio_id,fecha")
            log.info(f"Upsert {len(clima_rows)} registros de clima")
        except Exception as e:
            log.error(f"Error upsert clima: {e}")

    # Actualizar dias_sin_lluvia en clima_diario para hoy
    for cve, dsl in dias_sin_lluvia_db.items():
        mid = municipios_map.get(cve)
        if not mid:
            continue
        try:
            sb.upsert("clima_diario", [{
                "municipio_id": mid,
                "fecha": date.today().isoformat(),
                "dias_sin_lluvia": dsl,
            }], on_conflict="municipio_id,fecha")
        except Exception:
            pass
    log.info(f"Dias sin lluvia actualizados para {len(dias_sin_lluvia_db)} municipios")

    # Upsert predicciones (reglas + ML)
    todas_predicciones = predicciones + predicciones_ml
    pred_rows = []
    for p in todas_predicciones:
        mid = municipios_map.get(p["cve_muni"])
        if mid:
            pred_rows.append({
                "municipio_id": mid, "fecha": p["fecha"],
                "prob_incendio": p["prob"],           # ajustada (con factor)
                "prob_base": p["prob_base"],          # sin factor
                "nivel_riesgo": p["nivel"],
                "factor_antropogenico_pts": p.get("factor_pts", 0),
                "factor_antropogenico_etiquetas": p.get("factor_etiquetas", []),
                "features_json": json.dumps(p["features"]),
                "modelo_version": p.get("modelo_version", "rules_v1"),
            })
    if pred_rows:
        try:
            sb.upsert("predicciones", pred_rows, on_conflict="municipio_id,fecha,modelo_version")
            log.info(f"Upsert {len(pred_rows)} predicciones ({len(predicciones)} reglas + {len(predicciones_ml)} ML)")
        except Exception as e:
            log.error(f"Error upsert predicciones: {e}")

    # Paso 6: Resumen diario por WhatsApp (se envía siempre, no solo con alertas)
    resumen = generar_resumen_diario(predicciones)
    resumen_enviado = False
    try:
        contactos_resumen = sb.select("contactos", {
            "select": "*",
            "activo": "eq.true",
            "canal_pref": "eq.whatsapp",
        })
    except Exception:
        contactos_resumen = []

    for c in contactos_resumen:
        tel = c.get("telefono")
        if tel:
            try:
                if enviar_whatsapp(tel, resumen):
                    resumen_enviado = True
                    try:
                        sb.insert("alertas_enviadas", [{
                            "contacto_id": c["id"],
                            "canal": "whatsapp",
                            "status": "sent",
                            "mensaje": resumen[:500],
                            "sent_at": datetime.now().isoformat(),
                        }])
                    except Exception:
                        pass
            except Exception as e:
                log.error(f"Error resumen WhatsApp a {c.get('nombre', tel)}: {e}")

    if resumen_enviado:
        log.info(f"Resumen diario WhatsApp enviado a {len(contactos_resumen)} contactos")
    elif contactos_resumen:
        log.warning("No se pudo enviar resumen WhatsApp")
    else:
        log.info("Sin contactos WhatsApp para resumen diario")

    # Paso 7: Alertas individuales (municipios con riesgo ALTO+)
    niveles_alerta = {"ALTO", "MUY_ALTO", "EXTREMO"}
    alertas_set = {}
    for p in todas_predicciones:
        if p["nivel"] in niveles_alerta:
            cve = p["cve_muni"]
            if cve not in alertas_set or p["prob"] > alertas_set[cve]["prob"]:
                alertas_set[cve] = p
    alertas = list(alertas_set.values())

    if alertas:
        log.info(f"🔥 {len(alertas)} municipios con alerta")
        for pred in alertas:
            mid = municipios_map.get(pred["cve_muni"])
            if not mid:
                continue
            try:
                contactos = sb.select("contactos", {
                    "select": "*",
                    "municipio_id": f"eq.{mid}",
                    "activo": "eq.true",
                })
            except Exception:
                contactos = []

            for c in contactos:
                msg = generar_mensaje(pred, c)
                canal = c.get("canal_pref", "email")
                status = "pending"
                try:
                    if canal == "email" and c.get("email"):
                        enviar_email(c["email"], msg, pred)
                        status = "sent"
                    elif canal == "whatsapp" and c.get("telefono"):
                        enviar_whatsapp(c["telefono"], msg)
                        status = "sent"
                    elif canal == "sms" and c.get("telefono"):
                        enviar_sms(c["telefono"], msg)
                        status = "sent"
                except Exception as e:
                    log.error(f"Error alerta a {c['nombre']}: {e}")
                    status = "error"

                try:
                    sb.insert("alertas_enviadas", [{
                        "contacto_id": c["id"],
                        "canal": canal,
                        "status": status,
                        "mensaje": msg[:500],
                        "sent_at": datetime.now().isoformat(),
                    }])
                except Exception:
                    pass
    else:
        log.info("Sin municipios en nivel de alerta")

    # Paso 8: Envio personalizado a suscriptores publicos via Mailjet
    try:
        enviar_resumen_suscriptores(sb, predicciones, predicciones_ml, date.today().isoformat())
    except Exception as e:
        log.error(f"Error enviando a suscriptores: {e}")

    log.info("✅ ETL completado")


if __name__ == "__main__":
    main()
