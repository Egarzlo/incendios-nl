"""
Construye el dataset de entrenamiento para el modelo ML de incendios.
1. Lee CSV CONAFOR → incendios NL por municipio-día
2. Genera grid completo: 51 municipios × todos los días 2015-2024
3. Consulta clima histórico de Open-Meteo Archive API (ERA5)
4. Calcula features derivados (días sin lluvia, mes, etc.)
5. Etiqueta: hubo_incendio (0/1)
"""

import csv
import json
import time
import logging
import requests
import pandas as pd
from datetime import date, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Municipios NL con centroides oficiales INEGI 2025.1 + ecorregion ─────
# Ecorregion Vocacion Ambiental A.C.: R1 llanos, R2 desierto, R3 piedemonte/citrica,
# R4 sierra Madre Oriental, R5 altiplano semiarido.
MUNICIPIOS = [
    {"cve":"001","nombre":"Abasolo","lat":25.940543,"lon":-100.405942,"ecoregion":1},
    {"cve":"002","nombre":"Agualeguas","lat":26.298712,"lon":-99.703083,"ecoregion":1},
    {"cve":"003","nombre":"Los Aldamas","lat":26.091506,"lon":-99.273480,"ecoregion":1},
    {"cve":"004","nombre":"Allende","lat":25.301434,"lon":-100.029518,"ecoregion":3},
    {"cve":"005","nombre":"Anáhuac","lat":27.342154,"lon":-100.025355,"ecoregion":1},
    {"cve":"006","nombre":"Apodaca","lat":25.792542,"lon":-100.187381,"ecoregion":1},
    {"cve":"007","nombre":"Aramberri","lat":24.225118,"lon":-99.886509,"ecoregion":5},
    {"cve":"008","nombre":"Bustamante","lat":26.571711,"lon":-100.561792,"ecoregion":1},
    {"cve":"009","nombre":"Cadereyta Jiménez","lat":25.524591,"lon":-99.914185,"ecoregion":3},
    {"cve":"010","nombre":"El Carmen","lat":25.900771,"lon":-100.356885,"ecoregion":1},
    {"cve":"011","nombre":"Cerralvo","lat":26.072318,"lon":-99.705374,"ecoregion":1},
    {"cve":"012","nombre":"Ciénega de Flores","lat":25.977458,"lon":-100.185436,"ecoregion":1},
    {"cve":"013","nombre":"China","lat":25.480159,"lon":-98.972409,"ecoregion":1},
    {"cve":"014","nombre":"Doctor Arroyo","lat":23.860108,"lon":-100.306266,"ecoregion":5},
    {"cve":"015","nombre":"Doctor Coss","lat":25.964091,"lon":-99.030873,"ecoregion":1},
    {"cve":"016","nombre":"Doctor González","lat":25.849252,"lon":-99.804980,"ecoregion":1},
    {"cve":"017","nombre":"Galeana","lat":24.760509,"lon":-100.392287,"ecoregion":4},
    {"cve":"018","nombre":"García","lat":25.809011,"lon":-100.659777,"ecoregion":1},
    {"cve":"019","nombre":"San Pedro Garza García","lat":25.644597,"lon":-100.374758,"ecoregion":3},
    {"cve":"020","nombre":"General Bravo","lat":25.803315,"lon":-98.848418,"ecoregion":1},
    {"cve":"021","nombre":"General Escobedo","lat":25.821867,"lon":-100.355575,"ecoregion":1},
    {"cve":"022","nombre":"General Terán","lat":25.275874,"lon":-99.413017,"ecoregion":3},
    {"cve":"023","nombre":"General Treviño","lat":26.212648,"lon":-99.445967,"ecoregion":1},
    {"cve":"024","nombre":"General Zaragoza","lat":23.901029,"lon":-99.740174,"ecoregion":5},
    {"cve":"025","nombre":"General Zuazua","lat":25.911392,"lon":-100.134820,"ecoregion":1},
    {"cve":"026","nombre":"Guadalupe","lat":25.672760,"lon":-100.205599,"ecoregion":1},
    {"cve":"027","nombre":"Los Herreras","lat":25.916079,"lon":-99.414240,"ecoregion":1},
    {"cve":"028","nombre":"Higueras","lat":26.033168,"lon":-99.997508,"ecoregion":1},
    {"cve":"029","nombre":"Hualahuises","lat":24.883791,"lon":-99.678096,"ecoregion":3},
    {"cve":"030","nombre":"Iturbide","lat":24.638418,"lon":-99.848716,"ecoregion":4},
    {"cve":"031","nombre":"Juárez","lat":25.614087,"lon":-100.121406,"ecoregion":1},
    {"cve":"032","nombre":"Lampazos de Naranjo","lat":27.050841,"lon":-100.418216,"ecoregion":1},
    {"cve":"033","nombre":"Linares","lat":24.851771,"lon":-99.529220,"ecoregion":3},
    {"cve":"034","nombre":"Marín","lat":25.886143,"lon":-100.023315,"ecoregion":1},
    {"cve":"035","nombre":"Melchor Ocampo","lat":26.048931,"lon":-99.493516,"ecoregion":1},
    {"cve":"036","nombre":"Mier y Noriega","lat":23.417917,"lon":-100.160408,"ecoregion":5},
    {"cve":"037","nombre":"Mina","lat":26.285584,"lon":-100.786277,"ecoregion":2},
    {"cve":"038","nombre":"Montemorelos","lat":25.126305,"lon":-99.808381,"ecoregion":3},
    {"cve":"039","nombre":"Monterrey","lat":25.644640,"lon":-100.310952,"ecoregion":3},
    {"cve":"040","nombre":"Parás","lat":26.583042,"lon":-99.601554,"ecoregion":1},
    {"cve":"041","nombre":"Pesquería","lat":25.736769,"lon":-99.977135,"ecoregion":1},
    {"cve":"042","nombre":"Los Ramones","lat":25.653310,"lon":-99.587667,"ecoregion":1},
    {"cve":"043","nombre":"Rayones","lat":25.065823,"lon":-100.127733,"ecoregion":4},
    {"cve":"044","nombre":"Sabinas Hidalgo","lat":26.575102,"lon":-100.149288,"ecoregion":1},
    {"cve":"045","nombre":"Salinas Victoria","lat":26.161052,"lon":-100.270385,"ecoregion":1},
    {"cve":"046","nombre":"San Nicolás de los Garza","lat":25.736076,"lon":-100.270693,"ecoregion":1},
    {"cve":"047","nombre":"Hidalgo","lat":25.999402,"lon":-100.453190,"ecoregion":1},
    {"cve":"048","nombre":"Santa Catarina","lat":25.574610,"lon":-100.483863,"ecoregion":3},
    {"cve":"049","nombre":"Santiago","lat":25.384897,"lon":-100.237323,"ecoregion":3},
    {"cve":"050","nombre":"Vallecillo","lat":26.648106,"lon":-99.884763,"ecoregion":1},
    {"cve":"051","nombre":"Villaldama","lat":26.469705,"lon":-100.347070,"ecoregion":1},
]

METEO_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]


def load_conafor_fires(csv_path: str, ventana_dias: int = 3) -> tuple[dict, dict]:
    """
    Carga incendios NL y retorna:
      - fires_exact: {(cve, fecha_inicio): info}  — label estricto para hubo_incendio
      - fires_ventana: {(cve, fecha): 1}  — label expandido ±N dias para hubo_incendio_ventana
    El label expandido ayuda al modelo a aprender condiciones precursoras/post:
    las condiciones meteorologicas no cambian abruptamente en un dia, y un
    incendio real suele durar varios dias.
    """
    fires_exact = {}
    fires_ventana = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("CVE_ENT") != "19":
                continue
            cve = str(row.get("CVE_MUN", "")).zfill(3)
            fecha = row.get("Fecha_Inicio", "")
            if not fecha or not cve:
                continue
            key = (cve, fecha)
            ha = float(row.get("Total_hectareas", "0").replace(",", "") or "0")
            fires_exact[key] = {
                "causa": row.get("Causa", ""),
                "tipo_vegetacion": row.get("Tipo_Vegetacion", ""),
                "hectareas": ha,
                "duracion": row.get("Duracion_dias", ""),
            }
            # Expansion ventana [-N, +N] (mas duracion registrada si aplica)
            try:
                d0 = date.fromisoformat(fecha)
                try:
                    dur = int(float(row.get("Duracion_dias", "0") or "0"))
                except Exception:
                    dur = 0
                for delta in range(-ventana_dias, ventana_dias + max(dur, 0) + 1):
                    d = d0 + timedelta(days=delta)
                    fires_ventana[(cve, d.isoformat())] = 1
            except Exception:
                continue
    log.info(
        f"CONAFOR: {len(fires_exact)} incendios unicos en NL "
        f"(label ventana ±{ventana_dias}d + duracion: {len(fires_ventana)} muni-dias positivos)"
    )
    return fires_exact, fires_ventana


def fetch_climate_archive(municipios, start_date, end_date, batch_size=15):
    """
    Consulta Open-Meteo Archive API (ERA5) para clima histórico.
    Retorna dict {cve: [{fecha, temp_max, ...}, ...]}
    """
    results = {}
    total_batches = (len(municipios) + batch_size - 1) // batch_size

    for i in range(0, len(municipios), batch_size):
        batch = municipios[i:i + batch_size]
        batch_num = i // batch_size + 1
        lats = ",".join(str(m["lat"]) for m in batch)
        lons = ",".join(str(m["lon"]) for m in batch)

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lats,
            "longitude": lons,
            "daily": ",".join(METEO_VARS),
            "start_date": start_date,
            "end_date": end_date,
            "timezone": "America/Monterrey",
        }

        retries = 3
        for attempt in range(retries):
            try:
                resp = requests.get(url, params=params, timeout=120)
                if resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    log.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, dict) and "daily" in data:
                    data = [data]

                for j, muni_data in enumerate(data):
                    if j >= len(batch):
                        break
                    cve = batch[j]["cve"]
                    # Elevation viene en metadata top-level del response
                    elev = muni_data.get("elevation")
                    daily = muni_data.get("daily", {})
                    times = daily.get("time", [])

                    muni_climate = []
                    for k, fecha_str in enumerate(times):
                        row = {"fecha": fecha_str, "_elevation": elev}
                        for var in METEO_VARS:
                            vals = daily.get(var, [])
                            try:
                                v = vals[k]
                                row[var] = float(v) if v is not None else None
                            except (IndexError, TypeError, ValueError):
                                row[var] = None
                        muni_climate.append(row)

                    results[cve] = muni_climate

                log.info(f"  Batch {batch_num}/{total_batches}: {len(batch)} municipios OK ({start_date} to {end_date})")
                break

            except Exception as e:
                log.error(f"  Batch {batch_num} attempt {attempt+1} error: {e}")
                if attempt < retries - 1:
                    time.sleep(10 * (attempt + 1))

        # Pause between batches
        if i + batch_size < len(municipios):
            time.sleep(1.5)

    return results


def build_dataset(csv_path: str, output_path: str):
    """Construye el dataset completo."""

    # 1. Cargar incendios (label estricto + ventana ±3d)
    fires_exact, fires_ventana = load_conafor_fires(csv_path, ventana_dias=3)

    # 2. Fetch clima histórico por año (para no saturar la API)
    all_climate = defaultdict(list)  # cve -> [rows...]

    year_ranges = [
        ("2015-01-01", "2017-12-31"),
        ("2018-01-01", "2020-12-31"),
        ("2021-01-01", "2024-12-31"),
    ]

    for start, end in year_ranges:
        log.info(f"Fetching clima {start} → {end}...")
        chunk = fetch_climate_archive(MUNICIPIOS, start, end)
        for cve, rows in chunk.items():
            all_climate[cve].extend(rows)
        log.info(f"  Acumulado: {sum(len(v) for v in all_climate.values())} registros clima")
        time.sleep(3)  # Pausa entre rangos

    # 3. Construir DataFrame
    log.info("Construyendo DataFrame...")
    records = []

    muni_lookup = {m["cve"]: m for m in MUNICIPIOS}

    from collections import deque

    for cve, climate_rows in all_climate.items():
        muni = muni_lookup.get(cve, {})
        climate_sorted = sorted(climate_rows, key=lambda x: x["fecha"])

        # Contadores acumulativos por municipio
        dias_sin_lluvia = 0
        ventana_30 = deque(maxlen=30)  # ultimos 30 dias: 1 si seco, 0 si mojado

        for row in climate_sorted:
            fecha = row["fecha"]
            precip = row.get("precipitation_sum") or 0
            seco = precip < 1.0

            if seco:
                dias_sin_lluvia += 1
            else:
                dias_sin_lluvia = 0

            ventana_30.append(1 if seco else 0)
            dias_sin_lluvia_30d = sum(ventana_30)  # 0..30

            # Labels: estricto (Fecha_Inicio) + ventana (±3d + duracion)
            fire_key = (cve, fecha)
            hubo_incendio = 1 if fire_key in fires_exact else 0
            hubo_incendio_ventana = 1 if fire_key in fires_ventana else 0
            fire_info = fires_exact.get(fire_key, {})

            dt = pd.Timestamp(fecha)
            mes = dt.month
            dia_semana = dt.dayofweek
            dia_del_ano = dt.dayofyear

            records.append({
                "cve_muni": cve,
                "municipio": muni.get("nombre", ""),
                "fecha": fecha,
                "lat": muni.get("lat", 0),
                "lon": muni.get("lon", 0),
                "elevacion": row.get("_elevation") or 0,
                "ecoregion": muni.get("ecoregion", 1),
                "temp_max": row.get("temperature_2m_max"),
                "temp_min": row.get("temperature_2m_min"),
                "humedad_min": row.get("relative_humidity_2m_min"),
                "viento_max": row.get("wind_speed_10m_max"),
                "precipitacion": row.get("precipitation_sum"),
                "et0": row.get("et0_fao_evapotranspiration"),
                "dias_sin_lluvia": dias_sin_lluvia,
                "dias_sin_lluvia_30d": dias_sin_lluvia_30d,
                "mes": mes,
                "dia_semana": dia_semana,
                "dia_del_ano": dia_del_ano,
                "hubo_incendio": hubo_incendio,
                "hubo_incendio_ventana": hubo_incendio_ventana,
                "hectareas": fire_info.get("hectareas", 0),
                "causa": fire_info.get("causa", ""),
            })

    df = pd.DataFrame(records)

    # Stats
    log.info(
        f"Dataset: {len(df)} filas, "
        f"{df['hubo_incendio'].sum()} incendios exactos ({df['hubo_incendio'].mean()*100:.3f}%), "
        f"{df['hubo_incendio_ventana'].sum()} muni-dias en ventana ({df['hubo_incendio_ventana'].mean()*100:.3f}%)"
    )
    log.info(f"Municipios: {df['cve_muni'].nunique()}, Fechas: {df['fecha'].min()} → {df['fecha'].max()}")

    df.to_csv(output_path, index=False)
    log.info(f"Guardado: {output_path}")

    return df


if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Determinar rutas relativas al directorio del script
    script_dir = Path(__file__).parent

    # CSV de CONAFOR: buscar en data/ o en el directorio actual
    csv_candidates = [
        script_dir / "data" / "estadisticasincendiosforestales2015-2024.csv",
        script_dir / "estadisticasincendiosforestales2015-2024.csv",
        Path("estadisticasincendiosforestales2015-2024.csv"),
    ]
    csv_path = None
    for candidate in csv_candidates:
        if candidate.exists():
            csv_path = str(candidate)
            break

    if csv_path is None:
        print("ERROR: No se encontró el CSV de CONAFOR.")
        print("Descárgalo de: https://www.datos.gob.mx/dataset/incendios_forestales")
        print(f"Y colócalo en: {script_dir / 'data' / 'estadisticasincendiosforestales2015-2024.csv'}")
        sys.exit(1)

    output_path = str(script_dir / "data" / "training_dataset_incendios_nl.csv")

    print(f"CSV CONAFOR:  {csv_path}")
    print(f"Output:       {output_path}")

    df = build_dataset(csv_path=csv_path, output_path=output_path)
    print(df.describe())
