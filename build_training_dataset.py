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

# ─── Municipios de NL (del schema.sql) ─────────────────────────────
MUNICIPIOS = [
    {"cve":"001","nombre":"Abasolo","lat":25.9439,"lon":-100.4117},
    {"cve":"002","nombre":"Agualeguas","lat":26.3106,"lon":-99.5453},
    {"cve":"003","nombre":"Los Aldamas","lat":26.0614,"lon":-99.1328},
    {"cve":"004","nombre":"Allende","lat":25.2833,"lon":-100.0167},
    {"cve":"005","nombre":"Anáhuac","lat":27.2325,"lon":-100.1394},
    {"cve":"006","nombre":"Apodaca","lat":25.7814,"lon":-100.1883},
    {"cve":"007","nombre":"Aramberri","lat":24.1006,"lon":-99.8169},
    {"cve":"008","nombre":"Bustamante","lat":26.5325,"lon":-100.5142},
    {"cve":"009","nombre":"Cadereyta Jiménez","lat":25.5864,"lon":-99.9808},
    {"cve":"010","nombre":"El Carmen","lat":25.9667,"lon":-100.2500},
    {"cve":"011","nombre":"Cerralvo","lat":26.0833,"lon":-99.6167},
    {"cve":"012","nombre":"Ciénega de Flores","lat":25.9500,"lon":-100.1667},
    {"cve":"013","nombre":"China","lat":25.7000,"lon":-99.2333},
    {"cve":"014","nombre":"Doctor Arroyo","lat":23.6742,"lon":-100.1878},
    {"cve":"015","nombre":"Doctor Coss","lat":25.9500,"lon":-99.0167},
    {"cve":"016","nombre":"Doctor González","lat":25.8500,"lon":-100.0167},
    {"cve":"017","nombre":"Galeana","lat":24.8333,"lon":-100.0833},
    {"cve":"018","nombre":"García","lat":25.8167,"lon":-100.5833},
    {"cve":"019","nombre":"San Pedro Garza García","lat":25.6500,"lon":-100.4000},
    {"cve":"020","nombre":"General Bravo","lat":25.8000,"lon":-99.1667},
    {"cve":"021","nombre":"General Escobedo","lat":25.7833,"lon":-100.3167},
    {"cve":"022","nombre":"General Terán","lat":25.2500,"lon":-99.6667},
    {"cve":"023","nombre":"General Treviño","lat":26.2333,"lon":-99.2667},
    {"cve":"024","nombre":"General Zaragoza","lat":23.9833,"lon":-99.7833},
    {"cve":"025","nombre":"General Zuazua","lat":25.9500,"lon":-100.0833},
    {"cve":"026","nombre":"Guadalupe","lat":25.6833,"lon":-100.2500},
    {"cve":"027","nombre":"Los Herreras","lat":25.9167,"lon":-99.4167},
    {"cve":"028","nombre":"Higueras","lat":25.9500,"lon":-100.0167},
    {"cve":"029","nombre":"Hualahuises","lat":25.0667,"lon":-99.6667},
    {"cve":"030","nombre":"Iturbide","lat":24.7333,"lon":-99.9000},
    {"cve":"031","nombre":"Juárez","lat":25.6500,"lon":-100.0833},
    {"cve":"032","nombre":"Lampazos de Naranjo","lat":27.0333,"lon":-100.5167},
    {"cve":"033","nombre":"Linares","lat":24.8597,"lon":-99.5675},
    {"cve":"034","nombre":"Marín","lat":25.8833,"lon":-100.0333},
    {"cve":"035","nombre":"Melchor Ocampo","lat":26.4500,"lon":-99.4167},
    {"cve":"036","nombre":"Mier y Noriega","lat":23.5500,"lon":-100.2000},
    {"cve":"037","nombre":"Mina","lat":26.0167,"lon":-100.5833},
    {"cve":"038","nombre":"Montemorelos","lat":25.1833,"lon":-99.8333},
    {"cve":"039","nombre":"Monterrey","lat":25.6714,"lon":-100.3089},
    {"cve":"040","nombre":"Parás","lat":26.5000,"lon":-99.4333},
    {"cve":"041","nombre":"Pesquería","lat":25.7833,"lon":-100.0500},
    {"cve":"042","nombre":"Los Ramones","lat":25.7000,"lon":-99.6333},
    {"cve":"043","nombre":"Rayones","lat":25.0167,"lon":-100.0833},
    {"cve":"044","nombre":"Sabinas Hidalgo","lat":26.5083,"lon":-100.1778},
    {"cve":"045","nombre":"Salinas Victoria","lat":25.9667,"lon":-100.2833},
    {"cve":"046","nombre":"San Nicolás de los Garza","lat":25.7500,"lon":-100.2833},
    {"cve":"047","nombre":"Hidalgo","lat":25.9833,"lon":-100.4500},
    {"cve":"048","nombre":"Santa Catarina","lat":25.6833,"lon":-100.4500},
    {"cve":"049","nombre":"Santiago","lat":25.4167,"lon":-100.1500},
    {"cve":"050","nombre":"Vallecillo","lat":26.6667,"lon":-99.9833},
    {"cve":"051","nombre":"Villaldama","lat":26.5000,"lon":-100.4167},
]

METEO_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]


def load_conafor_fires(csv_path: str) -> dict:
    """Carga incendios NL y retorna dict {(cve_mun_3dig, fecha_inicio): info}."""
    fires = {}
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
            fires[key] = {
                "causa": row.get("Causa", ""),
                "tipo_vegetacion": row.get("Tipo_Vegetacion", ""),
                "hectareas": ha,
                "duracion": row.get("Duracion_dias", ""),
            }
    log.info(f"CONAFOR: {len(fires)} incendios únicos en NL")
    return fires


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
                    daily = muni_data.get("daily", {})
                    times = daily.get("time", [])

                    muni_climate = []
                    for k, fecha_str in enumerate(times):
                        row = {"fecha": fecha_str}
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

    # 1. Cargar incendios
    fires = load_conafor_fires(csv_path)

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

    for cve, climate_rows in all_climate.items():
        muni = muni_lookup.get(cve, {})

        # Calcular días sin lluvia acumulados
        dias_sin_lluvia = 0

        for row in sorted(climate_rows, key=lambda x: x["fecha"]):
            fecha = row["fecha"]
            precip = row.get("precipitation_sum") or 0

            if precip < 1.0:
                dias_sin_lluvia += 1
            else:
                dias_sin_lluvia = 0

            # ¿Hubo incendio?
            fire_key = (cve, fecha)
            hubo_incendio = 1 if fire_key in fires else 0
            fire_info = fires.get(fire_key, {})

            # Parsear fecha
            parts = fecha.split("-")
            mes = int(parts[1])
            dia_semana = pd.Timestamp(fecha).dayofweek
            dia_del_ano = pd.Timestamp(fecha).dayofyear

            records.append({
                "cve_muni": cve,
                "municipio": muni.get("nombre", ""),
                "fecha": fecha,
                "lat": muni.get("lat", 0),
                "lon": muni.get("lon", 0),
                "temp_max": row.get("temperature_2m_max"),
                "temp_min": row.get("temperature_2m_min"),
                "humedad_min": row.get("relative_humidity_2m_min"),
                "viento_max": row.get("wind_speed_10m_max"),
                "precipitacion": row.get("precipitation_sum"),
                "et0": row.get("et0_fao_evapotranspiration"),
                "dias_sin_lluvia": dias_sin_lluvia,
                "mes": mes,
                "dia_semana": dia_semana,
                "dia_del_ano": dia_del_ano,
                "hubo_incendio": hubo_incendio,
                "hectareas": fire_info.get("hectareas", 0),
                "causa": fire_info.get("causa", ""),
            })

    df = pd.DataFrame(records)

    # Stats
    log.info(f"Dataset: {len(df)} filas, {df['hubo_incendio'].sum()} incendios ({df['hubo_incendio'].mean()*100:.2f}%)")
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
