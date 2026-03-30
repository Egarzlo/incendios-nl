"""
Dataset de entrenamiento con clima sintético realista para NL.
Usa patrones climatológicos conocidos de Nuevo León + incendios reales de CONAFOR.

NOTA: Este dataset sirve para validar el pipeline completo.
Para producción, reentrenar con datos reales de Open-Meteo Archive (ver build_training_dataset.py).
"""

import csv
import math
import random
import logging
import numpy as np
import pandas as pd
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

random.seed(42)
np.random.seed(42)

# ─── Municipios NL ─────────────────────────────────────────────────
MUNICIPIOS = [
    {"cve":"001","nombre":"Abasolo","lat":25.9439,"lon":-100.4117,"elev":450,"zona":"llanura"},
    {"cve":"002","nombre":"Agualeguas","lat":26.3106,"lon":-99.5453,"elev":200,"zona":"llanura"},
    {"cve":"003","nombre":"Los Aldamas","lat":26.0614,"lon":-99.1328,"elev":150,"zona":"llanura"},
    {"cve":"004","nombre":"Allende","lat":25.2833,"lon":-100.0167,"elev":480,"zona":"piedemonte"},
    {"cve":"005","nombre":"Anáhuac","lat":27.2325,"lon":-100.1394,"elev":180,"zona":"llanura"},
    {"cve":"006","nombre":"Apodaca","lat":25.7814,"lon":-100.1883,"elev":400,"zona":"urbana"},
    {"cve":"007","nombre":"Aramberri","lat":24.1006,"lon":-99.8169,"elev":1400,"zona":"sierra"},
    {"cve":"008","nombre":"Bustamante","lat":26.5325,"lon":-100.5142,"elev":500,"zona":"llanura"},
    {"cve":"009","nombre":"Cadereyta Jiménez","lat":25.5864,"lon":-99.9808,"elev":370,"zona":"piedemonte"},
    {"cve":"010","nombre":"El Carmen","lat":25.9667,"lon":-100.2500,"elev":420,"zona":"llanura"},
    {"cve":"011","nombre":"Cerralvo","lat":26.0833,"lon":-99.6167,"elev":220,"zona":"llanura"},
    {"cve":"012","nombre":"Ciénega de Flores","lat":25.9500,"lon":-100.1667,"elev":410,"zona":"llanura"},
    {"cve":"013","nombre":"China","lat":25.7000,"lon":-99.2333,"elev":200,"zona":"llanura"},
    {"cve":"014","nombre":"Doctor Arroyo","lat":23.6742,"lon":-100.1878,"elev":1800,"zona":"sierra"},
    {"cve":"015","nombre":"Doctor Coss","lat":25.9500,"lon":-99.0167,"elev":150,"zona":"llanura"},
    {"cve":"016","nombre":"Doctor González","lat":25.8500,"lon":-100.0167,"elev":380,"zona":"llanura"},
    {"cve":"017","nombre":"Galeana","lat":24.8333,"lon":-100.0833,"elev":1600,"zona":"sierra"},
    {"cve":"018","nombre":"García","lat":25.8167,"lon":-100.5833,"elev":700,"zona":"piedemonte"},
    {"cve":"019","nombre":"San Pedro Garza García","lat":25.6500,"lon":-100.4000,"elev":600,"zona":"urbana"},
    {"cve":"020","nombre":"General Bravo","lat":25.8000,"lon":-99.1667,"elev":180,"zona":"llanura"},
    {"cve":"021","nombre":"General Escobedo","lat":25.7833,"lon":-100.3167,"elev":500,"zona":"urbana"},
    {"cve":"022","nombre":"General Terán","lat":25.2500,"lon":-99.6667,"elev":300,"zona":"piedemonte"},
    {"cve":"023","nombre":"General Treviño","lat":26.2333,"lon":-99.2667,"elev":180,"zona":"llanura"},
    {"cve":"024","nombre":"General Zaragoza","lat":23.9833,"lon":-99.7833,"elev":1500,"zona":"sierra"},
    {"cve":"025","nombre":"General Zuazua","lat":25.9500,"lon":-100.0833,"elev":400,"zona":"llanura"},
    {"cve":"026","nombre":"Guadalupe","lat":25.6833,"lon":-100.2500,"elev":500,"zona":"urbana"},
    {"cve":"027","nombre":"Los Herreras","lat":25.9167,"lon":-99.4167,"elev":200,"zona":"llanura"},
    {"cve":"028","nombre":"Higueras","lat":25.9500,"lon":-100.0167,"elev":380,"zona":"llanura"},
    {"cve":"029","nombre":"Hualahuises","lat":25.0667,"lon":-99.6667,"elev":420,"zona":"piedemonte"},
    {"cve":"030","nombre":"Iturbide","lat":24.7333,"lon":-99.9000,"elev":1200,"zona":"sierra"},
    {"cve":"031","nombre":"Juárez","lat":25.6500,"lon":-100.0833,"elev":400,"zona":"urbana"},
    {"cve":"032","nombre":"Lampazos de Naranjo","lat":27.0333,"lon":-100.5167,"elev":350,"zona":"llanura"},
    {"cve":"033","nombre":"Linares","lat":24.8597,"lon":-99.5675,"elev":350,"zona":"piedemonte"},
    {"cve":"034","nombre":"Marín","lat":25.8833,"lon":-100.0333,"elev":400,"zona":"llanura"},
    {"cve":"035","nombre":"Melchor Ocampo","lat":26.4500,"lon":-99.4167,"elev":180,"zona":"llanura"},
    {"cve":"036","nombre":"Mier y Noriega","lat":23.5500,"lon":-100.2000,"elev":1900,"zona":"sierra"},
    {"cve":"037","nombre":"Mina","lat":26.0167,"lon":-100.5833,"elev":600,"zona":"piedemonte"},
    {"cve":"038","nombre":"Montemorelos","lat":25.1833,"lon":-99.8333,"elev":430,"zona":"piedemonte"},
    {"cve":"039","nombre":"Monterrey","lat":25.6714,"lon":-100.3089,"elev":540,"zona":"urbana"},
    {"cve":"040","nombre":"Parás","lat":26.5000,"lon":-99.4333,"elev":170,"zona":"llanura"},
    {"cve":"041","nombre":"Pesquería","lat":25.7833,"lon":-100.0500,"elev":380,"zona":"llanura"},
    {"cve":"042","nombre":"Los Ramones","lat":25.7000,"lon":-99.6333,"elev":250,"zona":"llanura"},
    {"cve":"043","nombre":"Rayones","lat":25.0167,"lon":-100.0833,"elev":900,"zona":"sierra"},
    {"cve":"044","nombre":"Sabinas Hidalgo","lat":26.5083,"lon":-100.1778,"elev":350,"zona":"llanura"},
    {"cve":"045","nombre":"Salinas Victoria","lat":25.9667,"lon":-100.2833,"elev":430,"zona":"llanura"},
    {"cve":"046","nombre":"San Nicolás de los Garza","lat":25.7500,"lon":-100.2833,"elev":500,"zona":"urbana"},
    {"cve":"047","nombre":"Hidalgo","lat":25.9833,"lon":-100.4500,"elev":550,"zona":"piedemonte"},
    {"cve":"048","nombre":"Santa Catarina","lat":25.6833,"lon":-100.4500,"elev":700,"zona":"piedemonte"},
    {"cve":"049","nombre":"Santiago","lat":25.4167,"lon":-100.1500,"elev":800,"zona":"sierra"},
    {"cve":"050","nombre":"Vallecillo","lat":26.6667,"lon":-99.9833,"elev":250,"zona":"llanura"},
    {"cve":"051","nombre":"Villaldama","lat":26.5000,"lon":-100.4167,"elev":450,"zona":"llanura"},
]


def generate_climate_for_municipality(muni: dict, start_date: date, end_date: date) -> list[dict]:
    """
    Genera clima sintético realista para un municipio de NL.
    Basado en climatología de Nuevo León:
    - Temp media anual: 22°C (llanura), 18°C (sierra)
    - Temporada lluviosa: junio-octubre (~80% de la precip anual)
    - Precip anual: 400-800mm dependiendo de zona
    - Vientos más fuertes en marzo-mayo
    - Humedad más baja en marzo-mayo (temporada de incendios)
    """
    lat = muni["lat"]
    elev = muni["elev"]
    zona = muni["zona"]

    # Ajustes por zona
    if zona == "sierra":
        temp_base, temp_amp = 18, 8
        precip_annual = 600
        hum_base = 55
    elif zona == "piedemonte":
        temp_base, temp_amp = 22, 10
        precip_annual = 550
        hum_base = 50
    elif zona == "urbana":
        temp_base, temp_amp = 23, 10
        precip_annual = 500
        hum_base = 48
    else:  # llanura
        temp_base, temp_amp = 24, 11
        precip_annual = 450
        hum_base = 45

    # Ajuste por elevación (-6.5°C por km)
    temp_base -= (elev - 500) * 0.0065

    records = []
    current = start_date
    dias_sin_lluvia = 0

    while current <= end_date:
        doy = current.timetuple().tm_yday
        # Fase estacional (0 = 1 ene, pi = 1 jul)
        phase = 2 * math.pi * (doy - 15) / 365.0

        # Temperatura max/min con ciclo anual + ruido
        temp_max = temp_base + temp_amp * math.sin(phase) + np.random.normal(0, 2.5)
        temp_min = temp_max - 8 - abs(np.random.normal(0, 2))

        # Precipitación: estacional con más lluvia jun-oct
        rain_phase = 2 * math.pi * (doy - 60) / 365.0
        rain_prob_base = 0.08 + 0.30 * max(0, math.sin(rain_phase))
        # Más lluvia en sep
        if 240 < doy < 290:
            rain_prob_base += 0.15
        # Variación interanual
        year_factor = 0.8 + 0.4 * random.random()

        if random.random() < rain_prob_base * year_factor:
            # Distribución exponencial para cantidades de lluvia
            precip = np.random.exponential(8) * year_factor
            precip = max(0.1, min(precip, 120))
        else:
            precip = 0.0

        if precip < 1.0:
            dias_sin_lluvia += 1
        else:
            dias_sin_lluvia = 0

        # Humedad mínima: inversamente correlacionada con temp y sequía
        hum_season = hum_base - 15 * math.sin(phase - 0.5) + np.random.normal(0, 8)
        if dias_sin_lluvia > 7:
            hum_season -= min(dias_sin_lluvia * 0.8, 15)
        if precip > 5:
            hum_season += 15
        hum_min = max(8, min(95, hum_season))

        # Viento max: más fuerte en primavera (mar-may)
        wind_base = 15 + 10 * max(0, math.sin(2 * math.pi * (doy - 45) / 365.0))
        viento_max = max(2, wind_base + np.random.normal(0, 6))

        # ET0 (evapotranspiración)
        et0 = max(0, 2 + 4 * math.sin(phase) + np.random.normal(0, 0.8))

        records.append({
            "fecha": current.isoformat(),
            "temp_max": round(temp_max, 1),
            "temp_min": round(temp_min, 1),
            "humedad_min": round(hum_min, 1),
            "viento_max": round(viento_max, 1),
            "precipitacion": round(precip, 2),
            "et0": round(et0, 2),
            "dias_sin_lluvia": dias_sin_lluvia,
        })
        current += timedelta(days=1)

    return records


def load_conafor_fires(csv_path: str) -> dict:
    """Carga incendios NL: {(cve_mun_3dig, fecha): info}."""
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
            ha = float(row.get("Total_hectareas", "0").replace(",", "") or "0")
            fires[(cve, fecha)] = {
                "causa": row.get("Causa", ""),
                "tipo_vegetacion": row.get("Tipo_Vegetacion", ""),
                "hectareas": ha,
            }
    log.info(f"CONAFOR: {len(fires)} incendios únicos en NL")
    return fires


def build_dataset(csv_path: str, output_path: str) -> pd.DataFrame:
    start = date(2015, 1, 1)
    end = date(2024, 12, 31)

    fires = load_conafor_fires(csv_path)

    log.info("Generando clima sintético para 51 municipios × 10 años...")
    records = []
    for muni in MUNICIPIOS:
        climate = generate_climate_for_municipality(muni, start, end)
        for row in climate:
            fecha = row["fecha"]
            fire_key = (muni["cve"], fecha)
            hubo_incendio = 1 if fire_key in fires else 0
            fire_info = fires.get(fire_key, {})

            dt = pd.Timestamp(fecha)
            records.append({
                "cve_muni": muni["cve"],
                "municipio": muni["nombre"],
                "fecha": fecha,
                "lat": muni["lat"],
                "lon": muni["lon"],
                "elevacion": muni["elev"],
                "zona": muni["zona"],
                "temp_max": row["temp_max"],
                "temp_min": row["temp_min"],
                "humedad_min": row["humedad_min"],
                "viento_max": row["viento_max"],
                "precipitacion": row["precipitacion"],
                "et0": row["et0"],
                "dias_sin_lluvia": row["dias_sin_lluvia"],
                "mes": dt.month,
                "dia_semana": dt.dayofweek,
                "dia_del_ano": dt.dayofyear,
                "hubo_incendio": hubo_incendio,
                "hectareas": fire_info.get("hectareas", 0),
            })

    df = pd.DataFrame(records)
    n_fire = df["hubo_incendio"].sum()
    log.info(f"Dataset: {len(df):,} filas, {n_fire} incendios ({n_fire/len(df)*100:.3f}%)")
    log.info(f"Fechas: {df['fecha'].min()} → {df['fecha'].max()}")
    log.info(f"Municipios: {df['cve_muni'].nunique()}")

    df.to_csv(output_path, index=False)
    log.info(f"Guardado: {output_path}")
    return df


if __name__ == "__main__":
    import sys
    from pathlib import Path

    script_dir = Path(__file__).parent

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

    print("\n=== Distribución por mes (incendios) ===")
    fire_df = df[df["hubo_incendio"] == 1]
    print(fire_df.groupby("mes").size())
    print(f"\n=== Resumen numérico ===")
    print(df[["temp_max","humedad_min","viento_max","precipitacion","dias_sin_lluvia","et0"]].describe())
