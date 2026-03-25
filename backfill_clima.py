"""
Backfill histórico de clima — Últimos 30 días para 51 municipios de NL
======================================================================
Ejecutar UNA VEZ para poblar clima_diario con datos históricos.
Después de esto, el ETL diario mantendrá los datos actualizados.

Uso:
    python backfill_clima.py

Esto permitirá que el cálculo de "días sin lluvia" sea preciso
desde el primer día de operación del sistema.
"""

import os
import json
import time
import logging
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

METEO_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]

DAYS_BACK = 30


def safe_float(lst, idx):
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def supabase_request(method, table, data=None, params=None):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if method == "GET":
        r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    else:
        r = requests.post(url, headers=headers, json=data, timeout=60)
    if not r.ok:
        log.error(f"Supabase {method} {table}: {r.status_code} — {r.text[:300]}")
    r.raise_for_status()
    return r.json()


def main():
    log.info("=" * 60)
    log.info(f"Backfill de clima — últimos {DAYS_BACK} días")
    log.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("Configura SUPABASE_URL y SUPABASE_KEY en .env")
        return

    # Cargar municipios
    municipios = supabase_request("GET", "municipios", params={
        "select": "id,cve_muni,lat_centroide,lon_centroide,nombre"
    })
    log.info(f"Municipios: {len(municipios)}")

    today = date.today()
    start = today - timedelta(days=DAYS_BACK)

    # Fetch Open-Meteo en lotes de 15
    batch_size = 15
    all_rows = []

    for i in range(0, len(municipios), batch_size):
        batch = municipios[i:i + batch_size]
        lats = ",".join(str(m["lat_centroide"]) for m in batch)
        lons = ",".join(str(m["lon_centroide"]) for m in batch)

        log.info(f"Lote {i // batch_size + 1}: {len(batch)} municipios ({start} a {today})")

        try:
            resp = requests.get("https://api.open-meteo.com/v1/forecast", params={
                "latitude": lats,
                "longitude": lons,
                "daily": ",".join(METEO_DAILY_VARS),
                "start_date": start.isoformat(),
                "end_date": today.isoformat(),
                "timezone": "America/Monterrey",
                "past_days": 0,
            }, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict):
                data = [data]

            for j, muni_data in enumerate(data):
                if j >= len(batch):
                    break
                muni = batch[j]
                daily = muni_data.get("daily", {})
                times = daily.get("time", [])

                for k, fecha_str in enumerate(times):
                    all_rows.append({
                        "municipio_id": muni["id"],
                        "fecha": fecha_str,
                        "temp_max": safe_float(daily.get("temperature_2m_max", []), k),
                        "temp_min": safe_float(daily.get("temperature_2m_min", []), k),
                        "humedad_min": safe_float(daily.get("relative_humidity_2m_min", []), k),
                        "viento_max": safe_float(daily.get("wind_speed_10m_max", []), k),
                        "precipitacion": safe_float(daily.get("precipitation_sum", []), k),
                        "et0": safe_float(daily.get("et0_fao_evapotranspiration", []), k),
                    })

        except Exception as e:
            log.error(f"Error lote {i // batch_size + 1}: {e}")

        if i + batch_size < len(municipios):
            time.sleep(2)

    log.info(f"Total registros a insertar: {len(all_rows)}")

    # Upsert en lotes de 200
    upsert_url = f"{SUPABASE_URL}/rest/v1/clima_diario?on_conflict=municipio_id,fecha"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }

    inserted = 0
    for i in range(0, len(all_rows), 200):
        batch = all_rows[i:i + 200]
        try:
            r = requests.post(upsert_url, headers=headers, json=batch, timeout=60)
            if r.ok:
                inserted += len(batch)
                log.info(f"  Upsert {inserted}/{len(all_rows)} registros")
            else:
                log.error(f"  Error upsert: {r.status_code} — {r.text[:200]}")
        except Exception as e:
            log.error(f"  Error upsert: {e}")

    log.info(f"Backfill completado: {inserted} registros insertados")

    # Verificar: calcular días sin lluvia para hoy
    log.info("")
    log.info("Verificación — días sin lluvia para municipios clave:")
    munis_check = ["039", "049", "018", "048", "017"]  # MTY, Santiago, García, Sta Catarina, Galeana
    for cve in munis_check:
        muni = next((m for m in municipios if m["cve_muni"] == cve), None)
        if not muni:
            continue
        clima = supabase_request("GET", "clima_diario", params={
            "select": "fecha,precipitacion",
            "municipio_id": f"eq.{muni['id']}",
            "order": "fecha.desc",
            "limit": "30",
        })
        dias = 0
        for dia in clima:
            precip = dia.get("precipitacion") or 0
            if precip < 1.0:
                dias += 1
            else:
                break
        log.info(f"  {muni['nombre']}: {dias} días sin lluvia")


if __name__ == "__main__":
    main()
