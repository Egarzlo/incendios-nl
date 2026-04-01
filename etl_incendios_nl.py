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

MUNICIPIOS_SHP = "data/municipios_nl.shp"

METEO_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "relative_humidity_2m_min",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]


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
def fetch_firms_hotspots(day_range: int = 2) -> list[dict]:
    sources = ["VIIRS_NOAA20_NRT", "VIIRS_SNPP_NRT"]
    all_hotspots = []

    for source in sources:
        url = (
            f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            f"{FIRMS_MAP_KEY}/{source}/{NL_BBOX_STR}/{day_range}"
        )
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            lines = resp.text.strip().split("\n")
            if len(lines) <= 1:
                log.info(f"FIRMS {source}: sin hotspots detectados")
                continue

            headers = lines[0].split(",")
            for line in lines[1:]:
                vals = line.split(",")
                if len(vals) < len(headers):
                    continue
                row = dict(zip(headers, vals))
                all_hotspots.append({
                    "latitude": float(row.get("latitude", 0)),
                    "longitude": float(row.get("longitude", 0)),
                    "brightness": float(row.get("bright_ti4", 0) or row.get("brightness", 0)),
                    "frp": float(row.get("frp", 0)),
                    "confidence": row.get("confidence", ""),
                    "satellite": row.get("satellite", source),
                    "source": source,
                    "detected_at": parse_firms_datetime(row.get('acq_date', ''), row.get('acq_time', '0000')),
                })
            log.info(f"FIRMS {source}: {len(lines) - 1} hotspots descargados")
        except Exception as e:
            log.error(f"Error fetching FIRMS {source}: {e}")

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
def geocode_hotspots_simple(hotspots: list[dict], municipios: list[dict]) -> list[dict]:
    from math import radians, sin, cos, sqrt, atan2

    def haversine(lat1, lon1, lat2, lon2):
        R = 6371
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        return R * 2 * atan2(sqrt(a), sqrt(1 - a))

    geocoded = []
    for h in hotspots:
        best_dist = float("inf")
        best_cve = None
        for m in municipios:
            d = haversine(h["latitude"], h["longitude"], m["lat_centroide"], m["lon_centroide"])
            if d < best_dist:
                best_dist = d
                best_cve = m["cve_muni"]
        if best_cve and best_dist < 100:
            h["cve_muni"] = best_cve
            geocoded.append(h)

    log.info(f"Geocodificación simple: {len(geocoded)}/{len(hotspots)} asignados")
    return geocoded


def geocode_hotspots_geopandas(hotspots: list[dict]) -> list[dict]:
    try:
        import geopandas as gpd
        from shapely.geometry import Point

        gdf = gpd.read_file(MUNICIPIOS_SHP).to_crs(epsg=4326)
        points = [Point(h["longitude"], h["latitude"]) for h in hotspots]
        hotspots_gdf = gpd.GeoDataFrame(hotspots, geometry=points, crs="EPSG:4326")
        joined = gpd.sjoin(hotspots_gdf, gdf, how="inner", predicate="within")

        geocoded = []
        for _, row in joined.iterrows():
            h = {k: row[k] for k in ["latitude", "longitude", "brightness", "frp",
                                      "confidence", "satellite", "source", "detected_at"]}
            h["cve_muni"] = str(row.get("CVE_MUN", row.get("CVEGEO", ""))).zfill(3)[-3:]
            geocoded.append(h)

        log.info(f"Geocodificación precisa: {len(geocoded)}/{len(hotspots)} dentro de NL")
        return geocoded
    except Exception as e:
        log.warning(f"Shapefile no disponible ({e}), usando geocodificación simple")
        return None


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

    # Mapear features del ETL a los features del modelo
    feature_map = {
        "temp_max": features.get("temp_max") or 0,
        "temp_min": features.get("temp_min") or 0,
        "humedad_min": features.get("humedad_min") or 50,
        "viento_max": features.get("viento_max") or 0,
        "precipitacion": features.get("precipitacion") or 0,
        "et0": features.get("et0") or 0,
        "dias_sin_lluvia": features.get("dias_sin_lluvia", 0),
        "mes": date.today().month,
        "dia_del_ano": date.today().timetuple().tm_yday,
        "lat": muni_info.get("lat_centroide", 25.5),
        "lon": muni_info.get("lon_centroide", -100.0),
        "elevacion": muni_info.get("elevacion_media", 500),
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
        f"— Sistema de Prediccion de Incendios, Vocacion Ambiental A.C."
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
    lineas.append("— Vocacion Ambiental A.C.")
    return "\n".join(lineas)


def enviar_email(destinatario: str, mensaje: str, pred: dict):
    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key:
        log.warning("SENDGRID_API_KEY no configurada")
        return False

    muni = pred.get("muni_nombre", pred["cve_muni"])
    payload = {
        "personalizations": [{"to": [{"email": destinatario}]}],
        "from": {"email": "alertas@vocacionambiental.org", "name": "Alertas Incendios NL"},
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

    # Paso 3
    hotspots_geo = []
    if hotspots_raw:
        result = geocode_hotspots_geopandas(hotspots_raw) if os.path.exists(MUNICIPIOS_SHP) else None
        if result is None:
            hotspots_geo = geocode_hotspots_simple(hotspots_raw, municipios_db)
        else:
            hotspots_geo = result

    hotspots_por_muni = {}
    for h in hotspots_geo:
        hotspots_por_muni.setdefault(h["cve_muni"], []).append(h)

    # Paso 4: calcular días sin lluvia desde BD (usa histórico completo)
    dias_sin_lluvia_db = {}
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
            dias = 0
            for dia in clima_hist:
                precip = dia.get("precipitacion") or 0
                if precip < 1.0:
                    dias += 1
                else:
                    break
            dias_sin_lluvia_db[cve] = dias
        except Exception:
            dias_sin_lluvia_db[cve] = 0

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
        hs_muni = hotspots_por_muni.get(cve, [])

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
            "n_hotspots_24h": len(hs_muni),
            "frp_max": max((h["frp"] for h in hs_muni), default=0),
            "elevacion_media": info.get("elevacion_media", 0),
            "pendiente_media": info.get("pendiente_media", 0),
        }

        # Predicción por reglas (condiciones climáticas)
        prob, nivel = calcular_riesgo(features)
        predicciones.append({
            "cve_muni": cve,
            "fecha": clima_hoy["fecha"],
            "prob": prob,
            "nivel": nivel,
            "features": features,
            "muni_nombre": info.get("nombre", cve),
            "modelo_version": "rules_v1",
        })

        # Predicción ML
        if modelo_ml:
            try:
                prob_ml, nivel_ml = predecir_ml(modelo_ml, features, info)
                predicciones_ml.append({
                    "cve_muni": cve,
                    "fecha": clima_hoy["fecha"],
                    "prob": prob_ml,
                    "nivel": nivel_ml,
                    "features": features,
                    "muni_nombre": info.get("nombre", cve),
                    "modelo_version": "ml_v1",
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
                "prob_incendio": p["prob"], "nivel_riesgo": p["nivel"],
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

    log.info("✅ ETL completado")


if __name__ == "__main__":
    main()
