# Sistema de Predicción de Incendios Forestales — Nuevo León
## Documento de continuidad para Cowork

---

## 1. Resumen del proyecto

Sistema automatizado de predicción diaria de riesgo de incendios forestales para los 51 municipios de Nuevo León. Integra datos satelitales de NASA, meteorología en tiempo real y un modelo de riesgo para generar alertas a tomadores de decisiones municipales.

**Estado actual: EN PRODUCCIÓN — funcionando con cron diario automático.**

- **Dashboard público**: https://incendios-nl.netlify.app/
- **Repositorio**: https://github.com/Egarzlo/incendios-nl
- **Base de datos**: Supabase (PostgreSQL) — proyecto rgskntzjsrqbeszeyvwo
- **ETL automático**: GitHub Actions, corre diario a las 6:00 AM CST (12:00 UTC)

---

## 2. Arquitectura actual

```
NASA FIRMS (hotspots VIIRS) ──┐
                               ├──→ GitHub Actions (ETL Python diario)
Open-Meteo (clima 51 munis) ──┘              │
                                             ▼
                                    Supabase (PostgreSQL)
                                             │
                              ┌──────────────┴──────────────┐
                              ▼                             ▼
                     Dashboard web                  Alertas email/SMS
                     (Netlify)                     (SendGrid/Twilio)
                     incendios-nl.netlify.app       (pendiente config)
```

### Stack tecnológico
- **ETL**: Python 3.11 (requests, geopandas)
- **Cron**: GitHub Actions (scheduled workflow)
- **Base de datos**: Supabase PostgreSQL (tier gratuito)
- **Frontend**: HTML/JS + Leaflet.js (mapa coropleta)
- **Hosting dashboard**: Netlify (auto-deploy desde GitHub)
- **Datos satelitales**: NASA FIRMS API (MAP_KEY gratuito)
- **Datos clima**: Open-Meteo API (sin key, gratuito)
- **Costo total**: $0 USD/mes

---

## 3. Estructura del repositorio

```
incendios-nl/
├── .github/workflows/
│   └── etl-incendios.yml          ← Cron job diario 6AM CST
├── dashboard/
│   ├── index.html                  ← Dashboard con mapa coropleta Leaflet
│   └── nuevo_leon.json             ← GeoJSON municipios NL
├── data/
│   └── .gitkeep                    ← Para shapefile INEGI y CSV CONAFOR
├── etl_incendios_nl.py             ← Pipeline ETL principal
├── backfill_clima.py               ← Script one-time para poblar histórico clima
├── fix_hotspots_duplicados.sql     ← SQL limpieza duplicados + UNIQUE constraint
├── schema.sql                      ← Tablas Supabase (6 tablas + vista + RLS)
├── requirements.txt
├── netlify.toml
├── .env.example
├── .gitignore
└── README.md
```

---

## 4. Base de datos (Supabase)

### Tablas
- municipios (51) — Catálogo con centroides, claves INEGI
- hotspots (~120/día) — Puntos de calor NASA FIRMS VIIRS
- clima_diario (~51/día + backfill 30d) — Temp, humedad, viento, precip
- predicciones (51/día) — Probabilidad y nivel de riesgo
- contactos (0, pendiente) — Directorio tomadores de decisiones
- alertas_enviadas (0, pendiente) — Log de alertas

### Vista: v_riesgo_actual
Join de predicciones + clima del último día, ordenado por probabilidad desc.

### RLS
- municipios, predicciones, hotspots → lectura pública (anon key)
- contactos, alertas_enviadas → solo service_role

---

## 5. Modelo de predicción actual (reglas v1)

Score de 0-100 normalizado a probabilidad:
- Sequedad combustible (35 pts): Días sin lluvia ≥14d=35, ≥7d=25, ≥3d=15
- Temperatura (20 pts): ≥40°C=20, ≥35=15, ≥30=10
- Déficit humedad (20 pts): ≤15%=20, ≤25=15, ≤40=10
- Viento (15 pts): ≥50km/h=15, ≥30=10, ≥15=5
- Fuego activo (10 pts): ≥3 hotspots=10, ≥1=7

Niveles: BAJO (0-19%), MEDIO (20-39%), ALTO (40-59%), MUY_ALTO (60-79%), EXTREMO (80-100%)

---

## 6. Próximo paso prioritario: MODELO ML

### Datos disponibles
- CSV CONAFOR 2015-2024 (21 MB): https://www.datos.gob.mx/dataset/3a1d4a71-4dad-4ae9-9eec-44de7fa8ebf3/resource/ddf38874-6243-4437-8f76-19f797cafa5c/download/estadisticasincendiosforestales2015-2024.csv
  - Campos: anio, latitud, longitud, CVE_ENT, CVE_MUN, Municipio, Fecha_Inicio, Fecha_Termino, Causa, Tipo_Vegetacion, Total_hectareas
  - Filtrar CVE_ENT = 19 para Nuevo León
- Open-Meteo Historical API (ERA5): clima desde 1940
- NASA FIRMS Archive: hotspots desde 2012

### Estrategia
1. Descargar CSV CONAFOR → filtrar NL → incendios por municipio-día
2. Consultar clima histórico Open-Meteo para cada municipio-día 2015-2024
3. Dataset: ~186,000 filas (51 munis × 365 días × 10 años)
4. Variable objetivo: hubo_incendio (0/1)
5. Features: temp_max, hum_min, viento, precip, dias_sin_lluvia, et0, mes, dia_semana, elevacion, pendiente, tipo_vegetacion
6. Modelos: Random Forest, XGBoost, Logistic Regression
7. Validación temporal: train 2015-2022, test 2023-2024
8. Exportar .pkl con joblib → ETL usa model.predict_proba()

### Nota: dataset muy desbalanceado (~99% sin incendio). Usar SMOTE o class_weight='balanced'.

---

## 7. Otros pendientes

- Formulario de registro de contactos municipales
- Configurar SendGrid para alertas por email (100/día gratis)
- GeoJSON: falta municipio Hualahuises en el archivo
- Ejecutar fix_hotspots_duplicados.sql en Supabase si no se hizo
- Agregar NDVI y humedad del suelo como features adicionales

---

## 8. Para retomar en Cowork

Contexto para Claude:
"Estoy continuando el proyecto de predicción de incendios de Nuevo León. El repo está en github.com/Egarzlo/incendios-nl, el dashboard en incendios-nl.netlify.app, y la BD en Supabase. El ETL corre diario con GitHub Actions. [Tu siguiente paso]."

---

Proyecto: BIOIMPACT / SEMA Nuevo León — Marzo 2026
