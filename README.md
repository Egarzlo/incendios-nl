# 🔥 Sistema de Predicción de Incendios Forestales — Nuevo León

Sistema automatizado de predicción diaria de riesgo de incendios forestales para los 51 municipios de Nuevo León, con alertas por correo y SMS a tomadores de decisiones municipales.

## Arquitectura

```
NASA FIRMS (hotspots) ──┐
                        ├──→ GitHub Actions (ETL diario 6AM CST)
Open-Meteo (clima)  ────┘         │
                                  ▼
                            Supabase (PostgreSQL)
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
            Dashboard web               Alertas email/SMS
            (Netlify)                   (SendGrid/Twilio)
```

## Datos utilizados (todos gratuitos)

| Fuente | Datos | Frecuencia | Licencia |
|--------|-------|------------|----------|
| NASA FIRMS | Hotspots VIIRS/MODIS 375m | Cada 3 horas | Abierto |
| Open-Meteo | Temp, humedad, viento, precip | Horaria | CC BY 4.0 |
| CONAFOR | Históricos 2015-2024 (CSV/SHP) | Anual | Datos abiertos |
| INEGI | Uso suelo, vegetación, topografía | Periódica | Abierto |

## Configuración rápida

### 1. Obtener API keys gratuitas

- **NASA FIRMS MAP_KEY**: https://firms.modaps.eosdis.nasa.gov/api/map_key/
- **Supabase**: Crear proyecto en https://supabase.com (tier gratuito)

### 2. Crear base de datos

Ejecutar `schema.sql` en el SQL Editor de Supabase.

### 3. Configurar GitHub Secrets

En tu repositorio: Settings → Secrets and variables → Actions:

| Secret | Descripción |
|--------|-------------|
| `FIRMS_MAP_KEY` | Key de NASA FIRMS |
| `SUPABASE_URL` | URL de tu proyecto Supabase |
| `SUPABASE_KEY` | Service role key de Supabase |
| `SENDGRID_API_KEY` | (Opcional) Para alertas email |

### 4. Desplegar

```bash
git init
git add .
git commit -m "Sistema de predicción de incendios NL"
git remote add origin https://github.com/tu-usuario/incendios-nl.git
git push -u origin main
```

El workflow de GitHub Actions se ejecutará automáticamente a las 6:00 AM CST todos los días.

### 5. Test manual

Ir a: Actions → "ETL Predicción de Incendios NL" → "Run workflow"

## Estructura del proyecto

```
incendios-nl/
├── .github/workflows/
│   └── etl-incendios.yml      ← Cron job diario
├── data/
│   └── municipios_nl.shp      ← Shapefile INEGI (opcional)
├── dashboard/
│   └── index.html              ← Mapa de riesgo (Netlify)
├── etl_incendios_nl.py         ← Pipeline principal
├── schema.sql                  ← Tablas PostgreSQL
├── requirements.txt
├── netlify.toml
└── README.md
```

## Modelo de riesgo

Actualmente usa un modelo basado en reglas ponderadas:
- Días sin lluvia (35%)
- Temperatura máxima (20%)
- Humedad mínima (20%)
- Velocidad del viento (15%)
- Hotspots activos (10%)

Próximo paso: entrenar Random Forest / XGBoost con datos históricos de CONAFOR.


