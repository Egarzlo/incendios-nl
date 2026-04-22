-- ============================================================================
-- Schema: Sistema de Predicción de Incendios — Nuevo León
-- Ejecutar en: Supabase SQL Editor
-- ============================================================================

CREATE TABLE municipios (
    id SERIAL PRIMARY KEY,
    cve_muni VARCHAR(5) UNIQUE NOT NULL,
    nombre VARCHAR(100) NOT NULL,
    lat_centroide DOUBLE PRECISION NOT NULL,
    lon_centroide DOUBLE PRECISION NOT NULL,
    tipo_vegetacion VARCHAR(100),
    elevacion_media DOUBLE PRECISION DEFAULT 0,
    pendiente_media DOUBLE PRECISION DEFAULT 0,
    area_forestal_ha DOUBLE PRECISION DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE hotspots (
    id SERIAL PRIMARY KEY,
    municipio_id INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    brightness DOUBLE PRECISION,
    frp DOUBLE PRECISION,
    source VARCHAR(30) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    satellite VARCHAR(20),
    confidence VARCHAR(10),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Necesario para que el upsert on_conflict=latitude,longitude,detected_at,source
    -- del ETL sea idempotente y no genere duplicados entre runs diarios que solapan 48h.
    UNIQUE (latitude, longitude, detected_at, source)
);
CREATE INDEX idx_hotspots_muni_fecha ON hotspots(municipio_id, detected_at);

CREATE TABLE clima_diario (
    id SERIAL PRIMARY KEY,
    municipio_id INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
    fecha DATE NOT NULL,
    temp_max DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    humedad_min DOUBLE PRECISION,
    viento_max DOUBLE PRECISION,
    precipitacion DOUBLE PRECISION,
    dias_sin_lluvia INTEGER DEFAULT 0,
    et0 DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(municipio_id, fecha)
);

CREATE TABLE predicciones (
    id SERIAL PRIMARY KEY,
    municipio_id INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
    fecha DATE NOT NULL,
    -- prob_incendio = prob_base ajustada con factor antropogenico (valor operativo).
    -- prob_base = probabilidad del modelo sin factor (clima + ML solamente).
    prob_incendio DOUBLE PRECISION NOT NULL,
    prob_base DOUBLE PRECISION,
    nivel_riesgo VARCHAR(20) NOT NULL CHECK (nivel_riesgo IN ('BAJO','MEDIO','ALTO','MUY_ALTO','EXTREMO')),
    factor_antropogenico_pts INTEGER DEFAULT 0,
    factor_antropogenico_etiquetas JSONB DEFAULT '[]'::jsonb,
    ndvi DOUBLE PRECISION,
    fuel_moisture DOUBLE PRECISION,
    features_json JSONB,
    modelo_version VARCHAR(20) DEFAULT 'rules_v1',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Permite coexistir rules_v1 y ml_v1 para la misma (muni,fecha).
    -- El ETL upserta con on_conflict=(municipio_id, fecha, modelo_version).
    UNIQUE(municipio_id, fecha, modelo_version)
);
CREATE INDEX idx_pred_fecha_nivel ON predicciones(fecha, nivel_riesgo);

CREATE TABLE contactos (
    id SERIAL PRIMARY KEY,
    municipio_id INTEGER REFERENCES municipios(id) ON DELETE CASCADE,
    nombre VARCHAR(150) NOT NULL,
    cargo VARCHAR(150),
    email VARCHAR(200),
    telefono VARCHAR(20),
    canal_pref VARCHAR(20) DEFAULT 'email' CHECK (canal_pref IN ('email','sms','whatsapp')),
    activo BOOLEAN DEFAULT TRUE,
    notas TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE alertas_enviadas (
    id SERIAL PRIMARY KEY,
    prediccion_id INTEGER REFERENCES predicciones(id) ON DELETE SET NULL,
    contacto_id INTEGER REFERENCES contactos(id) ON DELETE SET NULL,
    canal VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sent','delivered','failed','error')),
    mensaje TEXT,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Vista: riesgo actual por modelo (incluye ambas versiones rules_v1 y ml_v1)
CREATE VIEW v_riesgo_actual AS
SELECT m.cve_muni, m.nombre AS municipio, p.fecha,
       p.prob_incendio,       -- ajustada (con factor antropogenico)
       p.prob_base,           -- del modelo (sin factor)
       p.nivel_riesgo,
       p.modelo_version,
       p.factor_antropogenico_pts,
       p.factor_antropogenico_etiquetas,
       c.temp_max, c.humedad_min, c.viento_max, c.dias_sin_lluvia, c.precipitacion,
       (SELECT COUNT(*) FROM hotspots h
        WHERE h.municipio_id = m.id
        AND h.detected_at >= NOW() - INTERVAL '24 hours') AS hotspots_24h
FROM municipios m
JOIN predicciones p ON p.municipio_id = m.id
    AND p.fecha = (SELECT MAX(fecha) FROM predicciones WHERE modelo_version = p.modelo_version)
LEFT JOIN clima_diario c ON c.municipio_id = m.id AND c.fecha = p.fecha
ORDER BY p.modelo_version, p.prob_incendio DESC;

-- Vista: comparativa lado a lado de reglas vs ML para el ultimo dia
CREATE VIEW v_comparativa_modelos AS
SELECT m.cve_muni, m.nombre AS municipio, r.fecha,
       r.prob_incendio AS prob_reglas, r.prob_base AS prob_reglas_base, r.nivel_riesgo AS nivel_reglas,
       ml.prob_incendio AS prob_ml, ml.prob_base AS prob_ml_base, ml.nivel_riesgo AS nivel_ml,
       r.factor_antropogenico_pts, r.factor_antropogenico_etiquetas,
       c.temp_max, c.humedad_min, c.viento_max, c.dias_sin_lluvia,
       (SELECT COUNT(*) FROM hotspots h
        WHERE h.municipio_id = m.id
        AND h.detected_at >= NOW() - INTERVAL '24 hours') AS hotspots_24h
FROM municipios m
JOIN predicciones r ON r.municipio_id = m.id
    AND r.modelo_version = 'rules_v1'
    AND r.fecha = (SELECT MAX(fecha) FROM predicciones WHERE modelo_version = 'rules_v1')
LEFT JOIN predicciones ml ON ml.municipio_id = m.id
    AND ml.modelo_version = 'ml_v1'
    AND ml.fecha = r.fecha
LEFT JOIN clima_diario c ON c.municipio_id = m.id AND c.fecha = r.fecha
ORDER BY GREATEST(r.prob_incendio, COALESCE(ml.prob_incendio, 0)) DESC;

-- RLS
ALTER TABLE municipios ENABLE ROW LEVEL SECURITY;
ALTER TABLE predicciones ENABLE ROW LEVEL SECURITY;
ALTER TABLE hotspots ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Lectura pública municipios" ON municipios FOR SELECT USING (true);
CREATE POLICY "Lectura pública predicciones" ON predicciones FOR SELECT USING (true);
CREATE POLICY "Lectura pública hotspots" ON hotspots FOR SELECT USING (true);

ALTER TABLE contactos ENABLE ROW LEVEL SECURITY;
ALTER TABLE alertas_enviadas ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Solo admin contactos" ON contactos FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Solo admin alertas" ON alertas_enviadas FOR ALL USING (auth.role() = 'service_role');

-- 51 municipios de Nuevo León
-- Centroides derivados del Marco Geoestadístico INEGI 2025.1 (shapely centroid,
-- con fallback a representative_point cuando el centroid cae fuera del polígono).
-- Todos los puntos están garantizados dentro de su propio polígono municipal.
INSERT INTO municipios (cve_muni, nombre, lat_centroide, lon_centroide) VALUES
('001','Abasolo',25.940543,-100.405942),('002','Agualeguas',26.298712,-99.703083),
('003','Los Aldamas',26.091506,-99.27348),('004','Allende',25.301434,-100.029518),
('005','Anáhuac',27.342154,-100.025355),('006','Apodaca',25.792542,-100.187381),
('007','Aramberri',24.225118,-99.886509),('008','Bustamante',26.571711,-100.561792),
('009','Cadereyta Jiménez',25.524591,-99.914185),('010','El Carmen',25.900771,-100.356885),
('011','Cerralvo',26.072318,-99.705374),('012','Ciénega de Flores',25.977458,-100.185436),
('013','China',25.480159,-98.972409),('014','Doctor Arroyo',23.860108,-100.306266),
('015','Doctor Coss',25.964091,-99.030873),('016','Doctor González',25.849252,-99.80498),
('017','Galeana',24.760509,-100.392287),('018','García',25.809011,-100.659777),
('019','San Pedro Garza García',25.644597,-100.374758),('020','General Bravo',25.803315,-98.848418),
('021','General Escobedo',25.821867,-100.355575),('022','General Terán',25.275874,-99.413017),
('023','General Treviño',26.212648,-99.445967),('024','General Zaragoza',23.901029,-99.740174),
('025','General Zuazua',25.911392,-100.13482),('026','Guadalupe',25.67276,-100.205599),
('027','Los Herreras',25.916079,-99.41424),('028','Higueras',26.033168,-99.997508),
('029','Hualahuises',24.883791,-99.678096),('030','Iturbide',24.638418,-99.848716),
('031','Juárez',25.614087,-100.121406),('032','Lampazos de Naranjo',27.050841,-100.418216),
('033','Linares',24.851771,-99.52922),('034','Marín',25.886143,-100.023315),
('035','Melchor Ocampo',26.048931,-99.493516),('036','Mier y Noriega',23.417917,-100.160408),
('037','Mina',26.285584,-100.786277),('038','Montemorelos',25.126305,-99.808381),
('039','Monterrey',25.64464,-100.310952),('040','Parás',26.583042,-99.601554),
('041','Pesquería',25.736769,-99.977135),('042','Los Ramones',25.65331,-99.587667),
('043','Rayones',25.065823,-100.127733),('044','Sabinas Hidalgo',26.575102,-100.149288),
('045','Salinas Victoria',26.161052,-100.270385),('046','San Nicolás de los Garza',25.736076,-100.270693),
('047','Hidalgo',25.999402,-100.45319),('048','Santa Catarina',25.57461,-100.483863),
('049','Santiago',25.384897,-100.237323),('050','Vallecillo',26.648106,-99.884763),
('051','Villaldama',26.469705,-100.34807)
ON CONFLICT (cve_muni) DO NOTHING;
