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
    created_at TIMESTAMPTZ DEFAULT NOW()
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
    prob_incendio DOUBLE PRECISION NOT NULL,
    nivel_riesgo VARCHAR(20) NOT NULL CHECK (nivel_riesgo IN ('BAJO','MEDIO','ALTO','MUY_ALTO','EXTREMO')),
    ndvi DOUBLE PRECISION,
    fuel_moisture DOUBLE PRECISION,
    features_json JSONB,
    modelo_version VARCHAR(20) DEFAULT 'rules_v1',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(municipio_id, fecha)
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

-- Vista: riesgo actual
CREATE VIEW v_riesgo_actual AS
SELECT m.cve_muni, m.nombre AS municipio, p.fecha, p.prob_incendio, p.nivel_riesgo,
       c.temp_max, c.humedad_min, c.viento_max, c.dias_sin_lluvia, c.precipitacion,
       (SELECT COUNT(*) FROM hotspots h WHERE h.municipio_id = m.id AND h.detected_at >= NOW() - INTERVAL '24 hours') AS hotspots_24h
FROM municipios m
JOIN predicciones p ON p.municipio_id = m.id AND p.fecha = (SELECT MAX(fecha) FROM predicciones)
LEFT JOIN clima_diario c ON c.municipio_id = m.id AND c.fecha = p.fecha
ORDER BY p.prob_incendio DESC;

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
INSERT INTO municipios (cve_muni, nombre, lat_centroide, lon_centroide) VALUES
('001','Abasolo',25.9439,-100.4117),('002','Agualeguas',26.3106,-99.5453),
('003','Los Aldamas',26.0614,-99.1328),('004','Allende',25.2833,-100.0167),
('005','Anáhuac',27.2325,-100.1394),('006','Apodaca',25.7814,-100.1883),
('007','Aramberri',24.1006,-99.8169),('008','Bustamante',26.5325,-100.5142),
('009','Cadereyta Jiménez',25.5864,-99.9808),('010','El Carmen',25.9667,-100.2500),
('011','Cerralvo',26.0833,-99.6167),('012','Ciénega de Flores',25.9500,-100.1667),
('013','China',25.7000,-99.2333),('014','Doctor Arroyo',23.6742,-100.1878),
('015','Doctor Coss',25.9500,-99.0167),('016','Doctor González',25.8500,-100.0167),
('017','Galeana',24.8333,-100.0833),('018','García',25.8167,-100.5833),
('019','San Pedro Garza García',25.6500,-100.4000),('020','General Bravo',25.8000,-99.1667),
('021','General Escobedo',25.7833,-100.3167),('022','General Terán',25.2500,-99.6667),
('023','General Treviño',26.2333,-99.2667),('024','General Zaragoza',23.9833,-99.7833),
('025','General Zuazua',25.9500,-100.0833),('026','Guadalupe',25.6833,-100.2500),
('027','Los Herreras',25.9167,-99.4167),('028','Higueras',25.9500,-100.0167),
('029','Hualahuises',25.0667,-99.6667),('030','Iturbide',24.7333,-99.9000),
('031','Juárez',25.6500,-100.0833),('032','Lampazos de Naranjo',27.0333,-100.5167),
('033','Linares',24.8597,-99.5675),('034','Marín',25.8833,-100.0333),
('035','Melchor Ocampo',26.4500,-99.4167),('036','Mier y Noriega',23.5500,-100.2000),
('037','Mina',26.0167,-100.5833),('038','Montemorelos',25.1833,-99.8333),
('039','Monterrey',25.6714,-100.3089),('040','Parás',26.5000,-99.4333),
('041','Pesquería',25.7833,-100.0500),('042','Los Ramones',25.7000,-99.6333),
('043','Rayones',25.0167,-100.0833),('044','Sabinas Hidalgo',26.5083,-100.1778),
('045','Salinas Victoria',25.9667,-100.2833),('046','San Nicolás de los Garza',25.7500,-100.2833),
('047','Hidalgo',25.9833,-100.4500),('048','Santa Catarina',25.6833,-100.4500),
('049','Santiago',25.4167,-100.1500),('050','Vallecillo',26.6667,-99.9833),
('051','Villaldama',26.5000,-100.4167)
ON CONFLICT (cve_muni) DO NOTHING;
