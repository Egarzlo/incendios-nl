-- ============================================================================
-- Migración: Soporte para modelo ML (v1)
-- Ejecutar en Supabase SQL Editor
-- ============================================================================

-- 1. Reemplazar el UNIQUE constraint en predicciones para incluir modelo_version
ALTER TABLE predicciones DROP CONSTRAINT IF EXISTS predicciones_municipio_id_fecha_key;
DROP INDEX IF EXISTS predicciones_municipio_id_fecha_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_predicciones_muni_fecha_modelo
ON predicciones(municipio_id, fecha, modelo_version);

-- 2. Vista: riesgo actual por modelo (reemplaza la anterior)
DROP VIEW IF EXISTS v_riesgo_actual;

CREATE VIEW v_riesgo_actual AS
SELECT m.cve_muni, m.nombre AS municipio, p.fecha,
       p.prob_incendio, p.nivel_riesgo, p.modelo_version,
       c.temp_max, c.humedad_min, c.viento_max, c.dias_sin_lluvia, c.precipitacion,
       (SELECT COUNT(*) FROM hotspots h
        WHERE h.municipio_id = m.id
        AND h.detected_at >= NOW() - INTERVAL '24 hours') AS hotspots_24h
FROM municipios m
JOIN predicciones p ON p.municipio_id = m.id
    AND p.fecha = (SELECT MAX(fecha) FROM predicciones WHERE modelo_version = p.modelo_version)
LEFT JOIN clima_diario c ON c.municipio_id = m.id AND c.fecha = p.fecha
ORDER BY p.modelo_version, p.prob_incendio DESC;

-- 3. Vista: comparativa de modelos (ambas predicciones lado a lado)
CREATE OR REPLACE VIEW v_comparativa_modelos AS
SELECT m.cve_muni, m.nombre AS municipio,
       r.fecha,
       r.prob_incendio AS prob_reglas, r.nivel_riesgo AS nivel_reglas,
       ml.prob_incendio AS prob_ml, ml.nivel_riesgo AS nivel_ml,
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

-- 4. Verificar
SELECT 'Migración ML v1 aplicada correctamente' AS status;
