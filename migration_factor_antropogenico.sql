-- ============================================================================
-- Migracion: Factor antropogenico almacenado en BD
-- Ejecutar en Supabase SQL Editor.
--
-- Motivacion: hasta ahora el dashboard calculaba el factor antropogenico en
-- el navegador y modificaba prob_incendio localmente. Esto provocaba que el
-- numero mostrado al usuario no coincidiera con el almacenado en BD. Ademas
-- la tabla ZONA_BY_CVE del frontend tenia un bug (claves numericas vs string)
-- que hacia que todos los municipios cayeran al default 'norte'.
--
-- Esta migracion mueve todo el calculo al ETL y almacena:
--   - prob_base: probabilidad del modelo (clima/ML) sin factor humano
--   - prob_incendio: probabilidad ajustada (base + factor), la que se usa
--     como valor operativo por defecto
--   - factor_antropogenico_pts: 0-20 pts del factor calendario+actividad
--   - factor_antropogenico_etiquetas: lista de eventos activos ese dia
--
-- El dashboard a partir de esta migracion tendra un checkbox para mostrar
-- prob_base o prob_incendio (ajustada), sin recalcular nada en el frontend.
-- ============================================================================

-- 1. Agregar columnas nuevas a predicciones
ALTER TABLE predicciones
    ADD COLUMN IF NOT EXISTS prob_base DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS factor_antropogenico_pts INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS factor_antropogenico_etiquetas JSONB DEFAULT '[]'::jsonb;

-- 2. Backfill: para filas historicas, prob_base = prob_incendio (asumimos
--    que el factor frontend era 0 por el bug de zone lookup; los calculos
--    historicos ya incorporaban el default 'norte' que aplicaba igual a todos)
UPDATE predicciones
SET prob_base = prob_incendio
WHERE prob_base IS NULL;

-- 3. Reemplazar vista v_riesgo_actual para incluir ambas probabilidades y el factor
DROP VIEW IF EXISTS v_riesgo_actual CASCADE;
CREATE VIEW v_riesgo_actual AS
SELECT m.cve_muni, m.nombre AS municipio, p.fecha,
       p.prob_incendio,       -- probabilidad ajustada (con factor antropogenico)
       p.prob_base,           -- probabilidad del modelo sin factor
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

-- 4. Reemplazar v_comparativa_modelos tambien para exponer prob_base
DROP VIEW IF EXISTS v_comparativa_modelos CASCADE;
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

-- 5. Verificacion
SELECT
    COUNT(*) AS total_predicciones,
    COUNT(prob_base) AS con_prob_base,
    COUNT(*) FILTER (WHERE factor_antropogenico_pts > 0) AS con_factor,
    MAX(factor_antropogenico_pts) AS max_pts
FROM predicciones;
