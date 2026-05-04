-- ============================================================================
-- Fix de auditoria — predicciones
-- Ejecutar en Supabase SQL Editor.
--
-- 1. Eliminar predicciones del modelo ml_v1 viejo. Era el modelo entrenado
--    con clima sintetico (build_dataset_local.py). Fue reemplazado por ml_v2
--    entrenado con ERA5 real. Las filas viejas siguen apareciendo en la
--    vista v_riesgo_actual con valores incorrectos (Galeana BAJO 11%).
--
-- 2. Re-clasificar 2 filas con prob_incendio=0.4 que quedaron como MEDIO
--    cuando deberian ser ALTO (artefacto de floating-point en el ETL ya
--    arreglado en codigo). Solo afecta 2 munis del dia actual.
-- ============================================================================

-- 1. Borrar ml_v1 (1173 filas)
DELETE FROM predicciones WHERE modelo_version = 'ml_v1';

-- 2. Re-clasificar filas con prob_incendio en [0.4, 0.4001] como ALTO
UPDATE predicciones
SET nivel_riesgo = 'ALTO'
WHERE prob_incendio >= 0.4 AND prob_incendio < 0.4001
  AND nivel_riesgo = 'MEDIO';

-- Verificacion
SELECT modelo_version, COUNT(*) AS filas, MIN(fecha) AS desde, MAX(fecha) AS hasta
FROM predicciones GROUP BY modelo_version ORDER BY modelo_version;

SELECT cve_muni, m.nombre, prob_incendio, nivel_riesgo
FROM predicciones p JOIN municipios m ON m.id = p.municipio_id
WHERE p.fecha = (SELECT MAX(fecha) FROM predicciones)
  AND p.modelo_version = 'rules_v1'
  AND p.prob_incendio BETWEEN 0.39 AND 0.41
ORDER BY prob_incendio DESC;
