-- ============================================================================
-- [HISTORICO - YA APLICADO EN PRODUCCION 2026-04] Limpieza de hotspots
-- duplicados y constraint UNIQUE (lat, lon, detected_at, source).
-- La constraint equivalente ya esta declarada directamente en schema.sql,
-- por lo que este script NO debe re-ejecutarse en nuevos deploys; se conserva
-- aqui como referencia historica de la migracion.
-- Validado 2026-04-22: 0 duplicados en 964 hotspots.
-- ============================================================================

-- 1. Ver cuántos duplicados hay
SELECT COUNT(*) as total,
       COUNT(DISTINCT (latitude::text || longitude::text || detected_at::text || source)) as unicos
FROM hotspots;

-- 2. Eliminar duplicados conservando solo el primer registro (menor id)
DELETE FROM hotspots
WHERE id NOT IN (
    SELECT MIN(id)
    FROM hotspots
    GROUP BY latitude, longitude, detected_at, source
);

-- 3. Agregar constraint UNIQUE para evitar futuros duplicados
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotspots_unique 
ON hotspots(latitude, longitude, detected_at, source);

-- 4. Verificar resultado
SELECT COUNT(*) as hotspots_despues_limpieza FROM hotspots;
