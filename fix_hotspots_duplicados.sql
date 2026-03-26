-- ============================================================================
-- Limpieza de hotspots duplicados y constraint para evitar futuros
-- Ejecutar en Supabase SQL Editor
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
