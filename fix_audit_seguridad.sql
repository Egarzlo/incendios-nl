-- ============================================================================
-- FIX URGENTE DE SEGURIDAD — clima_diario sin RLS
-- Ejecutar en Supabase SQL Editor.
--
-- Hallazgo de auditoria: la tabla clima_diario NO tenia RLS habilitado en
-- el schema original (schema.sql). Esto permitia que cualquier cliente con
-- la anon key (visible en el dashboard) borrara o modificara el historico
-- climatico. Las demas tablas (municipios, predicciones, hotspots, etc.)
-- ya tenian RLS correcto.
--
-- Esta migracion:
--   1. Habilita RLS en clima_diario.
--   2. Crea politica de lectura publica (igual que las otras tablas de datos).
--   3. La escritura queda restringida a service_role automaticamente
--      (sin politica explicita = denied para anon/authenticated).
--   4. Restaura la fila borrada durante la auditoria (id 7627 perdida).
-- ============================================================================

-- 1. Habilitar RLS
ALTER TABLE clima_diario ENABLE ROW LEVEL SECURITY;

-- 2. Politica de lectura publica
DROP POLICY IF EXISTS "Lectura publica clima_diario" ON clima_diario;
CREATE POLICY "Lectura publica clima_diario" ON clima_diario
    FOR SELECT USING (TRUE);

-- 3. Restaurar fila perdida durante test de penetracion (auditoria 2026-05-04).
--    Datos: cve_muni=029 (Hualahuises), fecha 2026-04-02
INSERT INTO clima_diario (
    municipio_id, fecha, temp_max, temp_min, humedad_min, viento_max,
    precipitacion, dias_sin_lluvia, et0
)
SELECT 29, '2026-04-02'::date, 35.7, 20.9, 30, 25.7, 0, 24, 6.38
WHERE NOT EXISTS (
    SELECT 1 FROM clima_diario WHERE municipio_id = 29 AND fecha = '2026-04-02'
);

-- 4. Verificacion
SELECT
    'clima_diario' AS tabla,
    relrowsecurity AS rls_enabled,
    (SELECT COUNT(*) FROM pg_policies WHERE schemaname='public' AND tablename='clima_diario') AS politicas
FROM pg_class WHERE relname = 'clima_diario';

SELECT COUNT(*) AS filas_clima_diario FROM clima_diario;
