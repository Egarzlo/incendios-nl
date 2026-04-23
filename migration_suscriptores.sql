-- ============================================================================
-- Migracion: sistema de suscripciones publicas con auto-registro
-- Ejecutar en Supabase SQL Editor.
--
-- Crea tabla suscriptores ligada a auth.users. Cada usuario se suscribe via
-- Magic Link (sin password), elige sus municipios de interes (multi-select),
-- cadencia y nivel minimo de alerta. El ETL diario consulta esta tabla y
-- envia correos personalizados via Mailjet.
--
-- Separada de la tabla contactos existente:
--   contactos    -> funcionarios SEMA pre-registrados via SQL (canal operativo)
--   suscriptores -> publico/operativos de campo con auto-registro por email
-- ============================================================================

-- 1. Tabla principal
CREATE TABLE IF NOT EXISTS suscriptores (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    email TEXT NOT NULL UNIQUE,
    nombre TEXT,
    -- Array de cve_muni (ej: ['017','014']); ['*'] significa todos los municipios.
    municipios_cve TEXT[] NOT NULL DEFAULT ARRAY['*'],
    -- Cadencia: diario=todos los dias, solo_alertas=solo cuando hay muni>=nivel_minimo.
    cadencia TEXT NOT NULL DEFAULT 'solo_alertas'
        CHECK (cadencia IN ('diario', 'solo_alertas')),
    -- Nivel minimo para disparar correo en modo solo_alertas.
    nivel_minimo TEXT NOT NULL DEFAULT 'ALTO'
        CHECK (nivel_minimo IN ('MEDIO', 'ALTO', 'MUY_ALTO', 'EXTREMO')),
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    -- Token para desuscribir sin login (link en cada correo).
    unsubscribe_token TEXT NOT NULL UNIQUE
        DEFAULT replace(gen_random_uuid()::text, '-', ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_suscriptores_activo
    ON suscriptores(activo) WHERE activo = TRUE;
CREATE INDEX IF NOT EXISTS idx_suscriptores_email ON suscriptores(email);

-- 2. Trigger para mantener updated_at
CREATE OR REPLACE FUNCTION touch_suscriptor_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_suscriptores_updated_at ON suscriptores;
CREATE TRIGGER trg_suscriptores_updated_at
    BEFORE UPDATE ON suscriptores
    FOR EACH ROW EXECUTE FUNCTION touch_suscriptor_updated();

-- 3. RLS: cada usuario solo ve/modifica su propia fila.
--    El ETL (service_role) puede leer todo y registrar entregas.
ALTER TABLE suscriptores ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Suscriptor ve su fila" ON suscriptores;
CREATE POLICY "Suscriptor ve su fila" ON suscriptores
    FOR SELECT USING (auth.uid() = id);

DROP POLICY IF EXISTS "Suscriptor actualiza su fila" ON suscriptores;
CREATE POLICY "Suscriptor actualiza su fila" ON suscriptores
    FOR UPDATE USING (auth.uid() = id) WITH CHECK (auth.uid() = id);

DROP POLICY IF EXISTS "Usuario crea su propia suscripcion" ON suscriptores;
CREATE POLICY "Usuario crea su propia suscripcion" ON suscriptores
    FOR INSERT WITH CHECK (auth.uid() = id);

DROP POLICY IF EXISTS "Service role total" ON suscriptores;
CREATE POLICY "Service role total" ON suscriptores
    FOR ALL USING (auth.role() = 'service_role');

-- 4. Funcion RPC: desuscribir via token (sin login).
--    Llamada desde desuscribir.html con ?token=xxx.
CREATE OR REPLACE FUNCTION unsubscribe_by_token(p_token TEXT)
RETURNS TABLE(email TEXT, success BOOLEAN)
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    v_email TEXT;
BEGIN
    UPDATE suscriptores
    SET activo = FALSE
    WHERE unsubscribe_token = p_token
    RETURNING suscriptores.email INTO v_email;

    IF v_email IS NULL THEN
        RETURN QUERY SELECT NULL::TEXT, FALSE;
    ELSE
        RETURN QUERY SELECT v_email, TRUE;
    END IF;
END;
$$;

GRANT EXECUTE ON FUNCTION unsubscribe_by_token(TEXT) TO anon;

-- 5. Log de correos enviados (auditoria)
CREATE TABLE IF NOT EXISTS correos_enviados (
    id SERIAL PRIMARY KEY,
    suscriptor_id UUID REFERENCES suscriptores(id) ON DELETE SET NULL,
    email TEXT NOT NULL,
    fecha DATE NOT NULL,
    asunto TEXT,
    n_municipios INTEGER,
    n_alertas INTEGER,
    status TEXT NOT NULL DEFAULT 'sent'
        CHECK (status IN ('sent', 'failed', 'bounced')),
    error_msg TEXT,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_correos_enviados_fecha
    ON correos_enviados(fecha, suscriptor_id);

ALTER TABLE correos_enviados ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Service role total correos" ON correos_enviados;
CREATE POLICY "Service role total correos" ON correos_enviados
    FOR ALL USING (auth.role() = 'service_role');

-- 6. Verificacion
SELECT
    'suscriptores' AS tabla, COUNT(*) AS filas FROM suscriptores
UNION ALL
SELECT 'correos_enviados', COUNT(*) FROM correos_enviados;
