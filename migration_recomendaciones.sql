-- ============================================================================
-- Migracion: recomendaciones operativas por nivel de riesgo
-- Ejecutar en Supabase SQL Editor.
--
-- El ETL consume esta tabla al enviar el correo diario y pega la recomendacion
-- correspondiente debajo de cada bloque "EXTREMO (1): Mina", etc., para que
-- los tomadores de decisiones tengan una accion concreta por nivel sin
-- necesidad de buscarla en otra parte.
--
-- Editable desde Supabase SQL Editor sin redeploy: los cambios se reflejan
-- en el siguiente envio diario (6:00 AM CST) o en cualquier workflow_dispatch.
-- ============================================================================

CREATE TABLE IF NOT EXISTS recomendaciones_nivel (
    nivel TEXT PRIMARY KEY
        CHECK (nivel IN ('BAJO','MEDIO','ALTO','MUY_ALTO','EXTREMO')),
    recomendacion TEXT NOT NULL,
    orden INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Lectura publica (util si el dashboard quiere mostrarlas en el futuro);
-- escritura solo service_role.
ALTER TABLE recomendaciones_nivel ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Lectura publica recomendaciones" ON recomendaciones_nivel;
CREATE POLICY "Lectura publica recomendaciones" ON recomendaciones_nivel
    FOR SELECT USING (TRUE);

DROP POLICY IF EXISTS "Solo admin escritura recomendaciones" ON recomendaciones_nivel;
CREATE POLICY "Solo admin escritura recomendaciones" ON recomendaciones_nivel
    FOR ALL USING (auth.role() = 'service_role');

-- Trigger de updated_at
CREATE OR REPLACE FUNCTION touch_recomendacion_updated()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_recomendaciones_updated_at ON recomendaciones_nivel;
CREATE TRIGGER trg_recomendaciones_updated_at
    BEFORE UPDATE ON recomendaciones_nivel
    FOR EACH ROW EXECUTE FUNCTION touch_recomendacion_updated();

-- Textos iniciales (se pueden editar con UPDATE sin redeploy)
INSERT INTO recomendaciones_nivel (nivel, recomendacion, orden) VALUES
('EXTREMO', 'Vigilancia activa las 24 horas. Las condiciones son ideales para que inicie y se propague fuego rapidamente. Restringir toda quema, coordinar con Proteccion Civil municipal y mantener brigadas en posicion avanzada. Considerar alertas preventivas a la poblacion rural.', 1),
('MUY_ALTO', 'Mantener comunicacion permanente con estos municipios y tener protocolos de respuesta rapida listos. Posicionar brigadas preventivamente en coordinacion con CONAFOR; intensificar vigilancia satelital y terrestre en zonas forestales y de interfaz urbano-forestal.', 2),
('ALTO', 'Alerta preventiva activada. Verificar disponibilidad de recursos (vehiculos, personal de brigadas, equipo de combate). Sensibilizar a la poblacion sobre la prohibicion de quemas y reforzar patrullas en areas historicamente criticas.', 3),
('MEDIO', 'Monitorear condiciones meteorologicas durante el dia. Las actividades agricolas de quema requieren extrema precaucion y solo deben realizarse sin viento fuerte. Mantener informadas a las brigadas y la poblacion local.', 4),
('BAJO', 'Monitoreo rutinario. El riesgo es bajo pero la vigilancia cotidiana se mantiene activa. Continuar con actividades de prevencion: limpieza de brechas corta-fuego, sensibilizacion comunitaria y mantenimiento de infraestructura.', 5)
ON CONFLICT (nivel) DO UPDATE
SET recomendacion = EXCLUDED.recomendacion,
    orden = EXCLUDED.orden;

-- Verificacion
SELECT orden, nivel, LEFT(recomendacion, 80) || '...' AS preview FROM recomendaciones_nivel ORDER BY orden;
