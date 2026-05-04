-- ============================================================================
-- Insertar contactos de prueba para alertas WhatsApp
-- Ejecutar en Supabase SQL Editor
-- ============================================================================
-- IMPORTANTE: Reemplaza los numeros de telefono con los reales.
-- El formato debe ser: +521234567890 (codigo de pais + numero)
-- Para WhatsApp sandbox de Twilio, ambos numeros deben haber enviado
-- "join <palabra-clave>" al numero del sandbox primero.

-- Contacto 1: plantilla institucional
INSERT INTO contactos (municipio_id, nombre, cargo, email, telefono, canal_pref, activo, notas)
VALUES (
    NULL,  -- NULL = recibe resumen de TODOS los municipios
    'Nombre Apellido',
    'cargo',
    'correo@ejemplo.org',
    '+52XXXXXXXXXX',  -- ← Pon el numero real aqui
    'whatsapp',
    true,
    'Contacto de prueba — recibe resumen diario'
);

-- Contacto 2: Segundo contacto de prueba
INSERT INTO contactos (municipio_id, nombre, cargo, email, telefono, canal_pref, activo, notas)
VALUES (
    NULL,  -- NULL = recibe resumen de TODOS los municipios
    'Contacto Prueba 2',
    'Cargo',
    'correo@ejemplo.com',
    '+52XXXXXXXXXX',  -- ← Pon el numero real aqui
    'whatsapp',
    true,
    'Contacto de prueba 2'
);

-- Verificar
SELECT id, nombre, telefono, canal_pref, activo FROM contactos WHERE activo = true;
