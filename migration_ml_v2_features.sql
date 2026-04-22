-- ============================================================================
-- Migracion: soportar features adicionales del modelo ML reentrenado con ERA5
-- Ejecutar en Supabase SQL Editor.
--
-- El modelo fue reentrenado con Open-Meteo Archive (ERA5 reanalysis) para los
-- 51 municipios × 10 años. Las mejoras:
--   PR-AUC: 0.017 -> 0.115 (6.8x), Recall: 0.07 -> 0.30 (4.3x)
--   F1: 0.06 -> 0.21 (3.5x), 137 TP en test vs 5 antes
-- El feature importance ahora esta dominado por elevacion (27.5%), lat/lon,
-- ecoregion y dia_del_año. Necesitamos alimentar esos features desde BD.
--
-- Cambios:
--   1. Agregar columna ecoregion (INTEGER 1-5) a municipios.
--   2. Actualizar elevacion_media y ecoregion con valores oficiales INEGI +
--      ERA5 (extraidos del training dataset, coinciden 1:1 con los centroides).
-- ============================================================================

-- 1. Agregar columna ecoregion (si no existe)
ALTER TABLE municipios
    ADD COLUMN IF NOT EXISTS ecoregion INTEGER DEFAULT 1
        CHECK (ecoregion BETWEEN 1 AND 5);

-- 2. Actualizar elevacion_media (Open-Meteo ERA5, metros)
UPDATE municipios SET elevacion_media=510.0 WHERE cve_muni='001';
UPDATE municipios SET elevacion_media=247.0 WHERE cve_muni='002';
UPDATE municipios SET elevacion_media=122.0 WHERE cve_muni='003';
UPDATE municipios SET elevacion_media=447.0 WHERE cve_muni='004';
UPDATE municipios SET elevacion_media=202.0 WHERE cve_muni='005';
UPDATE municipios SET elevacion_media=425.0 WHERE cve_muni='006';
UPDATE municipios SET elevacion_media=2027.0 WHERE cve_muni='007';
UPDATE municipios SET elevacion_media=924.0 WHERE cve_muni='008';
UPDATE municipios SET elevacion_media=287.0 WHERE cve_muni='009';
UPDATE municipios SET elevacion_media=495.0 WHERE cve_muni='010';
UPDATE municipios SET elevacion_media=372.0 WHERE cve_muni='011';
UPDATE municipios SET elevacion_media=408.0 WHERE cve_muni='012';
UPDATE municipios SET elevacion_media=120.0 WHERE cve_muni='013';
UPDATE municipios SET elevacion_media=1924.0 WHERE cve_muni='014';
UPDATE municipios SET elevacion_media=93.0 WHERE cve_muni='015';
UPDATE municipios SET elevacion_media=364.0 WHERE cve_muni='016';
UPDATE municipios SET elevacion_media=2153.0 WHERE cve_muni='017';
UPDATE municipios SET elevacion_media=788.0 WHERE cve_muni='018';
UPDATE municipios SET elevacion_media=651.0 WHERE cve_muni='019';
UPDATE municipios SET elevacion_media=152.0 WHERE cve_muni='020';
UPDATE municipios SET elevacion_media=526.0 WHERE cve_muni='021';
UPDATE municipios SET elevacion_media=241.0 WHERE cve_muni='022';
UPDATE municipios SET elevacion_media=158.0 WHERE cve_muni='023';
UPDATE municipios SET elevacion_media=3029.0 WHERE cve_muni='024';
UPDATE municipios SET elevacion_media=369.0 WHERE cve_muni='025';
UPDATE municipios SET elevacion_media=467.0 WHERE cve_muni='026';
UPDATE municipios SET elevacion_media=156.0 WHERE cve_muni='027';
UPDATE municipios SET elevacion_media=740.0 WHERE cve_muni='028';
UPDATE municipios SET elevacion_media=406.0 WHERE cve_muni='029';
UPDATE municipios SET elevacion_media=1109.0 WHERE cve_muni='030';
UPDATE municipios SET elevacion_media=434.0 WHERE cve_muni='031';
UPDATE municipios SET elevacion_media=307.0 WHERE cve_muni='032';
UPDATE municipios SET elevacion_media=335.0 WHERE cve_muni='033';
UPDATE municipios SET elevacion_media=414.0 WHERE cve_muni='034';
UPDATE municipios SET elevacion_media=195.0 WHERE cve_muni='035';
UPDATE municipios SET elevacion_media=1517.0 WHERE cve_muni='036';
UPDATE municipios SET elevacion_media=711.0 WHERE cve_muni='037';
UPDATE municipios SET elevacion_media=461.0 WHERE cve_muni='038';
UPDATE municipios SET elevacion_media=671.0 WHERE cve_muni='039';
UPDATE municipios SET elevacion_media=162.0 WHERE cve_muni='040';
UPDATE municipios SET elevacion_media=317.0 WHERE cve_muni='041';
UPDATE municipios SET elevacion_media=196.0 WHERE cve_muni='042';
UPDATE municipios SET elevacion_media=1239.0 WHERE cve_muni='043';
UPDATE municipios SET elevacion_media=294.0 WHERE cve_muni='044';
UPDATE municipios SET elevacion_media=570.0 WHERE cve_muni='045';
UPDATE municipios SET elevacion_media=495.0 WHERE cve_muni='046';
UPDATE municipios SET elevacion_media=594.0 WHERE cve_muni='047';
UPDATE municipios SET elevacion_media=1315.0 WHERE cve_muni='048';
UPDATE municipios SET elevacion_media=1462.0 WHERE cve_muni='049';
UPDATE municipios SET elevacion_media=231.0 WHERE cve_muni='050';
UPDATE municipios SET elevacion_media=400.0 WHERE cve_muni='051';

-- 3. Actualizar ecoregion (SEMA NL: R1..R5)
UPDATE municipios SET ecoregion=1 WHERE cve_muni IN
  ('001','002','003','005','006','008','010','011','012','013','015','016','018','020','021',
   '023','025','026','027','028','031','032','034','035','040','041','042','044','045','046','047','050','051');
UPDATE municipios SET ecoregion=2 WHERE cve_muni='037';
UPDATE municipios SET ecoregion=3 WHERE cve_muni IN
  ('004','009','019','022','029','033','038','039','048','049');
UPDATE municipios SET ecoregion=4 WHERE cve_muni IN ('017','030','043');
UPDATE municipios SET ecoregion=5 WHERE cve_muni IN ('007','014','024','036');

-- 4. Verificacion
SELECT
    ecoregion,
    COUNT(*) AS n_munis,
    ROUND(AVG(elevacion_media)::numeric, 0) AS elev_prom,
    ROUND(MIN(elevacion_media)::numeric, 0) AS elev_min,
    ROUND(MAX(elevacion_media)::numeric, 0) AS elev_max
FROM municipios GROUP BY ecoregion ORDER BY ecoregion;
