-- ============================================================================
-- Fix centroides municipales con Marco Geoestadístico INEGI 2025.1
-- Ejecutar en Supabase SQL Editor.
-- Calculado con shapely: usa centroid si cae dentro del polígono,
-- representative_point si el centroid cae fuera (polígonos cóncavos).
-- Precision: 6 decimales (~0.11 m en el ecuador, ~0.10 m en NL).
-- ============================================================================

UPDATE municipios SET lat_centroide=25.940543, lon_centroide=-100.405942
  WHERE cve_muni='001'; -- Abasolo [centroid]
UPDATE municipios SET lat_centroide=26.298712, lon_centroide=-99.703083
  WHERE cve_muni='002'; -- Agualeguas [centroid]
UPDATE municipios SET lat_centroide=26.091506, lon_centroide=-99.27348
  WHERE cve_muni='003'; -- Los Aldamas [centroid]
UPDATE municipios SET lat_centroide=25.301434, lon_centroide=-100.029518
  WHERE cve_muni='004'; -- Allende [centroid]
UPDATE municipios SET lat_centroide=27.342154, lon_centroide=-100.025355
  WHERE cve_muni='005'; -- Anáhuac [centroid]
UPDATE municipios SET lat_centroide=25.792542, lon_centroide=-100.187381
  WHERE cve_muni='006'; -- Apodaca [centroid]
UPDATE municipios SET lat_centroide=24.225118, lon_centroide=-99.886509
  WHERE cve_muni='007'; -- Aramberri [centroid]
UPDATE municipios SET lat_centroide=26.571711, lon_centroide=-100.561792
  WHERE cve_muni='008'; -- Bustamante [centroid]
UPDATE municipios SET lat_centroide=25.524591, lon_centroide=-99.914185
  WHERE cve_muni='009'; -- Cadereyta Jiménez [centroid]
UPDATE municipios SET lat_centroide=25.900771, lon_centroide=-100.356885
  WHERE cve_muni='010'; -- El Carmen [centroid]
UPDATE municipios SET lat_centroide=26.072318, lon_centroide=-99.705374
  WHERE cve_muni='011'; -- Cerralvo [centroid]
UPDATE municipios SET lat_centroide=25.977458, lon_centroide=-100.185436
  WHERE cve_muni='012'; -- Ciénega de Flores [centroid]
UPDATE municipios SET lat_centroide=25.480159, lon_centroide=-98.972409
  WHERE cve_muni='013'; -- China [centroid]
UPDATE municipios SET lat_centroide=23.860108, lon_centroide=-100.306266
  WHERE cve_muni='014'; -- Doctor Arroyo [centroid]
UPDATE municipios SET lat_centroide=25.964091, lon_centroide=-99.030873
  WHERE cve_muni='015'; -- Doctor Coss [centroid]
UPDATE municipios SET lat_centroide=25.849252, lon_centroide=-99.80498
  WHERE cve_muni='016'; -- Doctor González [centroid]
UPDATE municipios SET lat_centroide=24.760509, lon_centroide=-100.392287
  WHERE cve_muni='017'; -- Galeana [centroid]
UPDATE municipios SET lat_centroide=25.809011, lon_centroide=-100.659777
  WHERE cve_muni='018'; -- García [centroid]
UPDATE municipios SET lat_centroide=25.644597, lon_centroide=-100.374758
  WHERE cve_muni='019'; -- San Pedro Garza García [centroid]
UPDATE municipios SET lat_centroide=25.803315, lon_centroide=-98.848418
  WHERE cve_muni='020'; -- General Bravo [centroid]
UPDATE municipios SET lat_centroide=25.821867, lon_centroide=-100.355575
  WHERE cve_muni='021'; -- General Escobedo [centroid]
UPDATE municipios SET lat_centroide=25.275874, lon_centroide=-99.413017
  WHERE cve_muni='022'; -- General Terán [centroid]
UPDATE municipios SET lat_centroide=26.212648, lon_centroide=-99.445967
  WHERE cve_muni='023'; -- General Treviño [centroid]
UPDATE municipios SET lat_centroide=23.901029, lon_centroide=-99.740174
  WHERE cve_muni='024'; -- General Zaragoza [centroid]
UPDATE municipios SET lat_centroide=25.911392, lon_centroide=-100.13482
  WHERE cve_muni='025'; -- General Zuazua [centroid]
UPDATE municipios SET lat_centroide=25.67276, lon_centroide=-100.205599
  WHERE cve_muni='026'; -- Guadalupe [centroid]
UPDATE municipios SET lat_centroide=25.916079, lon_centroide=-99.41424
  WHERE cve_muni='027'; -- Los Herreras [centroid]
UPDATE municipios SET lat_centroide=26.033168, lon_centroide=-99.997508
  WHERE cve_muni='028'; -- Higueras [centroid]
UPDATE municipios SET lat_centroide=24.883791, lon_centroide=-99.678096
  WHERE cve_muni='029'; -- Hualahuises [centroid]
UPDATE municipios SET lat_centroide=24.638418, lon_centroide=-99.848716
  WHERE cve_muni='030'; -- Iturbide [centroid]
UPDATE municipios SET lat_centroide=25.614087, lon_centroide=-100.121406
  WHERE cve_muni='031'; -- Juárez [centroid]
UPDATE municipios SET lat_centroide=27.050841, lon_centroide=-100.418216
  WHERE cve_muni='032'; -- Lampazos de Naranjo [centroid]
UPDATE municipios SET lat_centroide=24.851771, lon_centroide=-99.52922
  WHERE cve_muni='033'; -- Linares [centroid]
UPDATE municipios SET lat_centroide=25.886143, lon_centroide=-100.023315
  WHERE cve_muni='034'; -- Marín [representative_point]
UPDATE municipios SET lat_centroide=26.048931, lon_centroide=-99.493516
  WHERE cve_muni='035'; -- Melchor Ocampo [centroid]
UPDATE municipios SET lat_centroide=23.417917, lon_centroide=-100.160408
  WHERE cve_muni='036'; -- Mier y Noriega [centroid]
UPDATE municipios SET lat_centroide=26.285584, lon_centroide=-100.786277
  WHERE cve_muni='037'; -- Mina [centroid]
UPDATE municipios SET lat_centroide=25.126305, lon_centroide=-99.808381
  WHERE cve_muni='038'; -- Montemorelos [centroid]
UPDATE municipios SET lat_centroide=25.64464, lon_centroide=-100.310952
  WHERE cve_muni='039'; -- Monterrey [centroid]
UPDATE municipios SET lat_centroide=26.583042, lon_centroide=-99.601554
  WHERE cve_muni='040'; -- Parás [centroid]
UPDATE municipios SET lat_centroide=25.736769, lon_centroide=-99.977135
  WHERE cve_muni='041'; -- Pesquería [centroid]
UPDATE municipios SET lat_centroide=25.65331, lon_centroide=-99.587667
  WHERE cve_muni='042'; -- Los Ramones [centroid]
UPDATE municipios SET lat_centroide=25.065823, lon_centroide=-100.127733
  WHERE cve_muni='043'; -- Rayones [centroid]
UPDATE municipios SET lat_centroide=26.575102, lon_centroide=-100.149288
  WHERE cve_muni='044'; -- Sabinas Hidalgo [centroid]
UPDATE municipios SET lat_centroide=26.161052, lon_centroide=-100.270385
  WHERE cve_muni='045'; -- Salinas Victoria [centroid]
UPDATE municipios SET lat_centroide=25.736076, lon_centroide=-100.270693
  WHERE cve_muni='046'; -- San Nicolás de los Garza [centroid]
UPDATE municipios SET lat_centroide=25.999402, lon_centroide=-100.45319
  WHERE cve_muni='047'; -- Hidalgo [centroid]
UPDATE municipios SET lat_centroide=25.57461, lon_centroide=-100.483863
  WHERE cve_muni='048'; -- Santa Catarina [centroid]
UPDATE municipios SET lat_centroide=25.384897, lon_centroide=-100.237323
  WHERE cve_muni='049'; -- Santiago [centroid]
UPDATE municipios SET lat_centroide=26.648106, lon_centroide=-99.884763
  WHERE cve_muni='050'; -- Vallecillo [centroid]
UPDATE municipios SET lat_centroide=26.469705, lon_centroide=-100.34807
  WHERE cve_muni='051'; -- Villaldama [centroid]

-- Verificación: los 51 municipios deben quedar con nuevas coords
SELECT cve_muni, nombre, lat_centroide, lon_centroide FROM municipios ORDER BY cve_muni;