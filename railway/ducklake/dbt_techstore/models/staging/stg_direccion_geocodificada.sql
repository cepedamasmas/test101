{{ config(materialized='table') }}

-- Dimensión de direcciones enriquecida con geocoordenadas.
-- Base: stg_direccion (todos los campos originales).
-- Enriquecimiento: stg_geo_coords (lat/lon del archivo de geocodificación).
-- Lógica de coordenadas:
--   - COALESCE: usa lat/lon de la dirección original cuando existe (MeLi),
--     si no, las del archivo de geocodificación.
-- matched_city / matched_provincia / population / match_type: solo vienen de geo_coords.

SELECT
    d.direccion_id,
    d.calle,
    d.altura,
    d.complemento,
    d.ciudad,
    d.provincia,
    d.cp,
    d.pais,
    COALESCE(NULLIF(d.latitud,  0), g.latitud)  AS latitud,
    COALESCE(NULLIF(d.longitud, 0), g.longitud) AS longitud,
    d.fuente,
    g.matched_city,
    g.matched_provincia,
    g.population,
    g.match_type
FROM {{ ref('stg_direccion') }} d
LEFT JOIN {{ ref('stg_geo_coords') }} g ON g.direccion_id = d.direccion_id
