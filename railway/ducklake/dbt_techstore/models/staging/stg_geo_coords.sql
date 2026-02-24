{{ config(materialized='table') }}

-- Coordenadas geocodificadas desde coords_enriched.csv.
-- Tipado limpio de todas las columnas; dedup por direccion_id (más completo primero).

SELECT
    direccion_id::VARCHAR                           AS direccion_id,
    TRY_CAST(latitud            AS DOUBLE)          AS latitud,
    TRY_CAST(longitud           AS DOUBLE)          AS longitud,
    matched_city::VARCHAR                           AS matched_city,
    matched_provincia::VARCHAR                      AS matched_provincia,
    TRY_CAST(population         AS INTEGER)         AS population,
    match_type::VARCHAR                             AS match_type
FROM {{ source('raw', 'geo_coords') }}
WHERE direccion_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY direccion_id
    ORDER BY TRY_CAST(latitud AS DOUBLE) NULLS LAST
) = 1
