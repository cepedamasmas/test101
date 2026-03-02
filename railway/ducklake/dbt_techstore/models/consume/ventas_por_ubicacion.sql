{{ config(materialized='table') }}

-- CU-003: Análisis geográfico de clientes y ventas.
-- Una fila por (ciudad, provincia, pais_codigo).
-- Resolución de ubicación principal por cliente: RN-005 / RN-010.
--   → se toma la ubicación con fecha_ultimo_pedido más reciente por cliente.
-- Toda la actividad del cliente se atribuye a su ubicación principal.
-- Enriquecido con lat/lon (stg_ubicacion_geo) y población (stg_direccion_geocodificada).

WITH ubicacion_principal AS (
    -- RN-005/010: una única ubicación principal por cliente
    SELECT cliente_id, ubicacion_id
    FROM {{ ref('stg_cliente_ubicacion_geo') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cliente_id
        ORDER BY fecha_ultimo_pedido DESC NULLS LAST
    ) = 1
),

-- Población por ciudad desde stg_direccion_geocodificada.
poblacion_por_ciudad AS (
    SELECT
        LOWER(ciudad)   AS ciudad_key,
        MAX(population) AS population
    FROM {{ ref('stg_direccion_geocodificada') }}
    WHERE population IS NOT NULL
      AND ciudad IS NOT NULL
    GROUP BY LOWER(ciudad)
),

-- Resolución antes del GROUP BY: evita filas duplicadas por distintos
-- ubicacion_id (distintos CP) de la misma ciudad con distinto lat/lon.
-- Las coordenadas ya vienen enriquecidas desde stg_ubicacion_geo.
base AS (
    SELECT
        ug.ciudad,
        ug.provincia,
        ug.pais_codigo,
        ug.latitud,
        ug.longitud,
        pop.population,
        up.cliente_id,
        cp.pedido_id,
        cp.monto_total
    FROM ubicacion_principal up
    JOIN {{ ref('stg_ubicacion_geo') }} ug
        ON ug.ubicacion_id = up.ubicacion_id
    LEFT JOIN {{ ref('stg_cliente_pedido') }} cp
        ON cp.cliente_id = up.cliente_id
        -- RN-002: solo pedidos con ítems asociados
        AND EXISTS (
            SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
            WHERE ip.pedido_id = cp.pedido_id
        )
    LEFT JOIN poblacion_por_ciudad pop
        ON LOWER(ug.ciudad) = pop.ciudad_key
    WHERE ug.ciudad IS NOT NULL
)

SELECT
    ciudad,
    provincia,
    pais_codigo,
    AVG(latitud)                        AS latitud,
    AVG(longitud)                       AS longitud,
    MAX(population)                     AS population,
    COUNT(DISTINCT cliente_id)          AS clientes_unicos,
    COUNT(DISTINCT pedido_id)           AS total_pedidos,
    SUM(monto_total)                    AS revenue_total,
    AVG(monto_total)                    AS ticket_promedio
FROM base
GROUP BY ciudad, provincia, pais_codigo
ORDER BY revenue_total DESC NULLS LAST
