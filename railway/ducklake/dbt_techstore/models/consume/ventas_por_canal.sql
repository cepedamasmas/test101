{{ config(materialized='table') }}

-- KPIs de ventas agregados por canal y provincia.
-- Métricas: pedidos, revenue, ticket promedio, clientes únicos, unidades vendidas (KPI-004).

WITH ubicacion_principal AS (
    SELECT cliente_id, ubicacion_id
    FROM {{ ref('stg_cliente_ubicacion_geo') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cliente_id
        ORDER BY fecha_ultimo_pedido DESC NULLS LAST
    ) = 1
),

pedidos_validos AS (
    SELECT
        p.canal,
        p.pedido_id,
        p.monto_total,
        p.fecha_creacion,
        cp.cliente_id,
        UPPER(replace(replace(replace(replace(replace(
               replace(replace(replace(replace(replace(
               replace(replace(replace(replace(replace(
               replace(replace(replace(replace(replace(
                   ug.provincia,
                   '\U00C1','a'),'\U00C9','e'),'\U00CD','i'),'\U00D3','o'),'\U00DA','u'),
                   '\U00E1','a'),'\U00E9','e'),'\U00ED','i'),'\U00F3','o'),'\U00FA','u'),
                   '\u00c1','a'),'\u00c9','e'),'\u00cd','i'),'\u00d3','o'),'\u00da','u'),
                   '\u00e1','a'),'\u00e9','e'),'\u00ed','i'),'\u00f3','o'),'\u00fa','u'))
                                                         AS provincia
    FROM {{ ref('stg_pedido') }} p
    LEFT JOIN {{ ref('stg_cliente_pedido') }} cp ON cp.pedido_id = p.pedido_id
    LEFT JOIN ubicacion_principal up ON up.cliente_id = cp.cliente_id
    LEFT JOIN {{ ref('stg_ubicacion_geo') }} ug ON ug.ubicacion_id = up.ubicacion_id
    WHERE p.monto_total > 0
      -- RN-002: excluir pedidos sin ítems asociados
      AND EXISTS (
          SELECT 1 FROM {{ ref('stg_item_pedido') }} ip
          WHERE ip.pedido_id = p.pedido_id
      )
),

por_canal AS (
    SELECT
        canal,
        provincia,
        COUNT(DISTINCT pedido_id)   AS total_pedidos,
        COUNT(DISTINCT cliente_id)  AS clientes_unicos,
        SUM(monto_total)            AS revenue_total,
        AVG(monto_total)            AS ticket_promedio,
        MIN(fecha_creacion)         AS primer_pedido,
        MAX(fecha_creacion)         AS ultimo_pedido
    FROM pedidos_validos
    GROUP BY canal, provincia
),

-- KPI-004: unidades vendidas por canal y provincia
unidades AS (
    SELECT pv.canal, pv.provincia, SUM(ip.cantidad) AS total_unidades
    FROM pedidos_validos pv
    JOIN {{ ref('stg_item_pedido') }} ip ON ip.pedido_id = pv.pedido_id
    GROUP BY pv.canal, pv.provincia
)

SELECT
    pc.canal,
    pc.provincia,
    pc.total_pedidos,
    pc.clientes_unicos,
    pc.revenue_total,
    pc.ticket_promedio,
    u.total_unidades,
    pc.primer_pedido,
    pc.ultimo_pedido
FROM por_canal pc
LEFT JOIN unidades u ON u.canal = pc.canal AND u.provincia = pc.provincia
ORDER BY pc.revenue_total DESC
