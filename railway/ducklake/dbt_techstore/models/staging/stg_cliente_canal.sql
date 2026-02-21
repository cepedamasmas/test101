{{ config(materialized='table') }}

-- Bridge: qué canales usa cada cliente, con métricas de actividad.
-- Depende de stg_cliente_pedido (ya tiene la relación cliente-canal) y stg_canal.

WITH base AS (
    SELECT
        cliente_id,
        canal,
        MIN(fecha_pedido)   AS primer_pedido,
        MAX(fecha_pedido)   AS ultimo_pedido,
        COUNT(*)            AS total_pedidos,
        SUM(monto_total)    AS monto_total_acumulado
    FROM {{ ref('stg_cliente_pedido') }}
    WHERE cliente_id IS NOT NULL
    GROUP BY cliente_id, canal
)

SELECT
    b.cliente_id,
    c.canal_id,
    b.canal,
    b.primer_pedido,
    b.ultimo_pedido,
    b.total_pedidos,
    b.monto_total_acumulado
FROM base b
JOIN {{ ref('stg_canal') }} c ON c.canal = b.canal
