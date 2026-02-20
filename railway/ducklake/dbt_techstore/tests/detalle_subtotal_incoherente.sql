-- TEST: Líneas de detalle donde el subtotal no cierra matemáticamente
--
-- Verifica que: subtotal ≈ (cantidad * precio_unitario) - descuento
-- Permite hasta $1 de tolerancia para cubrir redondeos de la fuente.
--
-- Si falla: hay corrupción de datos en la tabla detalle_pedidos de MySQL,
-- o la fórmula del descuento es diferente a lo que asumimos.

select
    detalle_id,
    pedido_id,
    cantidad,
    precio_unitario,
    descuento,
    subtotal,
    round((cantidad * precio_unitario) - coalesce(descuento, 0), 2) as subtotal_calculado,
    round(abs(subtotal - ((cantidad * precio_unitario) - coalesce(descuento, 0))), 2) as diferencia
from {{ ref('stg_detalle_pedidos') }}
where abs(subtotal - ((cantidad * precio_unitario) - coalesce(descuento, 0))) > 1
