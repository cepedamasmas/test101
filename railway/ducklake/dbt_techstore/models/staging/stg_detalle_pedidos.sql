select
    detalle_id, pedido_id, producto_id,
    cantidad, precio_unitario, descuento, subtotal
from {{ source('raw', 'detalle_pedidos') }}
where cantidad > 0 and subtotal > 0
