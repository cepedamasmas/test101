select
    pedido_id, cliente_id,
    cast(fecha as date) as fecha,
    estado, metodo_pago, total
from {{ source('raw', 'pedidos') }}
