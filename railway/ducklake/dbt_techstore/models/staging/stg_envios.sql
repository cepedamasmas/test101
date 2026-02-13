select
    envio_id, pedido_id, courier, tracking_code,
    destino_ciudad,
    cast(peso_kg as decimal(6,2)) as peso_kg,
    cast(costo_envio as decimal(10,2)) as costo_envio,
    estado_actual,
    cast(fecha_estimada_entrega as date) as fecha_estimada_entrega
from {{ source('raw', 'envios_courier') }}
