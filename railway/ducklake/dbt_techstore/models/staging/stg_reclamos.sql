select
    reclamo_id, pedido_id, cliente_id,
    cast(fecha_reclamo as date) as fecha_reclamo,
    tipo, estado, prioridad, comentario
from {{ source('raw', 'reclamos') }}
