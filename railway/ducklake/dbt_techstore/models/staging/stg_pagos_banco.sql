select
    pago_id, pedido_id,
    cast(fecha_pago as date) as fecha_pago,
    cast(monto as decimal(14,2)) as monto,
    banco, estado,
    cast(comision_bancaria as decimal(10,2)) as comision_bancaria,
    tipo
from {{ source('raw', 'pagos_banco') }}
