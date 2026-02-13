select
    operacion_mp, pedido_id,
    cast(fecha_operacion as date) as fecha_operacion,
    cast(fecha_liquidacion as date) as fecha_liquidacion,
    cast(monto_bruto as decimal(14,2)) as monto_bruto,
    cast(comision_mp as decimal(10,2)) as comision_mp,
    cast(iva_comision as decimal(10,2)) as iva_comision,
    cast(retencion_iibb as decimal(10,2)) as retencion_iibb,
    cast(monto_neto as decimal(14,2)) as monto_neto,
    estado, cuotas
from {{ source('raw', 'liquidacion_mp') }}
