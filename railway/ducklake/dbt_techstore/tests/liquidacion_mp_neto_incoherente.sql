-- TEST: Liquidaciones de MercadoPago donde el monto_neto no cierra
--
-- Verifica que: monto_neto = monto_bruto - comision_mp - iva_comision - retencion_iibb
-- Permite hasta $0.10 de diferencia por redondeos internos de MercadoPago.
--
-- Si falla: la estructura de la liquidación tiene retenciones o ajustes
-- adicionales que no estamos modelando en staging.

select
    operacion_mp,
    pedido_id,
    monto_bruto,
    comision_mp,
    iva_comision,
    retencion_iibb,
    monto_neto,
    round(monto_bruto - comision_mp - iva_comision - retencion_iibb, 2) as neto_calculado,
    round(abs(monto_neto - (monto_bruto - comision_mp - iva_comision - retencion_iibb)), 2) as diferencia
from {{ ref('stg_liquidacion_mp') }}
where abs(monto_neto - (monto_bruto - comision_mp - iva_comision - retencion_iibb)) > 0.10
