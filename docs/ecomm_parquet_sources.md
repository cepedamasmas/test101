# Fuentes ecomm_parquet - Documentación de tablas RAW

Datos exportados tal cual desde el cliente. Cada carpeta es una tabla en la capa RAW.
**No se modificó nada del origen** - lo que está acá es exactamente lo que mandó el cliente.

---

## Resumen general

| Tabla | Filas totales | Archivos | Período | IDs únicos |
|---|---|---|---|---|
| `vtex_pedido` | 785,593 | 2069 (diarios) | dic 2018 → feb 2026 | 785,553 |
| `meli_pedido` | 858,011 | 2285 (diarios) | sep 2018 → feb 2026 | 857,621 |
| `meli_pickup` | 1,980 | 290 (diarios) | abr 2019 → mar 2020 | 1,980 |
| `meli_shipping` | 966,534 | 1 (único) | sin fecha | 847,393 órdenes |
| `type_6` | 658,833 | 1722 (diarios) | jun 2021 → feb 2026 | 638,253 |
| `type_7` | 664,659 | 1 (único) | sin fecha | — |
| `type_8` | 109,999 | 440 (diarios) | nov 2024 → feb 2026 | 109,990 |
| `garbarino_pedido` | 43 | 25 (diarios) | ene 2020 → mar 2020 | 41 |

---

## Tablas con nombre descriptivo

### `vtex_pedido`
Pedidos procesados por la plataforma VTEX. Un archivo por día desde diciembre 2018.

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `orderId` | string | ID único del pedido en VTEX (ej: `914591295930-01`) |
| `status` | string | Estado del pedido (`invoiced`, `canceled`, etc.) |
| `value` | int | Valor total en centavos |
| `creationDate` | string | Fecha de creación (castear a timestamp en staging) |
| `lastChange` | string | Última modificación |
| `items` | string (JSON) | Lista de ítems del pedido (objeto nested) |
| `clientProfileData` | string (JSON) | Datos del cliente (nested) |
| `shippingData` | string (JSON) | Datos de envío (nested) |
| `paymentData` | string (JSON) | Datos de pago (nested) |
| `totals` | string (JSON) | Desglose de totales (nested) |

**Notas:**
- Tiene un archivo `vtex_pedido_unknown.parquet` sin fecha - revisar en staging
- La mayoría de columnas son objetos JSON serializados como string. El trabajo del staging es decidir qué extraer de cada uno.
- 40 duplicados aproximados (785,593 filas vs 785,553 IDs únicos)

---

### `meli_pedido`
Pedidos de MercadoLibre. El dataset más grande, cubre desde 2018 hasta hoy.

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `id` | string | ID único del pedido en MeLi |
| `status` | string | Estado (`paid`, `cancelled`, etc.) |
| `total_amount` | int | Monto total |
| `paid_amount` | int | Monto efectivamente pagado |
| `date_created` | string | Fecha de creación |
| `date_closed` | string | Fecha de cierre |
| `last_updated` | string | Última actualización |
| `order_items` | string (JSON) | Ítems del pedido (nested) |
| `payments` | string (JSON) | Pagos asociados (nested) |
| `shipping` | string (JSON) | Datos de envío (nested) |
| `buyer` | string (JSON) | Datos del comprador (nested) |
| `seller` | string (JSON) | Datos del vendedor (nested) |

**Notas:**
- ~390 duplicados (858,011 filas vs 857,621 IDs únicos) - dedup por `id` en staging
- `currency_id` presente para multi-moneda

---

### `meli_pickup`
Órdenes de retiro en sucursal de MercadoLibre. Dataset acotado (2019-2020).

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `id` | string | ID único del pickup |
| `order_id` | int | ID de la orden MeLi asociada |
| `status` | string | Estado del pickup |
| `date_created` | string | Fecha de creación |
| `date_ready` | string | Fecha en que estuvo listo para retirar |
| `store_id` | int | ID de la tienda |
| `store_info` | string (JSON) | Info de la tienda (nested) |
| `pickup_person` | string (JSON) | Info de quien retira (nested) |
| `buyer_id` | string | ID del comprador |

**Notas:**
- Dataset relativamente chico (1,980 filas), sin duplicados
- Solo cubre hasta marzo 2020 - posiblemente descontinuado o migrado

---

### `meli_shipping`
Ítems de envíos de MercadoLibre. Archivo único con toda la historia (no hay partición diaria).

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `order_id` | string | ID de la orden MeLi asociada |
| `item_id` | string | ID del ítem |
| `variation_id` | string | ID de variante del ítem |
| `description` | string | Descripción del ítem |
| `quantity` | float | Cantidad |
| `dimensions` | string (JSON) | Dimensiones del paquete (nested) |
| `sender_id` | string | ID del vendedor/remitente |
| `manufacturing_time` | string | Tiempo de fabricación |

**Notas:**
- 966,534 filas pero solo 847,393 `order_id` únicos - una orden puede tener múltiples ítems
- Archivo único, no tiene partición por fecha
- Es la tabla de ítems de envío, **complementa `meli_pedido`** via `order_id`

---

### `garbarino_pedido`
Pedidos de Garbarino. Dataset muy pequeño, solo cubre ene-mar 2020.

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `id` | int | ID único del pedido |
| `cart_id` | int | ID del carrito de origen |
| `created` | string | Fecha de creación |
| `status` | string | Estado del pedido |
| `shipping_type` | string | Tipo de envío |
| `company` | string | Empresa/sucursal |
| `customer` | string (JSON) | Datos del cliente (nested) |
| `sold_items` | string (JSON) | Ítems vendidos (nested) |
| `totals_sale` | string (JSON) | Totales de la venta (nested) |
| `billing_address` | string (JSON) | Dirección de facturación (nested) |

**Notas:**
- Solo 43 filas en total - dataset de prueba o período muy acotado
- 2 duplicados (`id` tiene 41 únicos vs 43 filas)

---

## Tablas sin nombre descriptivo

Estas tres tablas llegaron con nombre genérico del cliente. Por las columnas se puede inferir qué son.

### `type_6` → probable: `meli_envio_detalle`
Detalle de envíos de MercadoLibre. Columnas de tracking, logística y dirección.

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `id` | int | ID único del envío |
| `order_id` | string | ID de la orden MeLi asociada |
| `status` | string | Estado del envío |
| `substatus` | string | Sub-estado |
| `logistic_type` | string | Tipo de logística (`fulfillment`, `drop_off`, etc.) |
| `tracking_number` | string | Número de tracking |
| `tracking_method` | string | Método de tracking |
| `date_created` | string | Fecha de creación |
| `last_updated` | string | Última actualización |
| `sender_address` | string (JSON) | Dirección del remitente (nested) |
| `receiver_address` | string (JSON) | Dirección del destinatario (nested) |
| `shipping_option` | string (JSON) | Opción de envío elegida (nested) |
| `cost_components` | string (JSON) | Desglose de costos de envío (nested) |

**Notas:**
- Complementa `meli_pedido` y `meli_shipping` via `order_id`
- ~20,580 duplicados (`id`: 638,253 únicos vs 658,833 filas) - dedup necesario en staging
- Cubre 2021-2026, no 2018-2020

---

### `type_7` → probable: `meli_billing_info`
Información de facturación. Archivo único, columna única.

**Columnas:**
| Columna | Tipo | Descripción |
|---|---|---|
| `billing_info` | string (JSON) | Objeto JSON completo de facturación (todo nested) |

**Notas:**
- 664,659 filas, toda la data está en un único campo JSON
- El trabajo de staging es parsear `billing_info` y extraer los campos relevantes
- Sin columna ID explícita - habrá que extraerla del JSON
- **Requiere análisis más profundo** para entender la estructura del JSON antes de modelar

---

### `type_8` → probable: `meli_pago`
Pagos procesados por MercadoPago. Dataset más reciente (nov 2024 en adelante).

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `id` | int | ID único del pago |
| `order_id` | string | ID de la orden MeLi asociada |
| `status` | string | Estado del pago (`approved`, `rejected`, etc.) |
| `status_detail` | string | Detalle del estado |
| `date_created` | string | Fecha de creación |
| `date_approved` | string | Fecha de aprobación |
| `transaction_amount` | float | Monto de la transacción |
| `total_paid_amount` | float | Monto total pagado |
| `net_received_amount` | float | Monto neto recibido |
| `mercadopago_fee` | int | Comisión de MercadoPago |
| `marketplace_fee` | int | Comisión del marketplace |
| `payment_type` | string | Tipo de pago (`credit_card`, etc.) |
| `payment_method_id` | string | Método de pago |
| `installments` | int | Cuotas |
| `currency_id` | string | Moneda |
| `payer` | string (JSON) | Datos del pagador (nested) |

**Notas:**
- 9 duplicados (`id`: 109,990 únicos vs 109,999 filas)
- Solo cubre nov 2024 en adelante - posiblemente nuevo sistema de pagos
- Vincula con `meli_pedido` via `order_id`

---

## Relaciones entre tablas

```
meli_pedido (id)
    ├── meli_shipping    (order_id) → ítems del envío
    ├── type_6           (order_id) → detalle logístico del envío
    └── type_8           (order_id) → pagos del pedido

vtex_pedido    → independiente (otra plataforma)
meli_pickup    → independiente (retiros en sucursal)
garbarino_pedido → independiente (otra plataforma)
type_7         → billing info (relación a confirmar)
```

---

## Consideraciones para staging

1. **Dedup obligatorio** en todas las tablas que tienen duplicados
2. **Casteo de fechas**: todas las fechas llegan como string, hay que castear a `timestamp` o `date`
3. **Campos nested (JSON)**: la mayoría de columnas complejas son strings con JSON. Decidir en staging qué extraer según necesidad del negocio
4. **`type_7`**: requiere análisis previo del JSON antes de poder modelar - no hay columnas claras
5. **`type_6`, `type_7`, `type_8`**: confirmar nombres reales con el cliente antes de crear los modelos staging definitivos
