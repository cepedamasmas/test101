# Ideas: Análisis Geográfico de Ventas x Población

> Fuente de datos: `stg_direccion_geocodificada` (lat/lon, ciudad, provincia, population)
> combinada con `stg_pedido`, `stg_item_pedido`, `stg_cliente`.

---

## 1. Tasa de penetración por localidad

**Métrica**: `pedidos / population` (o `clientes_unicos / population`)

**Qué resuelve**: Encontrar ciudades subatendidas en relación a su tamaño.
Una ciudad de 50.000 habitantes con 10 pedidos tiene mucho más potencial
que una de 5.000 con 8 pedidos.

**Acción**: Priorizar campañas de awareness o descuentos en ciudades con
tasa baja pero población alta. Puede cuantificar el "revenue potencial"
si se igualara a la media nacional.

---

## 2. Evolución de la penetración en el tiempo

**Métrica**: misma tasa pero agregada por mes/trimestre.

**Qué resuelve**: Detectar ciudades en caída libre.
Si en 2022 había 1 pedido cada 500 habitantes y hoy es 1 cada 6.000,
algo pasó: ¿entró un competidor local? ¿falló la logística? ¿subió el precio?
¿cerró un punto de entrega?

**Acción**: Alertas automáticas cuando la tasa de una ciudad cae >X% versus
el trimestre anterior. Disparar una investigación de causa raíz
(soporte, reviews, competencia).

---

## 3. Mercados vírgenes (alta población, cero ventas)

**Métrica**: ciudades con `population > umbral` y `pedidos = 0`
(o `pedidos < percentil_5`).

**Qué resuelve**: Identificar dónde no se está llegando en absoluto.
Puede ser un problema de awareness, cobertura de envío, o simplemente
que nunca se hizo marketing ahí.

**Acción**: Primer experimento = activar publicidad geolocalizada.
Si la respuesta es buena → siguiente paso puede ser un punto de entrega o
una alianza local (kiosco, farmacia) como punto de retiro.

---

## 4. Revenue per cápita vs volumen de pedidos

**Métrica**: `sum(monto_total) / population` y `count(pedidos) / population`
por separado.

**Qué resuelve**: Hay dos perfiles bien distintos:
- Ciudad con muchos pedidos pero ticket bajo → compradores de productos baratos.
- Ciudad con pocos pedidos pero ticket alto → compradores premium.

Estos requieren estrategias opuestas: al primero le convienen bundles/volumen,
al segundo le convienen productos premium y experiencia de compra cuidada.

---

## 5. Dominancia de canal por geografía

**Métrica**: `% de pedidos por canal` (MeLi / VTEX / Garbarino) por provincia
o ciudad.

**Qué resuelve**: Puede pasar que MeLi domine en el interior y VTEX en CABA,
o al revés. Si en una región la penetración total es baja pero MeLi tiene
presencia fuerte, tal vez el problema sea que el site propio (VTEX) no se
conoce ahí.

**Acción**: En ciudades donde MeLi domina, invertir en conversión propia:
cupones de descuento exclusivos del site, mejora de SEO regional, etc.

---

## 6. Distancia al centro de distribución vs penetración

**Métrica**: distancia geográfica desde cada ciudad al depósito principal
(lat/lon fijo del CD) vs tasa de penetración.

**Qué resuelve**: Si hay una correlación fuerte entre "lejos del CD → menos
pedidos", el cuello de botella es logístico (tiempo o costo de envío),
no de demanda. Completamente diferente del caso donde el problema es marketing.

**Acción**: Evaluar si abrir un cross-dock o alianza con operador regional
en zonas alejadas con buena población podría desbloquear demanda latente.

---

## 7. Segmentación de ciudades (cuadrante estratégico)

Cruzar **penetración** (eje X) vs **tamaño de mercado / población** (eje Y):

| Cuadrante | Descripción | Estrategia |
|---|---|---|
| Alto pop + alta penetración | "Vacas lecheras" — ya están | Defender, retención |
| Alto pop + baja penetración | "Gigantes dormidos" | Máxima inversión en captación |
| Bajo pop + alta penetración | "Nichos fieles" | Upsell, LTV |
| Bajo pop + baja penetración | "Sin foco" | Ignorar por ahora |

---

## 8. Estacionalidad geográfica

**Métrica**: índice de estacionalidad por ciudad (`pedidos_mes / promedio_anual`).

**Qué resuelve**: Zonas turísticas o con clima extremo pueden tener picos
muy marcados. Si se detecta que Mar del Plata explota en enero/febrero,
conviene tener stock y logística lista en esa región antes, no después.

**Acción**: Calendario de promociones geolocalizado, pre-posicionamiento de
inventario en depósitos regionales según el patrón histórico.

---

## 9. Retención por geografía

**Métrica**: `% clientes con 2+ pedidos` por ciudad.

**Qué resuelve**: Una ciudad puede tener buena penetración pero pésima retención.
Eso puede indicar que la experiencia post-compra (entrega, calidad, devoluciones)
es peor en esa zona — probablemente problema logístico del último kilómetro.

**Acción**: Detectar ciudades con alta tasa de primer pedido pero baja
repetición → revisar SLA de entrega y tasa de devoluciones en esa zona.

---

## 10. Potencial de revenue incremental

**Métrica**: si una ciudad "dormida" igualara la tasa de penetración promedio
nacional, ¿cuántos pedidos/revenue adicional representaría?

**Qué resuelve**: Convierte el análisis geográfico en un número de negocio
concreto. "Si Córdoba tuviera la misma tasa que CABA, serían 4.200 pedidos
más por mes, ~$18M de revenue adicional."

**Acción**: Prioriza la inversión en captación en las ciudades con mayor
"revenue potencial no realizado", no solo por volumen absoluto.

---

## Stack de implementación sugerido

Todos estos análisis se pueden construir como modelos dbt en `consume/`:

```
consume/
  geo_penetracion_ciudad.sql    -- métricas 1, 3, 7, 10
  geo_evolucion_mensual.sql     -- métrica 2
  geo_canal_dominancia.sql      -- métrica 5
  geo_retencion_ciudad.sql      -- métrica 9
```

Expuestos en PostgreSQL → cualquier BI (Metabase, Grafana, etc.) puede
visualizarlos en mapas con los lat/lon de `stg_direccion_geocodificada`.
