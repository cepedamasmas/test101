# Plan de trabajo - ecomm_parquet: de SFTP a consume

## Contexto

Los archivos Parquet de e-commerce (VTEX, MercadoLibre, Garbarino) están en el SFTP
bajo `/upload/ecomm_parquet/`. Tu trabajo es hacerlos llegar desde ahí hasta tablas
listas para analizar, pasando por todas las capas del data lake.

Trabajás con Claude Code en cada paso. No hace falta saber Python ni la arquitectura
de memoria — Claude conoce el proyecto, vos traés el criterio de negocio y el SQL.

**Referencia obligatoria antes de arrancar:**
- [Guía de modelado dbt](guia_modelado_dbt.md) — qué es dbt, cómo crear modelos, cheat sheet
- [Documentación de fuentes](ecomm_parquet_sources.md) — columnas, filas, período y notas de cada tabla

---

## Las 8 tablas que vas a trabajar

| Carpeta en SFTP | Qué es |
|---|---|
| `vtex_pedido` | Pedidos VTEX (~785k filas, diarios desde 2018) |
| `meli_pedido` | Pedidos MercadoLibre (~858k filas, diarios desde 2018) |
| `meli_pickup` | Retiros en sucursal MeLi (~2k filas, 2019-2020) |
| `meli_shipping` | Ítems de envíos MeLi (~966k filas, archivo único) |
| `type_6` | Detalle logístico de envíos MeLi (~658k filas, diarios desde 2021) |
| `type_7` | Billing info MeLi (~664k filas, archivo único, todo en un JSON) |
| `type_8` | Pagos MercadoPago (~110k filas, diarios desde nov 2024) |
| `garbarino_pedido` | Pedidos Garbarino (~43 filas, ene-mar 2020) |

---

## Paso 1 — Extender el conector SFTP para leer Parquet

El conector SFTP ya sabe leer carpetas de JSONs. Hay que agregrarle soporte para Parquet.

**Decile a Claude:**
> "Necesito extender el SFTPConnector para que `_extract_folder()` también soporte
> `format: 'parquet'`. En vez de `json.load()` tiene que usar `pd.read_parquet()`.
> Mirá el método actual en `connectors/sftp_connector.py` y agregá el soporte."

**Cómo verificar:** Claude va a modificar `sftp_connector.py`. Revisá que el cambio
tenga sentido — debería ser una condición adicional dentro de `_extract_folder()`.

---

## Paso 2 — Registrar las 8 tablas en la configuración

Hay que decirle al pipeline que existen estas nuevas fuentes.

**Decile a Claude:**
> "Agregá las 8 carpetas de ecomm_parquet a `SFTP_FOLDERS` en `config.py` con
> `format: 'parquet'`. El path remoto base es `/upload/ecomm_parquet/`. Las carpetas
> son: `vtex_pedido`, `meli_pedido`, `meli_pickup`, `meli_shipping`, `type_6`,
> `type_7`, `type_8`, `garbarino_pedido`. También registralas en `BASE_RAW_TABLES`."

**Cómo verificar:** `config.py` debería tener 8 entradas nuevas en `SFTP_FOLDERS`
y las mismas 8 en `BASE_RAW_TABLES`.

---

## Paso 3 — Registrar las fuentes en dbt

dbt necesita saber que estas tablas existen para que puedas referenciarlas en tus modelos.

**Decile a Claude:**
> "Agregá las 8 tablas nuevas de ecomm_parquet en `models/sources.yml` bajo el
> source `raw`."

**Cómo verificar:** `sources.yml` debería tener 8 entradas nuevas.

---

## Paso 4 — Ejecutar el pipeline y verificar que los datos llegaron a RAW

Antes de modelar, confirmar que la ingesta funciona.

**Decile a Claude:**
> "¿Cómo puedo verificar en DuckDB que las tablas raw de ecomm_parquet se cargaron
> correctamente después de correr el pipeline?"

Claude te va a dar las queries para verificar. Deberías poder hacer:
```sql
SELECT count(*) FROM raw.vtex_pedido;
SELECT count(*) FROM raw.meli_pedido;
-- etc.
```
Si devuelven filas, la ingesta funcionó. Seguir al paso 5.

---

## Paso 5 — Crear modelos staging

Un modelo por tabla. Empezar por las más simples.

**Orden sugerido:** `meli_pickup` → `meli_shipping` → `garbarino_pedido` →
`meli_pedido` → `vtex_pedido` → `type_6` → `type_8` → `type_7` (la más compleja, ver nota)

Para cada tabla, **decile a Claude:**
> "Quiero crear el modelo staging para `raw.NOMBRE_TABLA`. Mirá las columnas que
> tiene y ayudame a armar el `stg_NOMBRE.sql`. Necesito castear las fechas, deduplicar
> por [ID_PRINCIPAL] y dejar los campos JSON como string por ahora."

Consultá [ecomm_parquet_sources.md](ecomm_parquet_sources.md) para saber:
- Cuál es el ID principal de cada tabla (para dedup)
- Qué columnas son fechas
- Cuáles son campos JSON nested

**Nota especial `type_7`:** Toda la data está en un único campo JSON llamado
`billing_info`. Antes de modelarlo hay que explorar qué tiene adentro:
```sql
SELECT billing_info FROM raw.type_7 LIMIT 3;
```
Coordiná con el equipo qué campos del JSON son necesarios antes de crear el modelo.

---

## Paso 6 — Agregar tests de calidad

Para cada modelo staging que creaste, agregar tests en `_schema.yml`.

**Decile a Claude:**
> "Agregá tests de `not_null` y `unique` para la columna ID de `stg_NOMBRE` en
> el archivo `models/staging/_schema.yml`."

Como mínimo, el ID principal de cada tabla debería tener `not_null` y `unique`.

---

## Paso 7 — Modelos consume

Con el staging andando, crear tablas de análisis cruzando las fuentes.

Algunas ideas de modelos útiles — discutir con el equipo cuáles son prioritarios:

| Modelo | Qué respondería |
|---|---|
| `ventas_por_canal` | Revenue total por plataforma (VTEX / MeLi / Garbarino) por mes |
| `fulfillment_meli` | Para cada pedido MeLi, en qué estado está el envío |
| `conciliacion_pagos_meli` | Cuánto se cobró vs cuánto llegó a la cuenta (fees, etc.) |
| `pickups_vs_envios_meli` | Comparativa de retiros en sucursal vs envíos a domicilio |
| `tiempos_entrega` | Tiempo promedio entre creación del pedido y entrega por canal |

Para cada modelo consume, **decile a Claude:**
> "Quiero crear un modelo consume llamado `NOMBRE` que responda [pregunta de negocio].
> Las tablas staging que necesito cruzar son [stg_x, stg_y]. Ayudame a armar el SQL."

---

## Cómo trabajar con Claude en cada paso

Claude conoce toda la arquitectura del proyecto. Podés pedirle:
- Que escriba el código
- Que explique algo que no entendés
- Que revise algo que ya escribiste
- Que te diga qué queries correr para verificar

**Ejemplos de cómo pedirle cosas:**
- `"Mirá el sftp_connector.py y explicame cómo funciona _extract_folder() antes de modificarlo"`
- `"Corrí dbt run y me tiró este error: [pegar error]. Qué está pasando?"`
- `"Esta query me devuelve nulls en la columna X. Cómo lo arreglo en staging?"`

---

## Checklist de entrega

- [ ] `sftp_connector.py` soporta `format: 'parquet'`
- [ ] 8 tablas en `SFTP_FOLDERS` en `config.py`
- [ ] 8 tablas en `BASE_RAW_TABLES` en `config.py`
- [ ] 8 tablas en `sources.yml`
- [ ] Pipeline corre sin errores y las tablas RAW tienen datos
- [ ] 8 modelos `stg_*.sql` creados en `models/staging/`
- [ ] Tests en `_schema.yml` para cada modelo staging
- [ ] Al menos 1 modelo consume creado y funcionando
- [ ] `dbt test` pasa sin errores críticos
