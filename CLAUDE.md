# DuckLake - Contexto para Claude Code

## Overview

DuckLake es un data lake config-driven usando DuckDB + Parquet + dbt.
Alternativa a Snowflake/Databricks para empresas con 100GB-10TB de datos.
Deployado en Railway con Dagster como orquestador y UI web.

## Arquitectura: Medallion (3 capas)

- **RAW (Bronze)**: Datos crudos 1:1, append-only, particionados por fecha
  - Path: `data/raw/{source}/{table}/year=YYYY/month=MM/day=DD/data.parquet`
  - Ingesta via Python (connectors)
- **STAGING (Silver)**: Limpieza, dedup, validación
  - Transformaciones via **dbt** (modelos `stg_*.sql`)
  - Materializado como tablas en DuckDB schema `staging`
- **CONSUME (Gold)**: Tablas listas para BI/ML
  - Agregaciones via **dbt** (modelos en `consume/`)
  - Materializado como tablas en DuckDB schema `consume`

## Stack

Python 3.11+, DuckDB 1.2, dbt-duckdb 1.9, Dagster 1.9, PyArrow, Pandas, Paramiko, psycopg2

## Estructura del proyecto

```
railway/ducklake/              # Pipeline principal (deploy en Railway)
├── config.py                  # Configs centralizadas (env vars)
├── connectors/                # Ingesta de fuentes
│   ├── base.py                # BaseConnector (interfaz abstracta, 2 patrones)
│   └── sftp_connector.py      # SFTPConnector.extract() — generator de (tabla, path_parquet)
├── layers/
│   └── raw.py                 # RawLayer.save() y save_from_path() (append-only, particionado)
├── dbt_techstore/             # Proyecto dbt (staging + consume)
│   ├── models/staging/        # 10 modelos stg_*.sql con tests
│   └── models/consume/        # 4 modelos de agregación con tests
├── dagster_pipeline/          # Dagster assets (orquestación)
│   ├── assets.py              # 5 assets: ingestion → catalog → dbt → export + volume_test
│   └── definitions.py         # Dagster Definitions (jobs + resources)
├── exporters/
│   ├── duckdb_export.py       # Crea vistas RAW en DuckDB
│   └── postgres_export.py     # Replica a PostgreSQL (bulk COPY, paralelo, incremental)
├── reporter.py                # Reportes en consola desde CONSUME layer
├── pipeline.py                # Orquestador batch (python pipeline.py)
├── Dockerfile                 # Container image (nginx + dagster-webserver)
├── start.sh                   # Arranca nginx (auth) + dagster-webserver
├── nginx.conf                 # Reverse proxy con basic auth
├── dagster.yaml               # Dagster storage en PostgreSQL
└── .env.example               # Variables de entorno (copiar a .env)

docker/                        # Demo local con docker-compose
railway/sftp/                  # Servicio SFTP en Railway
railway/dashboard/             # Dashboard FastAPI en Railway
```

## Flujo del pipeline

```
Dagster materialize()
  → raw_ingestion     (Python: SFTP → Parquet RAW)
    → duckdb_catalog   (Registra RAW views en DuckDB)
      → dbt_models     (dbt build: staging + consume)
        → postgres_export (DuckDB → PostgreSQL, bulk COPY paralelo)
```

## Modelos dbt actuales

### Staging (10 modelos)
`stg_canal`, `stg_canal_producto`, `stg_cliente`, `stg_cliente_canal`,
`stg_cliente_pedido`, `stg_cliente_ubicacion_geo`, `stg_item_pedido`,
`stg_pedido`, `stg_producto`, `stg_ubicacion_geo`

### Consume (4 modelos)
`ventas_por_canal`, `ventas_diarias`, `top_productos`, `clientes_resumen`

## Convenciones de código

- Type hints en funciones públicas
- Docstrings formato Google
- snake_case para archivos/funciones, PascalCase para clases
- Imports ordenados: stdlib, third-party, local
- Formatter: black (line-length=100)

## Cómo agregar un nuevo conector

1. Crear `railway/ducklake/connectors/nuevo.py` heredando `BaseConnector`
2. Elegir el patrón según el tamaño de la fuente:
   - **Patrón A** (dict): `extract() → dict[str, pd.DataFrame]` → usar `raw.save(df, source, table)`
   - **Patrón B** (generator): `extract() → Generator[(tabla, path_tmp)]` → usar `raw.save_from_path(path, source, table)`
3. Registrar en `connectors/__init__.py`
4. Agregar config en `config.py`
5. Agregar source en `dbt_techstore/models/sources.yml`
6. Invocar el conector en `dagster_pipeline/assets.py` dentro de `raw_ingestion`

## Cómo agregar un modelo dbt

1. Crear `.sql` en `dbt_techstore/models/staging/` o `consume/`
2. Usar `{{ source('raw', 'tabla') }}` para RAW o `{{ ref('stg_tabla') }}` para staging
3. Agregar tests en `_schema.yml` del directorio correspondiente
4. dbt resuelve el orden de ejecución automáticamente via ref()

## Deploy en Railway

- **Servicios activos**: ducklake (pipeline + Dagster UI), sftp (datos fuente), postgres (destino)
- **Dagster UI**: expuesto en el puerto 3000 via nginx con basic auth (DAGSTER_USER / DAGSTER_PASSWORD)
- **Auto-deploy**: push a `main` → Railway redeploya automáticamente
- **GitHub Action**: sincroniza `init.sql` y datos SFTP de `docker/` a `railway/`
- **Volumen**: montado en `/app/output` — persiste Parquets y DuckDB entre deployments

## Variables de entorno clave

| Variable | Descripción | Default |
|---|---|---|
| `OUTPUT_DIR` | Path base de datos | `/app/output` |
| `DUCKDB_PATH` | Path del archivo DuckDB | `OUTPUT_DIR/lake.duckdb` |
| `PG_HOST/PORT/USER/PASSWORD/DATABASE` | PostgreSQL destino | — |
| `SFTP_HOST/PORT/USER/PASSWORD` | SFTP fuente | — |
| `DAGSTER_USER` / `DAGSTER_PASSWORD` | Auth de la UI | `admin` / `admin` |

## Demo E-commerce TechStore

- **Source**: SFTP con 8 carpetas Parquet (vtex, meli, garbarino, etc.)
- **Output**: Parquets + DuckDB + PostgreSQL con 3 schemas (raw, staging, consume)
- **Local**: `docker-compose up` desde `docker/`
- **Railway**: push a main, se deploya solo

## Filosofía

- Config-driven: todo vía config.py + dbt YAML
- Idempotente: re-ejecutar sin duplicar datos
- RAW layer: append-only, particionado por fecha, nunca sobrescribir
- Transformaciones en SQL puro (dbt), Python solo para ingesta y export
- Export incremental: salta tablas sin cambios, bulk COPY para las que cambiaron
