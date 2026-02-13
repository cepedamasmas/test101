# DuckLake - Contexto para Claude Code

## Overview

DuckLake es un data lake config-driven usando DuckDB + Parquet + dbt.
Alternativa a Snowflake/Databricks para empresas con 100GB-10TB de datos.
Deployado en Railway con Dagster como orquestador.

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

Python 3.11+, DuckDB 1.2, dbt-duckdb, Dagster, PyArrow, Pandas, Paramiko, PyMySQL

## Estructura del proyecto

```
railway/ducklake/              # Pipeline principal (deploy en Railway)
├── config.py                  # Configs centralizadas (env vars)
├── connectors/                # Ingesta de fuentes
│   ├── base.py                # BaseConnector (interfaz abstracta)
│   ├── mysql_connector.py     # MySQLConnector.extract()
│   ├── sftp_connector.py      # SFTPConnector.extract() (multi-formato)
│   └── api_connector.py       # APIConnector.extract()
├── layers/
│   └── raw.py                 # RawLayer.save() (append-only, particionado)
├── dbt_techstore/             # Proyecto dbt (staging + consume)
│   ├── models/staging/        # 9 modelos stg_*.sql con tests
│   └── models/consume/        # 7 modelos de agregación con tests
├── dagster_pipeline/          # Dagster assets (orquestación)
│   ├── assets.py              # 4 assets: ingestion → catalog → dbt → export
│   └── definitions.py         # Dagster Definitions
├── exporters/
│   ├── duckdb_export.py       # Crea vistas/tablas en DuckDB
│   └── postgres_export.py     # Replica a PostgreSQL
├── reporter.py                # Reportes en consola
├── pipeline.py                # Orquestador (Dagster materialize)
├── Dockerfile                 # Container image
├── start.sh                   # Health checks + seed + pipeline
└── init.sql                   # Seed data MySQL

docker/                        # Demo local con docker-compose
railway/sftp/                  # Servicio SFTP en Railway
railway/dashboard/             # Dashboard FastAPI en Railway
```

## Flujo del pipeline

```
Dagster materialize()
  → raw_ingestion     (Python: MySQL + SFTP + APIs → Parquet)
    → duckdb_catalog   (Registra RAW views en DuckDB)
      → dbt_models     (dbt run: staging + consume | dbt test)
        → postgres_export (DuckDB → PostgreSQL)
```

## Convenciones de código

- Type hints en funciones públicas
- Docstrings formato Google
- snake_case para archivos/funciones, PascalCase para clases
- Imports ordenados: stdlib, third-party, local
- Formatter: black (line-length=100)

## Cómo agregar un nuevo conector

1. Crear `railway/ducklake/connectors/nuevo.py` heredando `BaseConnector`
2. Implementar `extract() -> dict[str, pd.DataFrame]`
3. Registrar en `connectors/__init__.py`
4. Agregar config en `config.py`
5. Agregar source en `dbt_techstore/models/sources.yml`

## Cómo agregar un modelo dbt

1. Crear `.sql` en `dbt_techstore/models/staging/` o `consume/`
2. Usar `{{ source('raw', 'tabla') }}` para RAW o `{{ ref('stg_tabla') }}` para staging
3. Agregar tests en `_schema.yml` del directorio correspondiente
4. dbt resuelve el orden de ejecución automáticamente via ref()

## Deploy en Railway

- **Servicios**: ducklake (pipeline), sftp (datos), mysql, postgres, dashboard
- **Dagster UI** (opcional): mismo root directory, start command `dagster-webserver -m dagster_pipeline -h 0.0.0.0 -p 3000`
- **Auto-deploy**: push a `main` → Railway redeploya automáticamente
- **GitHub Action**: sincroniza `init.sql` y datos SFTP de `docker/` a `railway/`

## Demo E-commerce TechStore

- **Sources**: MySQL (4 tablas), SFTP (5 archivos: CSV, JSON, XML, XLSX, TXT), APIs (dólar blue, feriados)
- **Output**: Parquets + DuckDB + PostgreSQL con 3 schemas (raw, staging, consume)
- **Local**: `docker-compose up` desde `docker/`
- **Railway**: push a main, se deploya solo

## Filosofía

- Config-driven: todo vía config.py + dbt YAML
- Idempotente: re-ejecutar sin duplicar datos
- RAW layer: append-only, particionado por fecha, nunca sobrescribir
- Transformaciones en SQL puro (dbt), Python solo para ingesta y export
