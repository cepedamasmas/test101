# Setup local (sin Docker)

Correr el pipeline directamente en tu máquina con Python. La opción más simple para desarrollo y debugging.

## Prerequisitos

| Requisito | Versión mínima | Verificar |
|-----------|---------------|-----------|
| Python | 3.11+ | `python --version` |
| pip | cualquiera | `pip --version` |
| PostgreSQL | 14+ | servicio corriendo y accesible |
| MySQL | 8+ | solo si usás fuente MySQL |
| Servidor SFTP | cualquiera | solo si usás fuente SFTP |

> PostgreSQL es el único servicio siempre requerido (es el destino de exportación).
> MySQL y SFTP solo son necesarios si el proyecto los usa como fuentes.

## Paso 1 — Clonar y entrar al directorio

```bash
git clone https://github.com/tu-org/tu-proyecto.git
cd tu-proyecto
```

## Paso 2 — Crear el entorno virtual

```bash
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
# .venv\Scripts\activate         # Windows
```

## Paso 3 — Instalar dependencias

```bash
pip install -r requirements.txt
```

Esto instala DuckDB, dbt, Dagster, conectores y todo lo demás. Puede tardar 2-3 minutos.

## Paso 4 — Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env` con los valores reales:

```env
# Path local donde se guardan los Parquet y el archivo DuckDB
OUTPUT_DIR=./output

# PostgreSQL destino (requerido)
PG_HOST=localhost
PG_PORT=5432
PG_USER=usuario
PG_PASSWORD=password
PG_DATABASE=mi_lake

# MySQL fuente (solo si aplica)
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=usuario
MYSQL_PASSWORD=password
MYSQL_DATABASE=mi_base

# SFTP fuente (solo si aplica)
SFTP_HOST=sftp.servidor.com
SFTP_PORT=22
SFTP_USER=usuario
SFTP_PASSWORD=password

# DuckDB (debe apuntar al mismo lugar que OUTPUT_DIR)
DUCKDB_PATH=./output/lake.duckdb
```

> **Importante:** `OUTPUT_DIR` y `DUCKDB_PATH` deben ser paths locales (no `/app/output` que es para Docker/Railway).

## Paso 5 — Cargar las variables en la sesión

```bash
export $(grep -v '^#' .env | xargs)   # Linux/Mac
```

En Windows (PowerShell):
```powershell
Get-Content .env | Where-Object { $_ -notmatch '^#' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($name, $value, 'Process')
}
```

## Paso 6 — Ajustar el path de DuckDB en dbt

`dbt_project/profiles.yml` tiene el path hardcodeado para Docker. Cambiarlo para desarrollo local:

```yaml
mi_proyecto:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./output/lake.duckdb   # <- path local, no /app/output
      threads: 4
```

## Paso 7 — Crear la base de datos PostgreSQL destino

```bash
createdb mi_lake   # o hacerlo desde psql/DBeaver
```

## Paso 8 — Verificar dbt

```bash
dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Si no hay errores, dbt encuentra los modelos y el DuckDB correctamente.

## Paso 9 — Levantar la UI de Dagster

```bash
dagster dev -m dagster_pipeline
```

Abre `http://localhost:3000` — desde ahí podés:
- Ver los 4 assets del pipeline
- Ejecutar manualmente con "Materialize all"
- Ver logs en tiempo real
- Revisar el linaje de datos

## Paso 10 — Correr el pipeline completo

Desde la UI de Dagster en `http://localhost:3000`, click en **Materialize all**.

O desde terminal:

```bash
python pipeline.py
```

## Estructura de output generada

Después del primer run, vas a encontrar:

```
output/
  lake.duckdb              base de datos DuckDB con todas las vistas
  datalake/
    raw/
      mysql/tabla/year=.../month=.../day=.../data.parquet
      sftp/tabla/...
      api/tabla/...
```

## Solución de problemas comunes

**`ModuleNotFoundError`** — El entorno virtual no está activado o faltó `pip install`.

**`DuckDB not found` en dbt** — El `path` en `profiles.yml` no apunta al mismo archivo que `DUCKDB_PATH`.

**`Connection refused` en PostgreSQL** — El servicio no está corriendo o las credenciales en `.env` son incorrectas. Verificar con `psql -h localhost -U usuario -d mi_lake`.

**`Permission denied` en `output/`** — Crear el directorio manualmente: `mkdir -p output`.
