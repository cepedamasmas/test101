# Setup local con Docker

Correr el pipeline en contenedores locales. Útil para simular el ambiente de producción (Railway) exactamente, o para no instalar Python y dependencias directamente en la máquina.

## Prerequisitos

| Requisito | Versión mínima | Verificar |
|-----------|---------------|-----------|
| Docker | 24+ | `docker --version` |
| Docker Compose | 2.20+ | `docker compose version` |

> No se necesita Python instalado en la máquina — todo corre dentro del contenedor.

## Paso 1 — Crear el archivo docker-compose.yml

El proyecto incluye un `Dockerfile` para el pipeline, pero no un `docker-compose.yml` (ese es específico de cada proyecto). Crear el siguiente archivo en la raíz del proyecto:

```yaml
# docker-compose.yml
services:

  pipeline:
    build: .
    env_file: .env
    environment:
      OUTPUT_DIR: /app/output
      DUCKDB_PATH: /app/output/lake.duckdb
      PG_HOST: postgres        # apunta al servicio postgres de este compose
    volumes:
      - ./output:/app/output   # persiste los Parquet y DuckDB entre runs
    depends_on:
      postgres:
        condition: service_healthy

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: ${PG_DATABASE}
    ports:
      - "5432:5432"            # expone postgres al host para conectarte con DBeaver/psql
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d ${PG_DATABASE}"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  postgres_data:
```

> Si el proyecto usa MySQL como fuente y no tenés uno externo, podés agregarlo también al compose (ver sección al final).

## Paso 2 — Configurar variables de entorno

```bash
cp .env.example .env
```

Editar `.env`. Para Docker, `PG_HOST` se sobreescribe en el compose, pero las credenciales sí se leen del `.env`:

```env
# PostgreSQL (credenciales — el host lo define el compose)
PG_HOST=localhost       # solo para uso fuera del compose (ej: DBeaver)
PG_PORT=5432
PG_USER=usuario
PG_PASSWORD=password
PG_DATABASE=mi_lake

# MySQL fuente (si aplica — host externo o agregar servicio al compose)
MYSQL_HOST=mysql.servidor.com
MYSQL_PORT=3306
MYSQL_USER=usuario
MYSQL_PASSWORD=password
MYSQL_DATABASE=mi_base

# SFTP fuente (si aplica — siempre externo)
SFTP_HOST=sftp.servidor.com
SFTP_PORT=22
SFTP_USER=usuario
SFTP_PASSWORD=password
```

> `OUTPUT_DIR` y `DUCKDB_PATH` no hace falta ponerlos en `.env` — el compose los define directamente en el contenedor.

## Paso 3 — Construir la imagen

```bash
docker compose build
```

Esto instala todas las dependencias de Python y parsea los modelos dbt dentro de la imagen. Tarda unos minutos la primera vez; las siguientes es más rápido gracias al cache de capas.

## Paso 4 — Correr el pipeline

```bash
docker compose up
```

El pipeline:
1. Espera a que PostgreSQL esté listo (health check automático)
2. Espera a MySQL/SFTP si están configurados
3. Corre `pipeline.py` completo
4. Queda en `sleep infinity` esperando el próximo run (no reinicia solo)

Los logs se ven en tiempo real en la terminal.

## Paso 5 — Ver los datos generados

Los Parquet y el DuckDB quedan en `./output/` en tu máquina (montado como volumen):

```
output/
  lake.duckdb
  datalake/raw/...
```

Para consultar el DuckDB directamente:

```bash
docker compose run --rm pipeline duckdb /app/output/lake.duckdb
```

O instalá el cliente DuckDB localmente y abrí `./output/lake.duckdb`.

## Correr solo una vez más (sin rebuild)

```bash
docker compose up --no-recreate
```

O si querés forzar un run limpio del pipeline (postgres ya está up):

```bash
docker compose run --rm pipeline python pipeline.py
```

## Limpiar todo

```bash
docker compose down -v   # borra contenedores Y el volumen de postgres
docker compose down      # borra contenedores, conserva el volumen de postgres
```

---

## Opcional: agregar MySQL al compose (para testing local)

Si necesitás simular una fuente MySQL sin tener un servidor externo:

```yaml
# Agregar dentro de services: en docker-compose.yml
  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_PASSWORD}
      MYSQL_DATABASE: ${MYSQL_DATABASE}
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      retries: 10

# Y en el servicio pipeline, agregar:
#   environment:
#     MYSQL_HOST: mysql

# Y en volumes:
#   mysql_data:
```

Luego cargar datos de prueba:

```bash
docker compose exec mysql mysql -u root -p${MYSQL_PASSWORD} ${MYSQL_DATABASE} < seed.sql
```
