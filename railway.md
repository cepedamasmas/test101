# Deploy DuckLake en Railway

## Arquitectura en Railway

```
┌────────────────────────────────────────────────────────────┐
│                     RAILWAY PROJECT                        │
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌────────────┐  │
│  │  MySQL   │  │ Postgres │  │  SFTP  │  │  DuckLake  │  │
│  │ (plugin) │  │ (plugin) │  │(Docker)│  │ (service)  │  │
│  │          │  │          │  │        │  │            │  │
│  │ source   │  │ output   │  │ files  │  │ pipeline   │  │
│  │ data     │  │ lake     │  │ upload │  │ cron job   │  │
│  └──────────┘  └──────────┘  └────────┘  └────────────┘  │
└────────────────────────────────────────────────────────────┘
```

Los 4 servicios se comunican por la red privada interna de Railway.

---

## Paso 1: Preparar el repo

Crear una carpeta `railway/` con subcarpetas para cada servicio.

```
railway/
├── ducklake/                 ← servicio pipeline
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── pipeline_ecommerce.py ← mismo que docker/ (sin cambios!)
│   └── start.sh
├── sftp/                     ← servicio SFTP
│   ├── Dockerfile
│   └── data/                 ← copiar de docker/sftp/data/
│       ├── pagos_banco.csv
│       ├── envios_courier.json
│       ├── catalogo_proveedor.xml
│       ├── liquidacion_mp.xlsx
│       └── reclamos.txt
└── mysql/
    └── init.sql              ← copiar de docker/mysql/init.sql
```

### 1.1 Dockerfile del pipeline (railway/ducklake/Dockerfile)

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline_ecommerce.py .
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]
```

### 1.2 requirements.txt (railway/ducklake/requirements.txt)

```txt
duckdb==1.2.2
pyarrow>=14.0.0
pymysql>=1.1.0
paramiko>=3.4.0
pandas>=2.0.0
openpyxl>=3.1.0
lxml>=5.0.0
requests>=2.31.0
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0
```

> Se mantiene `paramiko` porque ahora sí hay SFTP en Railway.

### 1.3 Dockerfile del SFTP (railway/sftp/Dockerfile)

```dockerfile
FROM atmoz/sftp:latest

# Copiar los archivos de datos al container
COPY data/ /home/techstore/upload/

# Crear usuario techstore con password techstore123 y UID 1001
CMD ["techstore:techstore123:1001"]
```

### 1.4 start.sh (railway/ducklake/start.sh)

```bash
#!/bin/bash

mkdir -p /app/output
chmod 777 /app/output

echo "Esperando a que MySQL esté listo..."
for i in $(seq 1 60); do
    if python -c "
import pymysql
pymysql.connect(
    host='$MYSQL_HOST', port=int('$MYSQL_PORT'),
    user='$MYSQL_USER', password='$MYSQL_PASSWORD',
    database='$MYSQL_DATABASE'
).close()
" 2>/dev/null; then
        echo "MySQL listo!"
        break
    fi
    echo "  MySQL no disponible, reintentando ($i/60)..."
    sleep 3
done

echo "Esperando a que SFTP esté listo..."
for i in $(seq 1 30); do
    if python -c "
import paramiko
t = paramiko.Transport(('$SFTP_HOST', int('$SFTP_PORT')))
t.connect(username='$SFTP_USER', password='$SFTP_PASSWORD')
t.close()
" 2>/dev/null; then
        echo "SFTP listo!"
        break
    fi
    echo "  SFTP no disponible, reintentando ($i/30)..."
    sleep 2
done

echo "Esperando a que PostgreSQL esté listo..."
for i in $(seq 1 30); do
    if python -c "
import psycopg2
psycopg2.connect(
    host='$PG_HOST', port=int('$PG_PORT'),
    user='$PG_USER', password='$PG_PASSWORD',
    dbname='$PG_DATABASE'
).close()
" 2>/dev/null; then
        echo "PostgreSQL listo!"
        break
    fi
    echo "  PostgreSQL no disponible, reintentando ($i/30)..."
    sleep 2
done

# Seed MySQL si está vacío
echo "Verificando seed data en MySQL..."
ROWS=$(python -c "
import pymysql
conn = pymysql.connect(host='$MYSQL_HOST', port=int('$MYSQL_PORT'), user='$MYSQL_USER', password='$MYSQL_PASSWORD', database='$MYSQL_DATABASE')
with conn.cursor() as c:
    c.execute('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=\"$MYSQL_DATABASE\" AND table_name=\"clientes\"')
    print(c.fetchone()[0])
conn.close()
" 2>/dev/null)

if [ "$ROWS" = "0" ]; then
    echo "  Tabla clientes no existe, ejecutando seed..."
    python -c "
import pymysql
conn = pymysql.connect(host='$MYSQL_HOST', port=int('$MYSQL_PORT'), user='$MYSQL_USER', password='$MYSQL_PASSWORD', database='$MYSQL_DATABASE')
with open('/app/init.sql', 'r', encoding='utf-8') as f:
    sql = f.read()
with conn.cursor() as c:
    for stmt in sql.split(';'):
        stmt = stmt.strip()
        if stmt:
            c.execute(stmt)
conn.commit()
conn.close()
print('  Seed data cargada!')
"
else
    echo "  MySQL ya tiene datos, skip seed."
fi

echo ""
echo "Ejecutando pipeline DuckLake..."
python pipeline_ecommerce.py
```

### 1.5 Copiar el init.sql

Copiar `docker/mysql/init.sql` a `railway/ducklake/init.sql` (el start.sh lo ejecuta si MySQL está vacío).

---

## Paso 2: Adaptar el pipeline

**No hay que adaptar nada.** El `pipeline_ecommerce.py` se usa tal cual
porque el SFTP corre como servicio en Railway. Solo hay que configurar
las variables de entorno correctamente (Paso 3).

---

## Paso 3: Crear el proyecto en Railway

### 3.1 Ir a [railway.app](https://railway.app) y crear nuevo proyecto

### 3.2 Agregar MySQL

1. Click **"+ New"** → **"Database"** → **"MySQL"**
2. Railway crea automáticamente las variables:
   - `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
   - Pero los nombres de Railway son diferentes (`MYSQLHOST`, `MYSQLPORT`, etc.)

3. En el servicio MySQL, ir a **Variables** y agregar manualmente:

```
MYSQL_HOST     = ${{MySQL.MYSQLHOST}}
MYSQL_PORT     = ${{MySQL.MYSQLPORT}}
MYSQL_USER     = ${{MySQL.MYSQLUSER}}
MYSQL_PASSWORD = ${{MySQL.MYSQLPASSWORD}}
MYSQL_DATABASE = ${{MySQL.MYSQLDATABASE}}
```

### 3.3 Agregar PostgreSQL

1. Click **"+ New"** → **"Database"** → **"PostgreSQL"**
2. En **Variables** del servicio PostgreSQL, agregar:

```
PG_HOST     = ${{Postgres.PGHOST}}
PG_PORT     = ${{Postgres.PGPORT}}
PG_USER     = ${{Postgres.PGUSER}}
PG_PASSWORD = ${{Postgres.PGPASSWORD}}
PG_DATABASE = ${{Postgres.PGDATABASE}}
```

### 3.4 Agregar el servicio SFTP

1. Click **"+ New"** → **"GitHub Repo"** → seleccionar el repo
2. En **Settings**:
   - **Root Directory**: `railway/sftp`
   - **Builder**: Dockerfile
3. El SFTP corre en el puerto 22 por defecto dentro de la red privada de Railway.

### 3.5 Agregar el servicio DuckLake

1. Click **"+ New"** → **"GitHub Repo"** → seleccionar el repo
2. En **Settings**:
   - **Root Directory**: `railway/ducklake`
   - **Builder**: Dockerfile
3. En **Variables**, referenciar las DBs y el SFTP:

```
MYSQL_HOST     = ${{MySQL.MYSQLHOST}}
MYSQL_PORT     = ${{MySQL.MYSQLPORT}}
MYSQL_USER     = ${{MySQL.MYSQLUSER}}
MYSQL_PASSWORD = ${{MySQL.MYSQLPASSWORD}}
MYSQL_DATABASE = ${{MySQL.MYSQLDATABASE}}
PG_HOST        = ${{Postgres.PGHOST}}
PG_PORT        = ${{Postgres.PGPORT}}
PG_USER        = ${{Postgres.PGUSER}}
PG_PASSWORD    = ${{Postgres.PGPASSWORD}}
PG_DATABASE    = ${{Postgres.PGDATABASE}}
SFTP_HOST      = sftp.railway.internal
SFTP_PORT      = 22
SFTP_USER      = techstore
SFTP_PASSWORD  = techstore123
```

> **Nota**: `sftp.railway.internal` es el hostname interno del servicio SFTP.
> Verificá el nombre exacto en Railway → servicio SFTP → **Settings** → **Private Networking**.

---

## Paso 4: Configurar GitHub Actions

### 4.1 Crear el token de Railway

1. Ir a [railway.app](https://railway.app) → **Account Settings** → **Tokens**
2. Crear un **Project Token** para el proyecto DuckLake
3. En GitHub → repo → **Settings** → **Secrets and variables** → **Actions**
4. Crear secret: `RAILWAY_TOKEN` = el token de Railway

### 4.2 Workflows incluidos

El repo ya tiene 2 workflows en `.github/workflows/`:

**`deploy-railway.yml`** - Deploy automático al pushear a `main`:
- Sincroniza archivos de `docker/` a `railway/` (pipeline, init.sql, sftp data)
- Deploya primero SFTP, luego DuckLake
- Solo se ejecuta si cambian archivos relevantes (`railway/**`, `docker/ducklake/pipeline_ecommerce.py`, etc.)

**`run-pipeline.yml`** - Ejecución programada:
- Cron: todos los días a las 6 AM UTC (3 AM Argentina)
- También tiene trigger manual desde GitHub → Actions → Run workflow

### 4.3 Deploy manual (primera vez)

```bash
# Instalar Railway CLI
npm install -g @railway/cli

# Login
railway login

# Linkear al proyecto
railway link

# Deploy SFTP primero
railway up --service sftp --directory railway/sftp

# Deploy DuckLake
railway up --service ducklake --directory railway/ducklake
```

### 4.4 Deploys siguientes

Cada push a `main` que toque archivos del pipeline triggerea deploy automático via GitHub Actions.

---

## Paso 5: Verificar

### Ver logs del pipeline

```bash
railway logs
```

### Conectar a PostgreSQL desde local

```bash
# Obtener la URL de conexión
railway variables

# Conectar con psql o DBeaver usando la PUBLIC URL de PostgreSQL
# Railway expone un puerto público automáticamente
```

En DBeaver:
- **Host**: la URL pública de Railway (ej: `roundhouse.proxy.rlwy.net`)
- **Port**: el puerto público asignado (ej: `12345`)
- **User/Pass/DB**: los valores de las variables de Railway

---

## Paso 6: Ejecución programada

Ya está configurado via GitHub Actions (`run-pipeline.yml`):

- **Automático**: Cron a las 6 AM UTC (3 AM Argentina) todos los días
- **Manual**: GitHub → Actions → "Run DuckLake Pipeline" → Run workflow

Para cambiar el horario, editar el cron en `.github/workflows/run-pipeline.yml`:

```yaml
schedule:
  - cron: '0 6 * * *'  # Cambiar aquí (formato: min hora dia mes dia_semana)
```

---

## Estructura final en Railway

```
RAILWAY PROJECT
│
├── MySQL (plugin)
│   └── techstore DB con seed data
│
├── PostgreSQL (plugin)
│   ├── raw.*           (tablas exportadas)
│   ├── staging.*       (tablas exportadas)
│   └── consume.*       (tablas BI)
│
├── SFTP (Docker service)
│   └── /home/techstore/upload/  (archivos de datos)
│
└── DuckLake (Docker service)
    ├── pipeline_ecommerce.py
    ├── start.sh
    └── init.sql         (seed MySQL)
```

---

## Costos estimados

| Servicio | Railway Plan | Costo aprox |
|----------|-------------|-------------|
| MySQL | Hobby | ~$5/mes |
| PostgreSQL | Hobby | ~$5/mes |
| SFTP (Docker) | Hobby | ~$2-3/mes (siempre encendido) |
| DuckLake (pipeline) | Hobby | ~$1-2/mes (corre y para) |
| **Total** | | **~$13-15/mes** |

> Con el plan Hobby ($5/mes base) tenés $5 de crédito incluido.
> El SFTP debe estar siempre corriendo para que el pipeline lo encuentre.
> El pipeline corre unos minutos y para, así que consume poco.

---

## Troubleshooting

### El pipeline no encuentra MySQL

- Verificar que las variables de entorno estén correctas en el servicio DuckLake
- Los nombres de Railway son `MYSQLHOST` (sin guión bajo), asegurate de mapearlos

### Error de conexión a PostgreSQL

- PostgreSQL de Railway usa SSL por defecto. Si falla, agregar `?sslmode=require` a la URL:

```python
pg_url = f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"
```

### El seed de MySQL se ejecuta cada vez

- El script `start.sh` verifica si la tabla `clientes` existe antes de hacer seed
- Si querés forzar un re-seed, eliminá la tabla manualmente

### El pipeline no encuentra SFTP

- Verificar que el servicio SFTP esté corriendo (no se apaga solo como el pipeline)
- El hostname interno se ve en Railway → servicio SFTP → **Settings** → **Private Networking**
- Si el hostname no es `sftp.railway.internal`, actualizar la variable `SFTP_HOST`

### Pipeline se queda sin memoria

- Railway Hobby tiene 512MB RAM
- Si el dataset crece, subir al plan Pro (8GB RAM)
- DuckDB puede configurarse con `SET memory_limit='256MB'`
