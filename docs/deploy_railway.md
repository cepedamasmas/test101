# Deploy en Railway

Railway es el ambiente de producción recomendado. El proyecto incluye un `Dockerfile` y un `start.sh` listos para Railway. Con un `git push` el pipeline se actualiza y corre automáticamente.

## Prerequisitos

- Cuenta en [Railway](https://railway.app) (plan Hobby o superior — el free tier no tiene persistencia suficiente)
- Repositorio en GitHub con el código del proyecto
- Credenciales de las fuentes de datos (MySQL, SFTP, APIs) accesibles desde internet

## Arquitectura en Railway

Railway crea servicios independientes dentro de un proyecto:

```
Railway Project
├── pipeline        ← imagen Docker del pipeline (este repo)
└── postgres        ← PostgreSQL gestionado por Railway (destino)
```

MySQL y SFTP son servicios **externos** — Railway solo los consume, no los hostea (a menos que los agregues manualmente como servicios adicionales).

---

## Paso 1 — Crear el proyecto en Railway

1. Ir a [railway.app](https://railway.app) → **New Project**
2. Elegir **Empty Project**
3. Darle un nombre al proyecto (ej: `sediment-mi-cliente`)

## Paso 2 — Agregar PostgreSQL

1. Dentro del proyecto → **+ New** → **Database** → **Add PostgreSQL**
2. Railway crea el servicio y genera las credenciales automáticamente
3. Click en el servicio PostgreSQL → pestaña **Variables**
4. Anotar los valores de `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`

> Railway usa el prefijo `PG` distinto al que usa este proyecto. Los valores son los mismos, los nombres de variables se mapean en el paso de configuración.

## Paso 3 — Agregar el servicio del pipeline

1. Dentro del proyecto → **+ New** → **GitHub Repo**
2. Conectar con GitHub si es la primera vez
3. Seleccionar el repositorio del proyecto
4. Railway detecta el `Dockerfile` automáticamente — no hace falta configurar nada más del build

## Paso 4 — Configurar las variables de entorno

Click en el servicio **pipeline** → pestaña **Variables** → **RAW Editor** y pegar:

```env
# Paths (fijos para Railway — no cambiar)
OUTPUT_DIR=/app/output
DUCKDB_PATH=/app/output/lake.duckdb

# PostgreSQL destino (usar los valores del paso 2)
PG_HOST=roundhouse.proxy.rlwy.net   # el PGHOST de Railway
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=xxxxx
PG_DATABASE=railway

# MySQL fuente (si aplica)
MYSQL_HOST=mysql.servidor.com
MYSQL_PORT=3306
MYSQL_USER=usuario
MYSQL_PASSWORD=password
MYSQL_DATABASE=mi_base

# SFTP fuente (si aplica)
SFTP_HOST=sftp.servidor.com
SFTP_PORT=22
SFTP_USER=usuario
SFTP_PASSWORD=password
```

> **Tip:** Para conectar el pipeline al PostgreSQL interno de Railway sin exponer credenciales, usar la **referencia de variable** de Railway: en el campo valor escribir `${{Postgres.PGHOST}}` y Railway lo resuelve internamente.

## Paso 5 — Verificar el build

Railway dispara el primer build automáticamente al conectar el repo. Para verlo:

1. Click en el servicio **pipeline** → pestaña **Deployments**
2. Click en el deployment activo → **View Logs**

El build hace:
1. `pip install -r requirements.txt` (~2-3 min)
2. `dbt parse` para generar el manifest

Si el build falla, el error aparece en los logs del build (no del runtime).

## Paso 6 — Primer run

Una vez que el build termina, el contenedor arranca y ejecuta `start.sh`:

1. Espera a PostgreSQL (health check)
2. Espera a MySQL/SFTP si están configurados
3. Corre `python pipeline.py`
4. Si termina OK → `sleep infinity` (el contenedor queda vivo pero inactivo)

Ver los logs en tiempo real: pestaña **Deployments** → deployment activo → **View Logs**.

## Paso 7 — Configurar re-deploy automático

Por defecto Railway redeploya en cada `git push`. El pipeline corre en cada redeploy.

Para correr el pipeline en un horario (ej: todos los días a las 6am):

1. Servicio **pipeline** → **Settings** → **Cron Schedule**
2. Ingresar la expresión cron: `0 6 * * *`

> Alternativamente, usar el `dagster-daemon` y la UI de Dagster para scheduling (requiere que el contenedor corra permanentemente con `dagster dev`, distinto al modo batch actual).

## Paso 8 — Ver los datos en PostgreSQL

Conectarse al PostgreSQL de Railway desde DBeaver, TablePlus o cualquier cliente:

- **Host:** el `PGHOST` de Railway (el externo, con puerto 6543 generalmente)
- **Puerto:** el externo de Railway (5432 interno, 6543 o similar externo — ver en Variables del servicio Postgres)
- **Usuario/Contraseña/DB:** los del paso 2

---

## Actualizar el pipeline

```bash
git add .
git commit -m "actualizar modelos dbt"
git push
```

Railway redeploya automáticamente y el pipeline corre con los cambios.

## Costos estimados

| Servicio | Costo aproximado |
|----------|-----------------|
| Pipeline (Hobby) | ~$5/mes base + uso de CPU/RAM durante el run |
| PostgreSQL (Hobby) | ~$5/mes |
| **Total** | **~$10-15/mes** para proyectos medianos |

El pipeline en modo batch (corre y duerme) consume muy poca RAM en reposo — solo paga la ejecución real.

## Solución de problemas comunes

**Build falla en `dbt parse`** — Algún modelo SQL tiene error de sintaxis. Verificar con `dbt parse` localmente antes de pushear.

**Pipeline falla con `Connection refused` a PostgreSQL** — Las variables `PG_*` no están bien configuradas. Verificar que `PG_HOST` apunte al host interno de Railway (no el externo).

**`sleep infinity` no aparece en logs** — El pipeline falló antes de terminar. Buscar el error en los logs del mismo deployment, no en un deployment anterior.

**Variables no disponibles** — Después de agregar o cambiar variables en Railway, el servicio no se redeploya automáticamente. Hacer click en **Redeploy** manualmente o hacer un `git push` vacío:
```bash
git commit --allow-empty -m "trigger redeploy" && git push
```
