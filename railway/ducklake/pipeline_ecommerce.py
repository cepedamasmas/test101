"""
Pipeline E-commerce TechStore Argentina
Ingesta multi-source: MySQL + SFTP + API -> RAW -> STAGING -> CONSUME
"""

import os
import json
import tempfile
from pathlib import Path
from datetime import datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pymysql
import paramiko
import pandas as pd
import requests

# ============================================================
# CONFIG
# ============================================================
OUTPUT = Path("/app/output")
OUTPUT.mkdir(parents=True, exist_ok=True)
DATA = OUTPUT / "datalake"

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "techstore123"),
    "database": os.environ.get("MYSQL_DATABASE", "techstore"),
}

SFTP_CONFIG = {
    "host": os.environ.get("SFTP_HOST", "sftp"),
    "port": int(os.environ.get("SFTP_PORT", 22)),
    "username": os.environ.get("SFTP_USER", "techstore"),
    "password": os.environ.get("SFTP_PASSWORD", "techstore123"),
}

conn = duckdb.connect(str(OUTPUT / "techstore.duckdb"))

print("=" * 70)
print("  TECHSTORE ARGENTINA - Pipeline DuckLake")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)


# ============================================================
# HELPERS
# ============================================================
def save_raw(df, source, table_name):
    """Guarda un DataFrame como parquet en RAW layer (append-only, particionado por fecha)."""
    now = datetime.now()
    raw_dir = (
        DATA / "raw" / source / table_name
        / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / "data.parquet"

    # Agregar metadata de ingestion
    df["_ingestion_timestamp"] = now.isoformat()
    df["_source_name"] = source

    table = pa.Table.from_pandas(df)
    pq.write_table(table, str(raw_file), compression="snappy")
    return len(df), raw_file


def save_parquet(df_or_result, path):
    """Guarda resultado DuckDB como parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(df_or_result, pd.DataFrame):
        table = pa.Table.from_pandas(df_or_result)
    else:
        table = df_or_result
    pq.write_table(table, str(path), compression="snappy")
    return len(df_or_result)


# ============================================================
# PASO 1: INGESTA RAW - MySQL
# ============================================================
print(f"\n[PASO 1a] Ingesta RAW desde MySQL ({MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']})")
print("-" * 50)

mysql_tables = ["clientes", "productos", "pedidos", "detalle_pedidos"]

try:
    mysql_conn = pymysql.connect(**MYSQL_CONFIG, cursorclass=pymysql.cursors.DictCursor)
    for table in mysql_tables:
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        n_rows, path = save_raw(df, "mysql", table)
        print(f"  RAW: {table:<20} -> {n_rows:>5} rows")
    mysql_conn.close()
    print("  MySQL OK")
except Exception as e:
    print(f"  ERROR MySQL: {e}")
    raise


# ============================================================
# PASO 1b: INGESTA RAW - SFTP
# ============================================================
print(f"\n[PASO 1b] Ingesta RAW desde SFTP ({SFTP_CONFIG['host']}:{SFTP_CONFIG['port']})")
print("-" * 50)

try:
    transport = paramiko.Transport((SFTP_CONFIG["host"], SFTP_CONFIG["port"]))
    transport.connect(username=SFTP_CONFIG["username"], password=SFTP_CONFIG["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    sftp_dir = "/upload"

    # --- pagos_banco.csv ---
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        sftp.get(f"{sftp_dir}/pagos_banco.csv", tmp.name)
        df = pd.read_csv(tmp.name, dtype={"cbu": str})
        n, _ = save_raw(df, "sftp", "pagos_banco")
        print(f"  RAW: pagos_banco.csv       -> {n:>5} rows")
        os.unlink(tmp.name)

    # --- envios_courier.json ---
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        sftp.get(f"{sftp_dir}/envios_courier.json", tmp.name)
        with open(tmp.name, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Flatten: sacar tracking_history como string JSON
        for row in data:
            row["tracking_history"] = json.dumps(row.get("tracking_history", []))
        df = pd.DataFrame(data)
        n, _ = save_raw(df, "sftp", "envios_courier")
        print(f"  RAW: envios_courier.json   -> {n:>5} rows")
        os.unlink(tmp.name)

    # --- catalogo_proveedor.xml ---
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tmp:
        sftp.get(f"{sftp_dir}/catalogo_proveedor.xml", tmp.name)
        df = pd.read_xml(tmp.name, xpath=".//producto")
        n, _ = save_raw(df, "sftp", "catalogo_proveedor")
        print(f"  RAW: catalogo_proveedor.xml-> {n:>5} rows")
        os.unlink(tmp.name)

    # --- liquidacion_mp.xlsx o .csv ---
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            sftp.get(f"{sftp_dir}/liquidacion_mp.xlsx", tmp.name)
            df = pd.read_excel(tmp.name, engine="openpyxl")
            n, _ = save_raw(df, "sftp", "liquidacion_mp")
            print(f"  RAW: liquidacion_mp.xlsx   -> {n:>5} rows")
            os.unlink(tmp.name)
    except FileNotFoundError:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            sftp.get(f"{sftp_dir}/liquidacion_mp.csv", tmp.name)
            df = pd.read_csv(tmp.name)
            n, _ = save_raw(df, "sftp", "liquidacion_mp")
            print(f"  RAW: liquidacion_mp.csv    -> {n:>5} rows (CSV fallback)")
            os.unlink(tmp.name)

    # --- reclamos.txt (pipe-delimited) ---
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
        sftp.get(f"{sftp_dir}/reclamos.txt", tmp.name)
        df = pd.read_csv(tmp.name, sep="|")
        n, _ = save_raw(df, "sftp", "reclamos")
        print(f"  RAW: reclamos.txt          -> {n:>5} rows")
        os.unlink(tmp.name)

    sftp.close()
    transport.close()
    print("  SFTP OK")
except Exception as e:
    print(f"  ERROR SFTP: {e}")
    raise


# ============================================================
# PASO 1c: INGESTA RAW - APIs
# ============================================================
print(f"\n[PASO 1c] Ingesta RAW desde APIs")
print("-" * 50)

# --- Dólar Blue ---
try:
    resp = requests.get("https://api.bluelytics.com.ar/v2/latest", timeout=10)
    dolar_data = resp.json()
    df = pd.DataFrame([{
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "dolar_blue_compra": dolar_data["blue"]["value_buy"],
        "dolar_blue_venta": dolar_data["blue"]["value_sell"],
        "dolar_oficial_compra": dolar_data["oficial"]["value_buy"],
        "dolar_oficial_venta": dolar_data["oficial"]["value_sell"],
    }])
    n, _ = save_raw(df, "api", "dolar")
    print(f"  RAW: dolar_blue            -> {n:>5} rows (blue: ${dolar_data['blue']['value_sell']})")
except Exception as e:
    print(f"  WARN: API dólar no disponible ({e}), usando valor default")
    df = pd.DataFrame([{
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "dolar_blue_compra": 1150, "dolar_blue_venta": 1200,
        "dolar_oficial_compra": 950, "dolar_oficial_venta": 1000,
    }])
    save_raw(df, "api", "dolar")

# --- Feriados Argentina ---
try:
    resp = requests.get(f"https://nolaborables.com.ar/api/v2/feriados/{datetime.now().year}", timeout=10)
    feriados = resp.json()
    df = pd.DataFrame(feriados)
    n, _ = save_raw(df, "api", "feriados")
    print(f"  RAW: feriados_2025         -> {n:>5} rows")
except Exception as e:
    print(f"  WARN: API feriados no disponible ({e}), skipping")


# ============================================================
# PASO 2: RAW -> STAGING
# ============================================================
print(f"\n[PASO 2] Transformación: RAW -> STAGING")
print("-" * 50)

# Helper para leer raw parquet con DuckDB (lee todas las particiones con hive partitioning)
def read_raw(source, table):
    path = str(DATA / "raw" / source / table / "*" / "*" / "*" / "data.parquet")
    return f"read_parquet('{path}', hive_partitioning=true)"


# --- Clientes STAGING ---
stg_clientes = DATA / "staging" / "clientes" / "data.parquet"
result = conn.execute(f"""
    SELECT
        cliente_id, nombre,
        COALESCE(email, 'sin_email@placeholder.com') AS email,
        ciudad, CAST(fecha_alta AS DATE) AS fecha_alta,
        estado, telefono,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY cliente_id) AS email_rank
    FROM {read_raw("mysql", "clientes")}
    WHERE estado != 'suspendido'
""").fetchdf()
# Marcar duplicados de email
result["email_duplicado"] = result["email_rank"] > 1
result = result.drop(columns=["email_rank"])
n = save_parquet(result, stg_clientes)
print(f"  STAGING: clientes          -> {n:>5} rows (filtrado suspendidos, flag email dup)")

# --- Productos STAGING ---
stg_productos = DATA / "staging" / "productos" / "data.parquet"
result = conn.execute(f"""
    SELECT
        p.producto_id, p.nombre, p.categoria,
        p.precio_unitario, p.stock, p.activo,
        p.costo, p.codigo_proveedor,
        ROUND((p.precio_unitario - p.costo) * 100.0 / p.precio_unitario, 1) AS margen_pct
    FROM {read_raw("mysql", "productos")} p
""").fetchdf()
n = save_parquet(result, stg_productos)
print(f"  STAGING: productos         -> {n:>5} rows (+ margen calculado)")

# --- Pedidos STAGING ---
stg_pedidos = DATA / "staging" / "pedidos" / "data.parquet"
result = conn.execute(f"""
    SELECT
        pedido_id, cliente_id,
        CAST(fecha AS DATE) AS fecha,
        estado, metodo_pago, total
    FROM {read_raw("mysql", "pedidos")}
""").fetchdf()
n = save_parquet(result, stg_pedidos)
print(f"  STAGING: pedidos           -> {n:>5} rows")

# --- Detalle Pedidos STAGING ---
stg_detalles = DATA / "staging" / "detalle_pedidos" / "data.parquet"
result = conn.execute(f"""
    SELECT
        detalle_id, pedido_id, producto_id,
        cantidad, precio_unitario, descuento, subtotal
    FROM {read_raw("mysql", "detalle_pedidos")}
    WHERE cantidad > 0 AND subtotal > 0
""").fetchdf()
n = save_parquet(result, stg_detalles)
print(f"  STAGING: detalle_pedidos   -> {n:>5} rows (validado qty>0)")

# --- Pagos Banco STAGING ---
stg_pagos = DATA / "staging" / "pagos_banco" / "data.parquet"
result = conn.execute(f"""
    SELECT
        pago_id, pedido_id,
        CAST(fecha_pago AS DATE) AS fecha_pago,
        CAST(monto AS DECIMAL(14,2)) AS monto,
        banco, estado,
        CAST(comision_bancaria AS DECIMAL(10,2)) AS comision_bancaria,
        tipo
    FROM {read_raw("sftp", "pagos_banco")}
""").fetchdf()
n = save_parquet(result, stg_pagos)
print(f"  STAGING: pagos_banco       -> {n:>5} rows")

# --- Envios STAGING ---
stg_envios = DATA / "staging" / "envios" / "data.parquet"
result = conn.execute(f"""
    SELECT
        envio_id, pedido_id, courier, tracking_code,
        destino_ciudad,
        CAST(peso_kg AS DECIMAL(6,2)) AS peso_kg,
        CAST(costo_envio AS DECIMAL(10,2)) AS costo_envio,
        estado_actual,
        CAST(fecha_estimada_entrega AS DATE) AS fecha_estimada_entrega
    FROM {read_raw("sftp", "envios_courier")}
""").fetchdf()
n = save_parquet(result, stg_envios)
print(f"  STAGING: envios            -> {n:>5} rows")

# --- Catalogo Proveedor STAGING ---
stg_catalogo = DATA / "staging" / "catalogo_proveedor" / "data.parquet"
result = conn.execute(f"""
    SELECT
        codigo, nombre, categoria,
        CAST(precio_lista AS DECIMAL(12,2)) AS precio_lista,
        CAST(precio_costo AS DECIMAL(12,2)) AS precio_costo,
        stock_proveedor, disponible, marca
    FROM {read_raw("sftp", "catalogo_proveedor")}
""").fetchdf()
n = save_parquet(result, stg_catalogo)
print(f"  STAGING: catalogo_prov     -> {n:>5} rows")

# --- Liquidacion MP STAGING ---
stg_mp = DATA / "staging" / "liquidacion_mp" / "data.parquet"
result = conn.execute(f"""
    SELECT
        operacion_mp, pedido_id,
        CAST(fecha_operacion AS DATE) AS fecha_operacion,
        CAST(fecha_liquidacion AS DATE) AS fecha_liquidacion,
        CAST(monto_bruto AS DECIMAL(14,2)) AS monto_bruto,
        CAST(comision_mp AS DECIMAL(10,2)) AS comision_mp,
        CAST(iva_comision AS DECIMAL(10,2)) AS iva_comision,
        CAST(retencion_iibb AS DECIMAL(10,2)) AS retencion_iibb,
        CAST(monto_neto AS DECIMAL(14,2)) AS monto_neto,
        estado, cuotas
    FROM {read_raw("sftp", "liquidacion_mp")}
""").fetchdf()
n = save_parquet(result, stg_mp)
print(f"  STAGING: liquidacion_mp    -> {n:>5} rows")

# --- Reclamos STAGING ---
stg_reclamos = DATA / "staging" / "reclamos" / "data.parquet"
result = conn.execute(f"""
    SELECT
        reclamo_id, pedido_id, cliente_id,
        CAST(fecha_reclamo AS DATE) AS fecha_reclamo,
        tipo, estado, prioridad, comentario
    FROM {read_raw("sftp", "reclamos")}
""").fetchdf()
n = save_parquet(result, stg_reclamos)
print(f"  STAGING: reclamos          -> {n:>5} rows")

# Quality Checks
print("\n  Quality Checks:")
for name, path in [("pedidos.pedido_id", stg_pedidos), ("clientes.cliente_id", stg_clientes)]:
    col = name.split(".")[1]
    nulls = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path}') WHERE {col} IS NULL").fetchone()[0]
    print(f"    {name} NOT NULL: {'PASS' if nulls == 0 else 'FAIL'} ({nulls} nulls)")

dups = conn.execute(f"""
    SELECT COUNT(*) - COUNT(DISTINCT email) FROM read_parquet('{stg_clientes}')
""").fetchone()[0]
print(f"    clientes.email DUPLICADOS: {dups} encontrados (flaggeados)")


# ============================================================
# PASO 3: STAGING -> CONSUME
# ============================================================
print(f"\n[PASO 3] Agregación: STAGING -> CONSUME")
print("-" * 50)

consume = DATA / "consume" / "bi"
consume.mkdir(parents=True, exist_ok=True)

# --- Revenue mensual ---
result = conn.execute(f"""
    SELECT
        DATE_TRUNC('month', p.fecha) AS mes,
        COUNT(DISTINCT p.pedido_id) AS pedidos,
        SUM(p.total) AS revenue,
        ROUND(AVG(p.total), 0) AS ticket_promedio,
        COUNT(DISTINCT p.cliente_id) AS clientes_unicos
    FROM read_parquet('{stg_pedidos}') p
    WHERE p.estado NOT IN ('cancelado')
    GROUP BY DATE_TRUNC('month', p.fecha)
    ORDER BY mes
""").fetchdf()
n = save_parquet(result, consume / "revenue_mensual.parquet")
print(f"  CONSUME: revenue_mensual      -> {n:>5} rows")

# --- Top clientes ---
result = conn.execute(f"""
    SELECT
        c.nombre AS cliente, c.ciudad, c.email,
        COUNT(DISTINCT p.pedido_id) AS compras,
        SUM(p.total) AS total_gastado,
        ROUND(AVG(p.total), 0) AS ticket_promedio,
        MIN(p.fecha) AS primera_compra,
        MAX(p.fecha) AS ultima_compra
    FROM read_parquet('{stg_pedidos}') p
    JOIN read_parquet('{stg_clientes}') c ON p.cliente_id = c.cliente_id
    WHERE p.estado != 'cancelado'
    GROUP BY c.nombre, c.ciudad, c.email
    ORDER BY total_gastado DESC
    LIMIT 20
""").fetchdf()
n = save_parquet(result, consume / "top_clientes.parquet")
print(f"  CONSUME: top_clientes          -> {n:>5} rows")

# --- Ventas por categoría ---
result = conn.execute(f"""
    SELECT
        prod.categoria,
        COUNT(DISTINCT d.pedido_id) AS pedidos,
        SUM(d.cantidad) AS unidades,
        SUM(d.subtotal) AS revenue,
        ROUND(AVG(prod.margen_pct), 1) AS margen_promedio_pct
    FROM read_parquet('{stg_detalles}') d
    JOIN read_parquet('{stg_productos}') prod ON d.producto_id = prod.producto_id
    GROUP BY prod.categoria
    ORDER BY revenue DESC
""").fetchdf()
n = save_parquet(result, consume / "ventas_por_categoria.parquet")
print(f"  CONSUME: ventas_por_categoria  -> {n:>5} rows")

# --- Conciliación de pagos ---
result = conn.execute(f"""
    SELECT
        p.metodo_pago,
        COUNT(*) AS transacciones,
        SUM(p.total) AS monto_pedidos,
        COALESCE(SUM(pb.monto), 0) AS monto_banco,
        COALESCE(SUM(mp.monto_bruto), 0) AS monto_mp,
        COALESCE(SUM(mp.comision_mp), 0) AS comisiones_mp,
        COALESCE(SUM(pb.comision_bancaria), 0) AS comisiones_banco,
        ROUND(
            CASE WHEN SUM(p.total) > 0
            THEN (COALESCE(SUM(pb.monto), 0) + COALESCE(SUM(mp.monto_bruto), 0)) * 100.0 / SUM(p.total)
            ELSE 0 END, 1
        ) AS pct_conciliado
    FROM read_parquet('{stg_pedidos}') p
    LEFT JOIN read_parquet('{stg_pagos}') pb ON p.pedido_id = pb.pedido_id
    LEFT JOIN read_parquet('{stg_mp}') mp ON p.pedido_id = mp.pedido_id
    WHERE p.estado != 'cancelado'
    GROUP BY p.metodo_pago
    ORDER BY transacciones DESC
""").fetchdf()
n = save_parquet(result, consume / "conciliacion_pagos.parquet")
print(f"  CONSUME: conciliacion_pagos    -> {n:>5} rows")

# --- Performance envíos ---
result = conn.execute(f"""
    SELECT
        e.courier,
        COUNT(*) AS envios,
        ROUND(AVG(e.costo_envio), 0) AS costo_promedio,
        ROUND(AVG(e.peso_kg), 2) AS peso_promedio_kg,
        SUM(CASE WHEN e.estado_actual = 'entregado' THEN 1 ELSE 0 END) AS entregados,
        SUM(CASE WHEN e.estado_actual = 'devuelto' THEN 1 ELSE 0 END) AS devueltos,
        ROUND(SUM(CASE WHEN e.estado_actual = 'devuelto' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_devolucion
    FROM read_parquet('{stg_envios}') e
    GROUP BY e.courier
    ORDER BY envios DESC
""").fetchdf()
n = save_parquet(result, consume / "performance_envios.parquet")
print(f"  CONSUME: performance_envios    -> {n:>5} rows")

# --- Análisis reclamos ---
result = conn.execute(f"""
    SELECT
        r.tipo,
        r.prioridad,
        COUNT(*) AS cantidad,
        SUM(CASE WHEN r.estado = 'resuelto' THEN 1 ELSE 0 END) AS resueltos,
        SUM(CASE WHEN r.estado = 'abierto' THEN 1 ELSE 0 END) AS abiertos,
        ROUND(SUM(CASE WHEN r.estado = 'resuelto' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_resolucion
    FROM read_parquet('{stg_reclamos}') r
    GROUP BY r.tipo, r.prioridad
    ORDER BY cantidad DESC
""").fetchdf()
n = save_parquet(result, consume / "analisis_reclamos.parquet")
print(f"  CONSUME: analisis_reclamos     -> {n:>5} rows")

# --- Alertas de stock ---
result = conn.execute(f"""
    SELECT
        p.producto_id, p.nombre, p.categoria,
        p.stock AS stock_interno,
        COALESCE(c.stock_proveedor, 0) AS stock_proveedor,
        p.precio_unitario,
        COALESCE(c.precio_costo, p.costo) AS precio_costo_actual,
        CASE
            WHEN p.stock = 0 THEN 'SIN_STOCK'
            WHEN p.stock < 10 THEN 'CRITICO'
            WHEN p.stock < 30 THEN 'BAJO'
            ELSE 'OK'
        END AS alerta_stock
    FROM read_parquet('{stg_productos}') p
    LEFT JOIN read_parquet('{stg_catalogo}') c ON p.codigo_proveedor = c.codigo
    WHERE p.activo = true AND p.stock < 30
    ORDER BY p.stock ASC
""").fetchdf()
n = save_parquet(result, consume / "alertas_stock.parquet")
print(f"  CONSUME: alertas_stock         -> {n:>5} rows")


# ============================================================
# PASO 4: REPORTES FINALES
# ============================================================
print(f"\n{'=' * 70}")
print("  REPORTES FINALES (desde CONSUME layer)")
print(f"{'=' * 70}")

# Revenue mensual
print(f"\n  REVENUE MENSUAL:")
print(f"  {'Mes':<12} {'Pedidos':>8} {'Revenue':>14} {'Ticket Prom':>12} {'Clientes':>9}")
print(f"  {'-'*55}")
df = conn.execute(f"SELECT * FROM read_parquet('{consume / 'revenue_mensual.parquet'}')").fetchdf()
for _, row in df.iterrows():
    mes = str(row["mes"])[:7] if pd.notna(row["mes"]) else "N/A"
    print(f"  {mes:<12} {int(row['pedidos']):>8} ${int(row['revenue']):>12,} {int(row['ticket_promedio']):>11,} {int(row['clientes_unicos']):>9}")

# Top 10 clientes
print(f"\n  TOP 10 CLIENTES:")
print(f"  {'Cliente':<25} {'Ciudad':<16} {'Compras':>8} {'Total Gastado':>14}")
print(f"  {'-'*63}")
df = conn.execute(f"SELECT * FROM read_parquet('{consume / 'top_clientes.parquet'}') LIMIT 10").fetchdf()
for _, row in df.iterrows():
    print(f"  {str(row['cliente'])[:24]:<25} {str(row['ciudad'])[:15]:<16} {int(row['compras']):>8} ${int(row['total_gastado']):>12,}")

# Ventas por categoría
print(f"\n  VENTAS POR CATEGORÍA:")
print(f"  {'Categoría':<16} {'Pedidos':>8} {'Unidades':>9} {'Revenue':>14} {'Margen%':>8}")
print(f"  {'-'*55}")
df = conn.execute(f"SELECT * FROM read_parquet('{consume / 'ventas_por_categoria.parquet'}')").fetchdf()
for _, row in df.iterrows():
    print(f"  {str(row['categoria'])[:15]:<16} {int(row['pedidos']):>8} {int(row['unidades']):>9} ${int(row['revenue']):>12,} {row['margen_promedio_pct']:>7.1f}%")

# Conciliación
print(f"\n  CONCILIACIÓN DE PAGOS:")
print(f"  {'Método':<18} {'Txns':>6} {'Monto Pedidos':>14} {'Conciliado%':>12}")
print(f"  {'-'*50}")
df = conn.execute(f"SELECT * FROM read_parquet('{consume / 'conciliacion_pagos.parquet'}')").fetchdf()
for _, row in df.iterrows():
    print(f"  {str(row['metodo_pago'])[:17]:<18} {int(row['transacciones']):>6} ${int(row['monto_pedidos']):>12,} {row['pct_conciliado']:>11.1f}%")

# Performance envíos
print(f"\n  PERFORMANCE ENVÍOS:")
print(f"  {'Courier':<18} {'Envíos':>7} {'Costo Prom':>11} {'Entregados':>11} {'Devueltos':>10} {'Dev%':>6}")
print(f"  {'-'*63}")
df = conn.execute(f"SELECT * FROM read_parquet('{consume / 'performance_envios.parquet'}')").fetchdf()
for _, row in df.iterrows():
    print(f"  {str(row['courier'])[:17]:<18} {int(row['envios']):>7} ${int(row['costo_promedio']):>9,} {int(row['entregados']):>11} {int(row['devueltos']):>10} {row['pct_devolucion']:>5.1f}%")

# Alertas stock
print(f"\n  ALERTAS DE STOCK:")
df = conn.execute(f"SELECT * FROM read_parquet('{consume / 'alertas_stock.parquet'}')").fetchdf()
sin_stock = len(df[df["alerta_stock"] == "SIN_STOCK"])
critico = len(df[df["alerta_stock"] == "CRITICO"])
bajo = len(df[df["alerta_stock"] == "BAJO"])
print(f"  SIN STOCK: {sin_stock} productos | CRÍTICO (<10): {critico} | BAJO (<30): {bajo}")

# Reclamos
print(f"\n  ANÁLISIS RECLAMOS:")
print(f"  {'Tipo':<20} {'Cantidad':>9} {'Resueltos':>10} {'Abiertos':>9} {'Resolución%':>12}")
print(f"  {'-'*60}")
df = conn.execute(f"""
    SELECT tipo, SUM(cantidad) AS cantidad, SUM(resueltos) AS resueltos,
           SUM(abiertos) AS abiertos,
           ROUND(SUM(resueltos)*100.0/SUM(cantidad),1) AS pct
    FROM read_parquet('{consume / 'analisis_reclamos.parquet'}')
    GROUP BY tipo ORDER BY cantidad DESC
""").fetchdf()
for _, row in df.iterrows():
    print(f"  {str(row['tipo'])[:19]:<20} {int(row['cantidad']):>9} {int(row['resueltos']):>10} {int(row['abiertos']):>9} {row['pct']:>11.1f}%")


# ============================================================
# PASO 5: CREAR TABLAS en DuckDB organizadas por schema (para DBeaver)
# ============================================================
print(f"\n[PASO 5] Creando schemas y tablas en DuckDB para consulta")
print("-" * 50)

# --- Schema RAW ---
conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
raw_tables = {
    "clientes": ("mysql", "clientes"),
    "productos": ("mysql", "productos"),
    "pedidos": ("mysql", "pedidos"),
    "detalle_pedidos": ("mysql", "detalle_pedidos"),
    "pagos_banco": ("sftp", "pagos_banco"),
    "envios_courier": ("sftp", "envios_courier"),
    "catalogo_proveedor": ("sftp", "catalogo_proveedor"),
    "liquidacion_mp": ("sftp", "liquidacion_mp"),
    "reclamos": ("sftp", "reclamos"),
}
# API sources (verificar con glob porque ahora usan particiones)
for api_table in ["dolar", "feriados"]:
    matches = list(DATA.glob(f"raw/api/{api_table}/*/*/*/data.parquet"))
    if matches:
        raw_tables[api_table] = ("api", api_table)

for tbl_name, (source, folder) in raw_tables.items():
    raw_glob = DATA / "raw" / source / folder / "*" / "*" / "*" / "data.parquet"
    conn.execute(f"CREATE OR REPLACE VIEW raw.{tbl_name} AS SELECT * FROM read_parquet('{raw_glob}', hive_partitioning=true)")
print(f"  {len(raw_tables)} vistas RAW creadas (schema: raw)")

# --- Schema STAGING ---
conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
conn.execute(f"CREATE OR REPLACE TABLE staging.clientes AS SELECT * FROM read_parquet('{stg_clientes}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.productos AS SELECT * FROM read_parquet('{stg_productos}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.pedidos AS SELECT * FROM read_parquet('{stg_pedidos}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.detalle_pedidos AS SELECT * FROM read_parquet('{stg_detalles}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.pagos_banco AS SELECT * FROM read_parquet('{stg_pagos}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.envios AS SELECT * FROM read_parquet('{stg_envios}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.catalogo_proveedor AS SELECT * FROM read_parquet('{stg_catalogo}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.liquidacion_mp AS SELECT * FROM read_parquet('{stg_mp}')")
conn.execute(f"CREATE OR REPLACE TABLE staging.reclamos AS SELECT * FROM read_parquet('{stg_reclamos}')")
print("  9 tablas STAGING creadas (schema: staging)")

# --- Schema CONSUME ---
conn.execute("CREATE SCHEMA IF NOT EXISTS consume")
conn.execute(f"CREATE OR REPLACE TABLE consume.revenue_mensual AS SELECT * FROM read_parquet('{consume / 'revenue_mensual.parquet'}')")
conn.execute(f"CREATE OR REPLACE TABLE consume.top_clientes AS SELECT * FROM read_parquet('{consume / 'top_clientes.parquet'}')")
conn.execute(f"CREATE OR REPLACE TABLE consume.ventas_por_categoria AS SELECT * FROM read_parquet('{consume / 'ventas_por_categoria.parquet'}')")
conn.execute(f"CREATE OR REPLACE TABLE consume.conciliacion_pagos AS SELECT * FROM read_parquet('{consume / 'conciliacion_pagos.parquet'}')")
conn.execute(f"CREATE OR REPLACE TABLE consume.performance_envios AS SELECT * FROM read_parquet('{consume / 'performance_envios.parquet'}')")
conn.execute(f"CREATE OR REPLACE TABLE consume.analisis_reclamos AS SELECT * FROM read_parquet('{consume / 'analisis_reclamos.parquet'}')")
conn.execute(f"CREATE OR REPLACE TABLE consume.alertas_stock AS SELECT * FROM read_parquet('{consume / 'alertas_stock.parquet'}')")
print("  7 tablas CONSUME creadas (schema: consume)")

print("  -> En DBeaver vas a ver 3 schemas: raw, staging, consume")


# ============================================================
# PASO 6: EXPORTAR TABLAS A POSTGRESQL (acceso remoto)
# ============================================================
print(f"\n[PASO 6] Exportando tablas a PostgreSQL (acceso remoto)")
print("-" * 50)

from sqlalchemy import create_engine, text

PG_CONFIG = {
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": os.environ.get("PG_PORT", "5432"),
    "user": os.environ.get("PG_USER", "techstore"),
    "password": os.environ.get("PG_PASSWORD", "techstore123"),
    "database": os.environ.get("PG_DATABASE", "techstore_lake"),
}

pg_url = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
pg_engine = create_engine(pg_url)

# Crear schemas en PostgreSQL
with pg_engine.begin() as pg_conn:
    pg_conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    pg_conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
    pg_conn.execute(text("CREATE SCHEMA IF NOT EXISTS consume"))

# Exportar RAW
raw_count = 0
for tbl_name, (source, folder) in raw_tables.items():
    raw_glob = DATA / "raw" / source / folder / "*" / "*" / "*" / "data.parquet"
    df = conn.execute(f"SELECT * FROM read_parquet('{raw_glob}', hive_partitioning=true)").fetchdf()
    df.to_sql(tbl_name, pg_engine, schema="raw", if_exists="replace", index=False)
    raw_count += 1
print(f"  PostgreSQL raw: {raw_count} tablas exportadas")

# Exportar STAGING
stg_exports = {
    "clientes": stg_clientes, "productos": stg_productos,
    "pedidos": stg_pedidos, "detalle_pedidos": stg_detalles,
    "pagos_banco": stg_pagos, "envios": stg_envios,
    "catalogo_proveedor": stg_catalogo, "liquidacion_mp": stg_mp,
    "reclamos": stg_reclamos,
}
for tbl_name, parquet_path in stg_exports.items():
    df = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetchdf()
    df.to_sql(tbl_name, pg_engine, schema="staging", if_exists="replace", index=False)
print(f"  PostgreSQL staging: {len(stg_exports)} tablas exportadas")

# Exportar CONSUME
consume_files = [
    "revenue_mensual", "top_clientes", "ventas_por_categoria",
    "conciliacion_pagos", "performance_envios", "analisis_reclamos", "alertas_stock",
]
for tbl_name in consume_files:
    parquet_path = consume / f"{tbl_name}.parquet"
    df = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetchdf()
    df.to_sql(tbl_name, pg_engine, schema="consume", if_exists="replace", index=False)
print(f"  PostgreSQL consume: {len(consume_files)} tablas exportadas")

pg_engine.dispose()
print("  -> PostgreSQL listo! Conectate con DBeaver al puerto 5433")


# ============================================================
# FIN
# ============================================================
conn.close()

print(f"\n{'=' * 70}")
print("  Pipeline completado exitosamente!")
print(f"  Output: /app/output/datalake/")
print(f"  DuckDB: /app/output/techstore.duckdb")
print(f"  PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'=' * 70}")
