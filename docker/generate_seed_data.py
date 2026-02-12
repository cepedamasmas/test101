"""
Genera TODA la data de prueba coherente para el demo TechStore Argentina.
- MySQL init.sql (clientes, productos, pedidos, detalle_pedidos)
- SFTP files (pagos_banco.csv, envios_courier.json, catalogo_proveedor.xml, liquidacion_mp.xlsx, reclamos.txt)

Ejecutar: python generate_seed_data.py
"""

import random
import json
import csv
import io
import os
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

BASE = Path(__file__).parent

# ============================================================
# CONFIGURACION
# ============================================================
N_CLIENTES = 100
N_PRODUCTOS = 50
N_PEDIDOS = 500
FECHA_INICIO = datetime(2025, 7, 1)
FECHA_FIN = datetime(2025, 12, 31)

CIUDADES = [
    "Buenos Aires", "Córdoba", "Rosario", "Mendoza", "Tucumán",
    "La Plata", "Mar del Plata", "Salta", "Santa Fe", "San Juan",
    "Neuquén", "Bahía Blanca", "Resistencia", "Posadas", "Paraná"
]

CATEGORIAS = {
    "Smartphones": [
        ("Samsung Galaxy S24", 899999), ("iPhone 15", 1299999), ("Motorola Edge 40", 549999),
        ("Xiaomi 14", 699999), ("Samsung Galaxy A54", 399999), ("iPhone 14", 999999),
        ("Motorola G84", 299999), ("Xiaomi Redmi Note 13", 249999),
    ],
    "Notebooks": [
        ("Lenovo IdeaPad 3", 749999), ("HP Pavilion 15", 899999), ("Dell Inspiron 14", 999999),
        ("ASUS VivoBook", 649999), ("Acer Aspire 5", 599999), ("MacBook Air M2", 1899999),
    ],
    "Audio": [
        ("JBL Tune 520BT", 49999), ("Sony WH-1000XM5", 299999), ("Samsung Buds2 Pro", 129999),
        ("AirPods Pro 2", 249999), ("Philips TAH1205", 29999), ("JBL Charge 5", 149999),
    ],
    "Accesorios": [
        ("Logitech MX Master 3S", 89999), ("Teclado Redragon K552", 39999),
        ("Webcam Logitech C920", 59999), ("Hub USB-C 7 en 1", 34999),
        ("Mousepad HyperX Fury", 14999), ("Cargador Inalámbrico Samsung", 24999),
    ],
    "Gaming": [
        ("Monitor LG 27'' 144Hz", 449999), ("Silla Gamer Corsair", 349999),
        ("Auriculares HyperX Cloud II", 79999), ("Control Xbox Wireless", 69999),
        ("Mousepad XXL RGB", 29999), ("Webcam Streaming 1080p", 44999),
    ],
    "Wearables": [
        ("Apple Watch SE", 349999), ("Samsung Galaxy Watch 6", 299999),
        ("Xiaomi Smart Band 8", 39999), ("Garmin Venu Sq 2", 249999),
    ],
    "Tablets": [
        ("iPad 10ma Gen", 699999), ("Samsung Galaxy Tab A9", 299999),
        ("Lenovo Tab M10", 199999), ("iPad Air M1", 999999),
    ],
}

METODOS_PAGO = ["mercadopago", "transferencia", "tarjeta_credito", "tarjeta_debito", "efectivo"]
METODOS_PAGO_WEIGHTS = [35, 25, 20, 15, 5]

NOMBRES = [
    "Martín García", "Lucía Fernández", "Juan Rodríguez", "María López", "Carlos Martínez",
    "Ana González", "Diego Pérez", "Valentina Sánchez", "Federico Romero", "Camila Díaz",
    "Pablo Torres", "Sofía Álvarez", "Matías Ruiz", "Florencia Castro", "Nicolás Morales",
    "Julieta Acosta", "Tomás Herrera", "Agustina Molina", "Sebastián Núñez", "Milagros Medina",
    "Lucas Ortiz", "Daniela Suárez", "Ezequiel Vega", "Carolina Giménez", "Facundo Cabrera",
    "Rocío Flores", "Ignacio Ríos", "Candela Domínguez", "Joaquín Peralta", "Abril Figueroa",
    "Ramiro Aguirre", "Ailén Sosa", "Lautaro Paz", "Micaela Ramírez", "Franco Navarro",
    "Bianca Rojas", "Thiago Gutiérrez", "Emilia Luna", "Santiago Vargas", "Renata Ojeda",
    "Maximiliano Silva", "Catalina Méndez", "Bruno Campos", "Victoria Godoy", "Gonzalo Arias",
    "Pilar Ledesma", "Iván Córdoba", "Delfina Bustos", "Adrián Villalba", "Jazmín Pereyra",
    "Andrés Blanco", "Paula Quiroga", "Rodrigo Maidana", "Clara Ibáñez", "Marcos Vera",
    "Natalia Paredes", "Gabriel Cáceres", "Guadalupe Ramos", "Hernán Rivero", "Constanza Bravo",
    "Manuel Espinoza", "Elena Contreras", "Fernando Lucero", "Antonella Ponce", "Leonardo Fuentes",
    "Macarena Arce", "Cristian Miranda", "Amparo Carrizo", "Mauricio Barreto", "Serena Montoya",
    "Damián Lagos", "Trinidad Rossi", "Enzo Barrios", "Josefina Maldonado", "Ariel Quiroz",
    "Isabel Sandoval", "Germán Soria", "Martina Ayala", "Hugo Mansilla", "Eugenia Correa",
    "Leandro Ochoa", "Celeste Franco", "Walter Palacios", "Alma Duarte", "Darío Escobar",
    "Julia Montes", "Nelson Arrieta", "Luz Cardozo", "Rubén Salas", "Eliana Benítez",
    "César Melo", "Viviana Vallejo", "Oscar Bernal", "Laura Castaño", "Roberto Galván",
    "Silvia Heredia", "Esteban Villegas", "Marina Bazán", "Alberto Muñoz", "Teresa Agüero",
]

DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com.ar", "outlook.com", "live.com.ar"]
ESTADOS_CLIENTE = ["activo"] * 90 + ["inactivo"] * 8 + ["suspendido"] * 2

ESTADOS_PEDIDO = ["entregado"] * 60 + ["en_camino"] * 15 + ["preparando"] * 10 + ["cancelado"] * 10 + ["devuelto"] * 5

COURIER_ESTADOS = {
    "entregado": ["preparando", "despachado", "en_camino", "en_sucursal", "entregado"],
    "en_camino": ["preparando", "despachado", "en_camino"],
    "preparando": ["preparando"],
    "cancelado": [],
    "devuelto": ["preparando", "despachado", "en_camino", "devuelto"],
}

RECLAMO_TIPOS = ["demora_envio", "producto_dañado", "faltante", "garantia", "error_facturacion", "cambio_producto"]
RECLAMO_ESTADOS = ["abierto", "en_proceso", "resuelto", "cerrado"]

# ============================================================
# GENERACION DE DATOS
# ============================================================

# --- CLIENTES ---
clientes = []
for i in range(1, N_CLIENTES + 1):
    nombre = NOMBRES[i - 1] if i <= len(NOMBRES) else f"Cliente Test {i}"
    email_name = nombre.lower().replace(" ", ".").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    email = f"{email_name}@{random.choice(DOMINIOS)}"
    ciudad = random.choice(CIUDADES)
    fecha_alta = FECHA_INICIO - timedelta(days=random.randint(30, 365))
    estado = random.choice(ESTADOS_CLIENTE)
    telefono = f"+54 9 {random.randint(11,381)} {random.randint(1000,9999)}-{random.randint(1000,9999)}"
    clientes.append({
        "id": i, "nombre": nombre, "email": email, "ciudad": ciudad,
        "fecha_alta": fecha_alta.strftime("%Y-%m-%d"), "estado": estado, "telefono": telefono
    })

# Meter algunos emails duplicados (data sucia)
for idx in [15, 30, 45]:
    clientes[idx]["email"] = clientes[idx - 1]["email"]

# --- PRODUCTOS ---
productos = []
prod_id = 1
for cat, items in CATEGORIAS.items():
    for nombre, precio in items:
        activo = 1 if random.random() < 0.9 else 0
        stock = random.randint(0, 200) if activo else 0
        costo = int(precio * random.uniform(0.45, 0.65))
        codigo_proveedor = f"PROV-{prod_id:04d}"
        productos.append({
            "id": prod_id, "nombre": nombre, "categoria": cat,
            "precio_unitario": precio, "stock": stock, "activo": activo,
            "costo": costo, "codigo_proveedor": codigo_proveedor
        })
        prod_id += 1

N_PRODUCTOS = len(productos)

# --- PEDIDOS + DETALLES ---
pedidos = []
detalles = []
detalle_id = 1

for ped_id in range(1, N_PEDIDOS + 1):
    cliente = random.choice([c for c in clientes if c["estado"] == "activo"])
    dias = random.randint(0, (FECHA_FIN - FECHA_INICIO).days)
    fecha = FECHA_INICIO + timedelta(days=dias)
    # Estacionalidad: más pedidos en noviembre (Black Friday)
    if fecha.month == 11:
        if random.random() < 0.3:
            fecha = fecha  # keep
    estado = random.choice(ESTADOS_PEDIDO)
    metodo_pago = random.choices(METODOS_PAGO, weights=METODOS_PAGO_WEIGHTS, k=1)[0]

    n_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5], k=1)[0]
    total_pedido = 0
    items_pedido = []

    productos_elegidos = random.sample(productos, min(n_items, len(productos)))
    for prod in productos_elegidos:
        cantidad = random.randint(1, 3)
        precio = prod["precio_unitario"]
        # Descuento random (0%, 5%, 10%, 15%)
        descuento = random.choice([0, 0, 0, 0.05, 0.10, 0.15])
        subtotal = int(cantidad * precio * (1 - descuento))
        total_pedido += subtotal

        detalles.append({
            "id": detalle_id, "pedido_id": ped_id, "producto_id": prod["id"],
            "cantidad": cantidad, "precio_unitario": precio,
            "descuento": descuento, "subtotal": subtotal
        })
        detalle_id += 1

    pedidos.append({
        "id": ped_id, "cliente_id": cliente["id"], "fecha": fecha.strftime("%Y-%m-%d"),
        "estado": estado, "metodo_pago": metodo_pago, "total": total_pedido
    })


# ============================================================
# GENERAR init.sql
# ============================================================
print("Generando MySQL init.sql...")
sql_lines = []
sql_lines.append("CREATE DATABASE IF NOT EXISTS techstore;")
sql_lines.append("USE techstore;")
sql_lines.append("")

# Clientes
sql_lines.append("""CREATE TABLE clientes (
    cliente_id INT PRIMARY KEY,
    nombre VARCHAR(100),
    email VARCHAR(100),
    ciudad VARCHAR(50),
    fecha_alta DATE,
    estado VARCHAR(20),
    telefono VARCHAR(30)
);""")
sql_lines.append("")
for c in clientes:
    nombre = c["nombre"].replace("'", "''")
    sql_lines.append(
        f"INSERT INTO clientes VALUES ({c['id']}, '{nombre}', '{c['email']}', "
        f"'{c['ciudad']}', '{c['fecha_alta']}', '{c['estado']}', '{c['telefono']}');"
    )

sql_lines.append("")

# Productos
sql_lines.append("""CREATE TABLE productos (
    producto_id INT PRIMARY KEY,
    nombre VARCHAR(100),
    categoria VARCHAR(50),
    precio_unitario DECIMAL(12,2),
    stock INT,
    activo TINYINT,
    costo DECIMAL(12,2),
    codigo_proveedor VARCHAR(20)
);""")
sql_lines.append("")
for p in productos:
    nombre = p["nombre"].replace("'", "''")
    sql_lines.append(
        f"INSERT INTO productos VALUES ({p['id']}, '{nombre}', '{p['categoria']}', "
        f"{p['precio_unitario']}, {p['stock']}, {p['activo']}, {p['costo']}, '{p['codigo_proveedor']}');"
    )

sql_lines.append("")

# Pedidos
sql_lines.append("""CREATE TABLE pedidos (
    pedido_id INT PRIMARY KEY,
    cliente_id INT,
    fecha DATE,
    estado VARCHAR(20),
    metodo_pago VARCHAR(30),
    total DECIMAL(14,2),
    FOREIGN KEY (cliente_id) REFERENCES clientes(cliente_id)
);""")
sql_lines.append("")
for p in pedidos:
    sql_lines.append(
        f"INSERT INTO pedidos VALUES ({p['id']}, {p['cliente_id']}, '{p['fecha']}', "
        f"'{p['estado']}', '{p['metodo_pago']}', {p['total']});"
    )

sql_lines.append("")

# Detalle pedidos
sql_lines.append("""CREATE TABLE detalle_pedidos (
    detalle_id INT PRIMARY KEY,
    pedido_id INT,
    producto_id INT,
    cantidad INT,
    precio_unitario DECIMAL(12,2),
    descuento DECIMAL(4,2),
    subtotal DECIMAL(14,2),
    FOREIGN KEY (pedido_id) REFERENCES pedidos(pedido_id),
    FOREIGN KEY (producto_id) REFERENCES productos(producto_id)
);""")
sql_lines.append("")
for d in detalles:
    sql_lines.append(
        f"INSERT INTO detalle_pedidos VALUES ({d['id']}, {d['pedido_id']}, {d['producto_id']}, "
        f"{d['cantidad']}, {d['precio_unitario']}, {d['descuento']}, {d['subtotal']});"
    )

sql_path = BASE / "mysql" / "init.sql"
sql_path.parent.mkdir(parents=True, exist_ok=True)
with open(sql_path, "w", encoding="utf-8") as f:
    f.write("\n".join(sql_lines))
print(f"  -> {sql_path} ({len(clientes)} clientes, {len(productos)} productos, {len(pedidos)} pedidos, {len(detalles)} detalles)")


# ============================================================
# GENERAR SFTP FILES
# ============================================================
sftp_dir = BASE / "sftp" / "data"
sftp_dir.mkdir(parents=True, exist_ok=True)

# --- pagos_banco.csv (transferencias y débitos) ---
print("Generando pagos_banco.csv...")
pagos_banco = []
for ped in pedidos:
    if ped["metodo_pago"] in ("transferencia", "tarjeta_debito") and ped["estado"] != "cancelado":
        fecha_pago = datetime.strptime(ped["fecha"], "%Y-%m-%d") + timedelta(days=random.randint(0, 2))
        banco = random.choice(["Galicia", "Santander", "BBVA", "Macro", "Nación", "HSBC", "ICBC"])
        cbu = f"0{random.randint(10,99)}0{random.randint(1000000000, 9999999999)}00{random.randint(10000000, 99999999)}"
        estado_pago = "acreditado" if ped["estado"] in ("entregado", "en_camino") else "pendiente"
        comision = round(ped["total"] * 0.008, 2)  # 0.8% comisión bancaria
        pagos_banco.append({
            "pago_id": f"PAG-{len(pagos_banco)+1:05d}",
            "pedido_id": ped["id"],
            "fecha_pago": fecha_pago.strftime("%Y-%m-%d"),
            "monto": ped["total"],
            "banco": banco,
            "cbu": cbu,
            "estado": estado_pago,
            "comision_bancaria": comision,
            "tipo": ped["metodo_pago"]
        })

csv_path = sftp_dir / "pagos_banco.csv"
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=pagos_banco[0].keys())
    writer.writeheader()
    writer.writerows(pagos_banco)
print(f"  -> {csv_path} ({len(pagos_banco)} pagos)")


# --- envios_courier.json ---
print("Generando envios_courier.json...")
envios = []
for ped in pedidos:
    if ped["estado"] in ("entregado", "en_camino", "devuelto"):
        cliente = next(c for c in clientes if c["id"] == ped["cliente_id"])
        fecha_base = datetime.strptime(ped["fecha"], "%Y-%m-%d")

        tracking_history = []
        estados_envio = COURIER_ESTADOS.get(ped["estado"], [])
        for i, est in enumerate(estados_envio):
            tracking_history.append({
                "estado": est,
                "fecha": (fecha_base + timedelta(days=i + 1)).strftime("%Y-%m-%d %H:%M:%S"),
                "ubicacion": random.choice(["Centro Distribución CABA", "Sucursal " + cliente["ciudad"], "En tránsito"])
            })

        peso = round(random.uniform(0.2, 15.0), 2)
        costo_envio = round(peso * random.uniform(500, 1500), 0)

        envios.append({
            "envio_id": f"ENV-{len(envios)+1:05d}",
            "pedido_id": ped["id"],
            "courier": random.choice(["Andreani", "OCA", "Correo Argentino", "Via Cargo"]),
            "tracking_code": f"AR{random.randint(100000000, 999999999)}",
            "destino_ciudad": cliente["ciudad"],
            "peso_kg": peso,
            "costo_envio": costo_envio,
            "estado_actual": ped["estado"],
            "fecha_estimada_entrega": (fecha_base + timedelta(days=random.randint(3, 10))).strftime("%Y-%m-%d"),
            "tracking_history": tracking_history
        })

json_path = sftp_dir / "envios_courier.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(envios, f, ensure_ascii=False, indent=2)
print(f"  -> {json_path} ({len(envios)} envios)")


# --- catalogo_proveedor.xml ---
print("Generando catalogo_proveedor.xml...")
xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<catalogo proveedor="TechDistribuidor SA" fecha="2025-12-01">']
for p in productos:
    nombre_xml = p["nombre"].replace("&", "&amp;").replace("'", "&apos;").replace('"', "&quot;")
    xml_lines.append(f'  <producto>')
    xml_lines.append(f'    <codigo>{p["codigo_proveedor"]}</codigo>')
    xml_lines.append(f'    <nombre>{nombre_xml}</nombre>')
    xml_lines.append(f'    <categoria>{p["categoria"]}</categoria>')
    xml_lines.append(f'    <precio_lista>{p["precio_unitario"]}</precio_lista>')
    xml_lines.append(f'    <precio_costo>{p["costo"]}</precio_costo>')
    xml_lines.append(f'    <stock_proveedor>{random.randint(10, 500)}</stock_proveedor>')
    xml_lines.append(f'    <disponible>{"si" if p["activo"] else "no"}</disponible>')
    xml_lines.append(f'    <marca>{nombre_xml.split()[0]}</marca>')
    xml_lines.append(f'  </producto>')
xml_lines.append('</catalogo>')

xml_path = sftp_dir / "catalogo_proveedor.xml"
with open(xml_path, "w", encoding="utf-8") as f:
    f.write("\n".join(xml_lines))
print(f"  -> {xml_path} ({len(productos)} productos)")


# --- liquidacion_mp.xlsx (generamos como CSV porque openpyxl puede no estar, el pipeline lo leerá) ---
# Usamos openpyxl si está disponible, sino CSV como fallback
print("Generando liquidacion_mp...")
liquidaciones = []
for ped in pedidos:
    if ped["metodo_pago"] == "mercadopago" and ped["estado"] != "cancelado":
        fecha_liq = datetime.strptime(ped["fecha"], "%Y-%m-%d") + timedelta(days=random.randint(1, 5))
        comision_mp = round(ped["total"] * random.uniform(0.045, 0.065), 2)  # 4.5-6.5%
        iva_comision = round(comision_mp * 0.21, 2)
        retencion_iibb = round(ped["total"] * 0.02, 2) if random.random() < 0.3 else 0
        neto = round(ped["total"] - comision_mp - iva_comision - retencion_iibb, 2)

        liquidaciones.append({
            "operacion_mp": f"MP-{random.randint(10000000, 99999999)}",
            "pedido_id": ped["id"],
            "fecha_operacion": ped["fecha"],
            "fecha_liquidacion": fecha_liq.strftime("%Y-%m-%d"),
            "monto_bruto": ped["total"],
            "comision_mp": comision_mp,
            "iva_comision": iva_comision,
            "retencion_iibb": retencion_iibb,
            "monto_neto": neto,
            "estado": random.choice(["liquidado", "liquidado", "liquidado", "retenido"]),
            "cuotas": random.choice([1, 1, 1, 3, 6, 12])
        })

try:
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Liquidaciones"
    headers = list(liquidaciones[0].keys())
    ws.append(headers)
    for liq in liquidaciones:
        ws.append(list(liq.values()))
    xlsx_path = sftp_dir / "liquidacion_mp.xlsx"
    wb.save(str(xlsx_path))
    print(f"  -> {xlsx_path} ({len(liquidaciones)} liquidaciones) [XLSX]")
except ImportError:
    # Fallback a CSV
    csv_liq_path = sftp_dir / "liquidacion_mp.csv"
    with open(csv_liq_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=liquidaciones[0].keys())
        writer.writeheader()
        writer.writerows(liquidaciones)
    print(f"  -> {csv_liq_path} ({len(liquidaciones)} liquidaciones) [CSV fallback - instalar openpyxl para XLSX]")


# --- reclamos.txt (pipe-delimited) ---
print("Generando reclamos.txt...")
reclamos = []
pedidos_entregados = [p for p in pedidos if p["estado"] in ("entregado", "devuelto")]
pedidos_reclamo = random.sample(pedidos_entregados, min(80, len(pedidos_entregados)))

for i, ped in enumerate(pedidos_reclamo, 1):
    fecha_ped = datetime.strptime(ped["fecha"], "%Y-%m-%d")
    fecha_reclamo = fecha_ped + timedelta(days=random.randint(3, 30))
    tipo = random.choice(RECLAMO_TIPOS)
    estado = random.choice(RECLAMO_ESTADOS)
    prioridad = random.choice(["alta", "media", "media", "baja"])

    comentarios = {
        "demora_envio": "El pedido no llegó en la fecha estimada",
        "producto_dañado": "El producto llegó con golpes en la caja",
        "faltante": "Falta un item del pedido",
        "garantia": "El producto dejó de funcionar",
        "error_facturacion": "La factura tiene datos incorrectos",
        "cambio_producto": "Quiero cambiar por otro modelo/color",
    }

    reclamos.append(
        f"{i}|{ped['id']}|{ped['cliente_id']}|{fecha_reclamo.strftime('%Y-%m-%d')}|"
        f"{tipo}|{estado}|{prioridad}|{comentarios[tipo]}"
    )

txt_path = sftp_dir / "reclamos.txt"
with open(txt_path, "w", encoding="utf-8") as f:
    f.write("reclamo_id|pedido_id|cliente_id|fecha_reclamo|tipo|estado|prioridad|comentario\n")
    f.write("\n".join(reclamos))
print(f"  -> {txt_path} ({len(reclamos)} reclamos)")


# ============================================================
# RESUMEN
# ============================================================
print(f"\n{'='*60}")
print("RESUMEN DE DATOS GENERADOS")
print(f"{'='*60}")
print(f"  MySQL:")
print(f"    clientes:        {len(clientes):>6} rows")
print(f"    productos:       {len(productos):>6} rows")
print(f"    pedidos:         {len(pedidos):>6} rows")
print(f"    detalle_pedidos: {len(detalles):>6} rows")
print(f"  SFTP:")
print(f"    pagos_banco.csv:       {len(pagos_banco):>6} rows")
print(f"    envios_courier.json:   {len(envios):>6} rows")
print(f"    catalogo_proveedor.xml:{len(productos):>6} rows")
print(f"    liquidacion_mp:        {len(liquidaciones):>6} rows")
print(f"    reclamos.txt:          {len(reclamos):>6} rows")
print(f"  TOTAL:                   {len(clientes)+len(productos)+len(pedidos)+len(detalles)+len(pagos_banco)+len(envios)+len(productos)+len(liquidaciones)+len(reclamos):>6} rows")
print(f"{'='*60}")
