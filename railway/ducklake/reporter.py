"""Reporter: imprime reportes bonitos desde la capa CONSUME."""

import duckdb
import pandas as pd


class Reporter:
    """Genera reportes en consola desde las tablas CONSUME de DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def print_all(self):
        print(f"\n{'=' * 70}")
        print("  REPORTES FINALES (desde CONSUME layer)")
        print(f"{'=' * 70}")
        self._revenue_mensual()
        self._top_clientes()
        self._ventas_por_categoria()
        self._conciliacion_pagos()
        self._performance_envios()
        self._alertas_stock()
        self._analisis_reclamos()

    def _revenue_mensual(self):
        print(f"\n  REVENUE MENSUAL:")
        print(f"  {'Mes':<12} {'Pedidos':>8} {'Revenue':>14} {'Ticket Prom':>12} {'Clientes':>9}")
        print(f"  {'-'*55}")
        df = self.conn.execute("SELECT * FROM consume.revenue_mensual ORDER BY mes").fetchdf()
        for _, row in df.iterrows():
            mes = str(row["mes"])[:7] if pd.notna(row["mes"]) else "N/A"
            print(f"  {mes:<12} {int(row['pedidos']):>8} ${int(row['revenue']):>12,} "
                  f"{int(row['ticket_promedio']):>11,} {int(row['clientes_unicos']):>9}")

    def _top_clientes(self):
        print(f"\n  TOP 10 CLIENTES:")
        print(f"  {'Cliente':<25} {'Ciudad':<16} {'Compras':>8} {'Total Gastado':>14}")
        print(f"  {'-'*63}")
        df = self.conn.execute("SELECT * FROM consume.top_clientes LIMIT 10").fetchdf()
        for _, row in df.iterrows():
            print(f"  {str(row['cliente'])[:24]:<25} {str(row['ciudad'])[:15]:<16} "
                  f"{int(row['compras']):>8} ${int(row['total_gastado']):>12,}")

    def _ventas_por_categoria(self):
        print(f"\n  VENTAS POR CATEGORIA:")
        print(f"  {'Categoria':<16} {'Pedidos':>8} {'Unidades':>9} {'Revenue':>14} {'Margen%':>8}")
        print(f"  {'-'*55}")
        df = self.conn.execute("SELECT * FROM consume.ventas_por_categoria").fetchdf()
        for _, row in df.iterrows():
            print(f"  {str(row['categoria'])[:15]:<16} {int(row['pedidos']):>8} "
                  f"{int(row['unidades']):>9} ${int(row['revenue']):>12,} "
                  f"{row['margen_promedio_pct']:>7.1f}%")

    def _conciliacion_pagos(self):
        print(f"\n  CONCILIACION DE PAGOS:")
        print(f"  {'Metodo':<18} {'Txns':>6} {'Monto Pedidos':>14} {'Conciliado%':>12}")
        print(f"  {'-'*50}")
        df = self.conn.execute("SELECT * FROM consume.conciliacion_pagos").fetchdf()
        for _, row in df.iterrows():
            print(f"  {str(row['metodo_pago'])[:17]:<18} {int(row['transacciones']):>6} "
                  f"${int(row['monto_pedidos']):>12,} {row['pct_conciliado']:>11.1f}%")

    def _performance_envios(self):
        print(f"\n  PERFORMANCE ENVIOS:")
        print(f"  {'Courier':<18} {'Envios':>7} {'Costo Prom':>11} {'Entregados':>11} "
              f"{'Devueltos':>10} {'Dev%':>6}")
        print(f"  {'-'*63}")
        df = self.conn.execute("SELECT * FROM consume.performance_envios").fetchdf()
        for _, row in df.iterrows():
            print(f"  {str(row['courier'])[:17]:<18} {int(row['envios']):>7} "
                  f"${int(row['costo_promedio']):>9,} {int(row['entregados']):>11} "
                  f"{int(row['devueltos']):>10} {row['pct_devolucion']:>5.1f}%")

    def _alertas_stock(self):
        print(f"\n  ALERTAS DE STOCK:")
        df = self.conn.execute("SELECT * FROM consume.alertas_stock").fetchdf()
        sin_stock = len(df[df["alerta_stock"] == "SIN_STOCK"])
        critico = len(df[df["alerta_stock"] == "CRITICO"])
        bajo = len(df[df["alerta_stock"] == "BAJO"])
        print(f"  SIN STOCK: {sin_stock} productos | CRITICO (<10): {critico} | BAJO (<30): {bajo}")

    def _analisis_reclamos(self):
        print(f"\n  ANALISIS RECLAMOS:")
        print(f"  {'Tipo':<20} {'Cantidad':>9} {'Resueltos':>10} {'Abiertos':>9} {'Resolucion%':>12}")
        print(f"  {'-'*60}")
        df = self.conn.execute("""
            SELECT tipo, SUM(cantidad) AS cantidad, SUM(resueltos) AS resueltos,
                   SUM(abiertos) AS abiertos,
                   ROUND(SUM(resueltos)*100.0/SUM(cantidad),1) AS pct
            FROM consume.analisis_reclamos
            GROUP BY tipo ORDER BY cantidad DESC
        """).fetchdf()
        for _, row in df.iterrows():
            print(f"  {str(row['tipo'])[:19]:<20} {int(row['cantidad']):>9} "
                  f"{int(row['resueltos']):>10} {int(row['abiertos']):>9} {row['pct']:>11.1f}%")
