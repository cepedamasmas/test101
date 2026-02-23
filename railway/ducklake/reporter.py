"""Reporter: imprime reportes desde la capa CONSUME."""

import duckdb


class Reporter:
    """Genera reportes en consola desde las tablas CONSUME de DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def print_all(self) -> None:
        print(f"\n{'=' * 70}")
        print("  REPORTES FINALES (desde CONSUME layer)")
        print(f"{'=' * 70}")
        self._ventas_por_canal()
        self._ventas_diarias()
        self._top_productos()
        self._clientes_resumen()

    def _ventas_por_canal(self) -> None:
        print(f"\n  VENTAS POR CANAL:")
        print(f"  {'Canal':<20} {'Pedidos':>8} {'Clientes':>9} {'Revenue':>14} {'Ticket Prom':>12}")
        print(f"  {'-' * 63}")
        df = self.conn.execute(
            "SELECT * FROM consume.ventas_por_canal ORDER BY revenue DESC"
        ).fetchdf()
        for _, row in df.iterrows():
            print(
                f"  {str(row['canal'])[:19]:<20} {int(row['pedidos']):>8} "
                f"{int(row['clientes_unicos']):>9} ${float(row['revenue']):>12,.0f} "
                f"${float(row['ticket_promedio']):>10,.0f}"
            )

    def _ventas_diarias(self) -> None:
        print(f"\n  VENTAS DIARIAS (últimos 7 días):")
        print(f"  {'Fecha':<12} {'Canal':<20} {'Pedidos':>8} {'Revenue':>14}")
        print(f"  {'-' * 54}")
        df = self.conn.execute(
            "SELECT * FROM consume.ventas_diarias ORDER BY fecha DESC, canal LIMIT 14"
        ).fetchdf()
        for _, row in df.iterrows():
            fecha = str(row["fecha"])[:10] if row["fecha"] is not None else "N/A"
            print(
                f"  {fecha:<12} {str(row['canal'])[:19]:<20} "
                f"{int(row['pedidos']):>8} ${float(row['revenue']):>12,.0f}"
            )

    def _top_productos(self) -> None:
        print(f"\n  TOP 10 PRODUCTOS (por revenue):")
        print(f"  {'Producto':<30} {'Pedidos':>8} {'Unidades':>9} {'Revenue':>14} {'Rank':>5}")
        print(f"  {'-' * 66}")
        df = self.conn.execute(
            "SELECT * FROM consume.top_productos ORDER BY rank_revenue LIMIT 10"
        ).fetchdf()
        for _, row in df.iterrows():
            print(
                f"  {str(row['producto_id'])[:29]:<30} {int(row['pedidos']):>8} "
                f"{int(row['unidades_vendidas']):>9} ${float(row['revenue']):>12,.0f} "
                f"{int(row['rank_revenue']):>5}"
            )

    def _clientes_resumen(self) -> None:
        print(f"\n  TOP 10 CLIENTES (por LTV):")
        print(f"  {'Cliente':<20} {'Pedidos':>8} {'LTV':>14} {'Recencia (días)':>16}")
        print(f"  {'-' * 58}")
        df = self.conn.execute(
            "SELECT * FROM consume.clientes_resumen ORDER BY ltv DESC LIMIT 10"
        ).fetchdf()
        for _, row in df.iterrows():
            recencia = int(row["recencia_dias"]) if row["recencia_dias"] is not None else -1
            print(
                f"  {str(row['cliente_id'])[:19]:<20} {int(row['cantidad_pedidos']):>8} "
                f"${float(row['ltv']):>12,.0f} {recencia:>16}"
            )
