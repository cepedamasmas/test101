"""
DuckLake Dashboard - Status y métricas del pipeline
FastAPI minimal para Railway
"""

import os
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text

app = FastAPI(title="DuckLake Dashboard", version="1.0")

PG_CONFIG = {
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": os.environ.get("PG_PORT", "5432"),
    "user": os.environ.get("PG_USER", "techstore"),
    "password": os.environ.get("PG_PASSWORD", "techstore123"),
    "database": os.environ.get("PG_DATABASE", "techstore_lake"),
}

pg_url = (
    f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
    f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
)


def get_engine():
    return create_engine(pg_url, pool_pre_ping=True, pool_size=2)


def query_safe(engine, sql):
    """Ejecuta query y retorna lista de dicts, o None si falla."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            cols = list(result.keys())
            return [dict(zip(cols, row)) for row in result.fetchall()]
    except Exception:
        return None


def get_layer_tables(engine):
    """Retorna conteo de tablas y filas por schema."""
    layers = {}
    for schema in ["raw", "staging", "consume"]:
        tables = query_safe(engine, f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER BY table_name
        """)
        if tables is None:
            layers[schema] = {"status": "error", "tables": []}
            continue

        table_info = []
        for t in tables:
            name = t["table_name"]
            count = query_safe(engine, f'SELECT COUNT(*) AS n FROM {schema}."{name}"')
            rows = count[0]["n"] if count else "?"
            table_info.append({"name": name, "rows": rows})
        layers[schema] = {"status": "ok", "tables": table_info}
    return layers


STYLE = """
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f172a; color: #e2e8f0; padding: 24px; }
  .header { display: flex; align-items: center; gap: 16px; margin-bottom: 32px; }
  .header h1 { font-size: 28px; color: #f8fafc; }
  .header .badge { background: #22c55e; color: #000; padding: 4px 12px;
                   border-radius: 12px; font-size: 13px; font-weight: 600; }
  .header .badge.error { background: #ef4444; color: #fff; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 20px; }
  .card { background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; }
  .card h2 { font-size: 16px; color: #94a3b8; margin-bottom: 12px; text-transform: uppercase;
             letter-spacing: 1px; }
  .card h2 .count { color: #38bdf8; font-size: 14px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #334155; font-size: 14px; }
  th { color: #64748b; font-weight: 500; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; color: #38bdf8; }
  .metric { text-align: center; padding: 16px; }
  .metric .value { font-size: 36px; font-weight: 700; color: #38bdf8; }
  .metric .label { font-size: 13px; color: #64748b; margin-top: 4px; }
  .footer { margin-top: 32px; text-align: center; color: #475569; font-size: 13px; }
  a { color: #38bdf8; text-decoration: none; }
</style>
"""


@app.get("/health")
def health():
    """Health check para Railway."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return {"status": "healthy", "database": "connected", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "database": str(e), "timestamp": datetime.utcnow().isoformat()}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    engine = get_engine()

    # Check DB connection
    db_ok = query_safe(engine, "SELECT 1 AS ok") is not None

    # Get layers info
    layers = get_layer_tables(engine) if db_ok else {}

    # Get key metrics from consume layer
    revenue = query_safe(engine, """
        SELECT SUM(revenue) AS total_revenue, SUM(pedidos) AS total_pedidos,
               ROUND(AVG(ticket_promedio)) AS avg_ticket
        FROM consume.revenue_mensual
    """) if db_ok else None

    stock_alerts = query_safe(engine, """
        SELECT alerta_stock, COUNT(*) AS n FROM consume.alertas_stock GROUP BY alerta_stock
    """) if db_ok else None

    engine.dispose()

    # Total rows across all layers
    total_tables = sum(len(l["tables"]) for l in layers.values())
    total_rows = sum(
        t["rows"] for l in layers.values() for t in l["tables"] if isinstance(t["rows"], int)
    )

    status_badge = '<span class="badge">ONLINE</span>' if db_ok else '<span class="badge error">OFFLINE</span>'

    # Build metrics section
    metrics_html = ""
    if revenue and revenue[0]["total_revenue"]:
        r = revenue[0]
        metrics_html = f"""
        <div class="grid" style="grid-template-columns: repeat(3, 1fr); margin-bottom: 20px;">
            <div class="card metric">
                <div class="value">${int(r['total_revenue']):,}</div>
                <div class="label">Revenue Total</div>
            </div>
            <div class="card metric">
                <div class="value">{int(r['total_pedidos']):,}</div>
                <div class="label">Pedidos</div>
            </div>
            <div class="card metric">
                <div class="value">${int(r['avg_ticket']):,}</div>
                <div class="label">Ticket Promedio</div>
            </div>
        </div>
        """

    # Build stock alerts
    stock_html = ""
    if stock_alerts:
        for a in stock_alerts:
            color = "#ef4444" if a["alerta_stock"] == "SIN_STOCK" else "#f59e0b" if a["alerta_stock"] == "CRITICO" else "#38bdf8"
            stock_html += f'<span style="color:{color};font-weight:600">{a["alerta_stock"]}: {a["n"]}</span>&nbsp;&nbsp;'

    # Build layer cards
    layers_html = ""
    layer_names = {"raw": "RAW (Bronze)", "staging": "STAGING (Silver)", "consume": "CONSUME (Gold)"}
    layer_colors = {"raw": "#f59e0b", "staging": "#38bdf8", "consume": "#22c55e"}
    for schema, info in layers.items():
        rows_html = ""
        for t in info["tables"]:
            rows_html += f'<tr><td>{t["name"]}</td><td class="num">{t["rows"]:,}</td></tr>'
        layers_html += f"""
        <div class="card">
            <h2 style="color:{layer_colors[schema]}">{layer_names[schema]}
                <span class="count">({len(info['tables'])} tablas)</span></h2>
            <table><tr><th>Tabla</th><th style="text-align:right">Filas</th></tr>
            {rows_html}</table>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>DuckLake Dashboard</title>{STYLE}</head>
<body>
    <div class="header">
        <h1>DuckLake - TechStore</h1>
        {status_badge}
    </div>

    {metrics_html}

    {"<div class='card' style='margin-bottom:20px'><h2>Alertas de Stock</h2><p>" + stock_html + "</p></div>" if stock_html else ""}

    <div class="grid">{layers_html}</div>

    <div class="card" style="margin-top:20px">
        <h2>Resumen</h2>
        <p>{total_tables} tablas &middot; {total_rows:,} filas totales &middot;
           3 capas Medallion &middot; PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}</p>
    </div>

    <div class="footer">
        DuckLake Pipeline Dashboard &middot; {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
        &middot; <a href="/health">/health</a>
    </div>
</body></html>"""

    return HTMLResponse(content=html)
