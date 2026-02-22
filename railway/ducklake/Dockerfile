FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    apache2-utils \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-instalar extensión postgres de DuckDB en build time (evita descarga en runtime)
RUN python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL postgres')"

# Copiar codigo modular
COPY config.py .
COPY connectors/ connectors/
COPY layers/ layers/
COPY exporters/ exporters/
COPY dagster_pipeline/ dagster_pipeline/
COPY reporter.py .
COPY pipeline.py .

# Copiar dbt project (incluye macros) y generar manifest para dagster-dbt
COPY dbt_techstore/ dbt_techstore/
RUN dbt parse --project-dir /app/dbt_techstore --profiles-dir /app/dbt_techstore

# Copiar archivos de soporte
COPY dagster.yaml .
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]
