FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar codigo modular
COPY config.py .
COPY connectors/ connectors/
COPY layers/ layers/
COPY exporters/ exporters/
COPY dagster_pipeline/ dagster_pipeline/
COPY reporter.py .
COPY pipeline.py .

# Copiar dbt project y generar manifest para dagster-dbt
COPY dbt_techstore/ dbt_techstore/
RUN dbt parse --project-dir /app/dbt_techstore --profiles-dir /app/dbt_techstore

# Copiar archivos de soporte
COPY init.sql .
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]
