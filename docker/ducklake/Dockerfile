FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline_ecommerce.py .

# Esperar a que MySQL y SFTP estén listos
COPY wait_and_run.sh .
RUN chmod +x wait_and_run.sh

CMD ["./wait_and_run.sh"]
