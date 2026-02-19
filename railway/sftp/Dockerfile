FROM atmoz/sftp:latest

# Copiar los archivos de datos al container
COPY data/ /home/techstore/upload/

# Crear carpeta ecomm_parquet con permisos correctos
# (el volume de Railway se monta acá, atmoz/sftp necesita que exista)
RUN mkdir -p /home/techstore/upload/ecomm_parquet && \
    chown -R 1001:1001 /home/techstore/upload/ecomm_parquet && \
    chmod 755 /home/techstore/upload/ecomm_parquet

# Crear usuario techstore con password techstore123 y UID 1001
CMD ["techstore:techstore123:1001"]
