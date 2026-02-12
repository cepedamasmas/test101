FROM atmoz/sftp:latest

# Copiar los archivos de datos al container
COPY data/ /home/techstore/upload/

# Crear usuario techstore con password techstore123 y UID 1001
CMD ["techstore:techstore123:1001"]
