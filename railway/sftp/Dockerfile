FROM atmoz/sftp:latest

# Copiar los archivos de datos al container
COPY data/ /home/techstore/upload/

# Script que atmoz/sftp ejecuta al arrancar (como root), fija permisos del volume montado
COPY fix-permissions.sh /etc/sftp.d/fix-permissions.sh
RUN chmod +x /etc/sftp.d/fix-permissions.sh

# Crear usuario techstore con password techstore123 y UID 1001
CMD ["techstore:techstore123:1001"]
