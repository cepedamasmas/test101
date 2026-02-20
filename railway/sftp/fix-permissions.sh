#!/bin/bash
# Este script lo ejecuta atmoz/sftp al arrancar, como root.
# Fija permisos del volume montado para que el usuario techstore (UID 1001) pueda escribir.
chown -R 1001:1001 /home/techstore/upload/ecomm_parquet
chmod -R 755 /home/techstore/upload/ecomm_parquet
