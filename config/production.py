# integracion_worldsys_bcra_production

import os
# Primero importar todas las variables que contiene default.py
from .default import *

MYSQL_HOST = os.environ.get('MYSQL_HOST') or "msql_host_que_corresponda"
MYSQL_DB = os.environ.get('MYSQL_DB') or "msql_db_que_corresponda"
MYSQL_USER = os.environ.get('MYSQL_USER') or "msql_user_que_corresponda"
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD') or "msql_password_que_corresponda"