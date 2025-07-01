import sys
import pymysql
import mysql.connector
import os
from os.path import abspath, dirname, join
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta


# Busco las personas  que tengan problemas con el nombre o cuil entre CENDEU y DEUDORES para armar la tabla
# bcra_deudores_err con datos a corregir 
def grabar_deudores_err(documento):
    pass