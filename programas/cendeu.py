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

from config.default import SQLALCHEMY_DATABASE_BCRA

# armo las conexiones a las distintas bases de datos
connbcra = create_engine(SQLALCHEMY_DATABASE_BCRA)

def get_deudores():
    sql = """
    CREATE TEMPORAY TABLE
    """

# Busco la maxima situacion dentro de cendeu.deudor


def get_cendeu(tipoIden, nroIden):
    sql = """
    SELECT 
    IFNULL(MAX(cendeu.deudor.situacion),'0')
    FROM cendeu.deudor
    WHERE cendeu.deudor.tipoIdentificacion = '{}'
    AND cendeu.deudor.numeroIdentificacion = '{}'
    """.format(tipoIden, nroIden)

    try:
        with connbcra.connect() as connection:
            situacion = connection.execute(sql)

            for s in situacion:
                situacion_crediticia = s

    except Exception as e:
        print("ERROR EN LA CONSULTA DE cendeu.deudor", e)
        situacion_crediticia = '00'

    return situacion_crediticia
