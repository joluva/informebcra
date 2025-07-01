# Script de control de facturacion
# Las funciones verifican que esten las facturas del periodo solicitado
# Si estan en la vista de cendeu, la inserta en la tabla bcra_factura de la base infobcra
#import pdb; pdb.set_trace()
import sys
import pymysql
import mysql.connector
import os
from programas import errores
from os.path import abspath, dirname, join
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

from config.default import SQLALCHEMY_DATABASE_BCRA

# armo las conexiones a las distintas bases de datos
conn = create_engine(SQLALCHEMY_DATABASE_BCRA)

# Verifico si ya está la facturacion correspondiente al mes de proceso de bcra
def get_factura(fyear, fmonth):
    contador=0
    sql="""
    SELECT COUNT(*) cantidad
    FROM cendeu.vw_facturacioncf f
    WHERE YEAR(f.fechaFacturacion) = '{}'
    AND MONTH(f.fechaFacturacion) = '{}' 
    """.format(fyear, fmonth)

    try:
        with conn.connect() as connection:
            cantidad=connection.execute(sql)
            for c in cantidad:
                print("A PROCESAR FACTURAS: ",c['cantidad'])
            #print('LA CONSULTA DE FACTURA EN cendeu.vw_facturacioncf SE PRDUJO CON EXITO')
            errores.logger.info('LA CONSULTA DE FACTURA EN cendeu.vw_facturacioncf SE PRDUJO CON EXITO')
    except Exception as e:
        #print('SE PRODUJO UN ERROR EN LA CONSULTA DE FACTURA cendeu.vw_facturacioncf ', e)
        errores.logger.error('SE PRODUJO UN ERROR EN LA CONSULTA DE FACTURA cendeu.vw_facturacioncf ', e)
        #sys.exit()
    
    return c['cantidad']

# Verifico si la facturacion del periodo solicitado ya esta ingresada en infobcra.bcra_factura
def get_factura_bcra(fyear, fmonth):
    sql="""
    SELECT COUNT(*) cantidad
    FROM infobcra.bcra_factura bf
    WHERE YEAR(bf.fec_factura) = '{}'
    AND MONTH(bf.fec_factura) = '{}'
    """.format(fyear, fmonth)

    try:
        with conn.connect() as connection:
            cantidad=connection.execute(sql)
            for c in cantidad:
                print("FACTURACION EN bcra_factura", c['cantidad'])
                errores.logger.info('LA CONSULTA DE FACTURACION EN bcra_factura SE PRODUJO CON EXITO EXITO')
    except Exception as e:
        errores.logger.error('SE PRODUJO UN ERROR AL CONSILTAR infobcra.bcra_factura ', e)
    return c['cantidad']


# Inserto las facturas en la tabla de infobra.bcra_factura
def put_factura(fyear, fmonth):
    sql="""
    INSERT INTO infobcra.bcra_factura(infobcra.bcra_factura.id_comercio,
                                      infobcra.bcra_factura.nro_credito,
                                      infobcra.bcra_factura.nro_documento,
                                      infobcra.bcra_factura.ape_nom,
                                      infobcra.bcra_factura.fec_factura)

    SELECT vf.codigoComercio ,
    vf.NumeroCredito ,
    vf.documento ,
    SUBSTRING(dc.Nombre, 1, 30),
    vf.fechaFacturacion 
    FROM cendeu.vw_facturacioncf vf 
    INNER JOIN credixware.datos_clientes dc ON vf.Documento = dc.Documento 
    WHERE year(vf.fechaFacturacion) = '{}'
    AND month(vf.fechaFacturacion) = '{}'
    """.format(fyear, fmonth)


    #WHERE vf.fechaFacturacion BETWEEN '{}' AND '{}'
    try:
        session = Session(conn)
        session.begin()
        conn.execute(sql)

        print('LA CARGA DE FACTURA EN infobcra.bcra_factura SE PRDUJO CON EXITO')
        #conn.commit()
        session.commit()
    except Exception as e:
        print('SE PRODUJO UN ERRO EN LA CARGA DE FACTURA infobcra.bcra_factura ', e)
        #conn.rollback()
        session.rollback()
        sys.exit()

