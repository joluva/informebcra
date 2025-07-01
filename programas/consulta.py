# Script principal de ejecucion de procesos de deudores BCRA
# Aca estan las funciones de consulta - insercion -  calculo y analisis de la imformacion
# Genera dos DataFrame
# 1 - deudores_a_procesar.csv que contiene los datos a analizar
# 2 - cendeu_credifacil.csv archivo de salida final para enviar a worldsys
#import pdb; pdb.set_trace
import sys
import pymysql
import mysql.connector
import time
import os
import os.path as path
from programas import errores
from programas import verificore
from programas import configuraciones

from os.path import abspath, dirname, join
from mysql.connector import Error
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta

#from config.configuraciones import RUTA, ARCHIVO_DEUDORES, ARCHIVO_TRABAJO
from config.default import SQLALCHEMY_DATABASE_BCRA
#from config.configuraciones import ARCHIVO_DEUDORES, ARCHIVO_TRABAJO, RUTA

#armo las conexiones a las distintas bases de datos
connbcra = create_engine(SQLALCHEMY_DATABASE_BCRA)

# Agrupo los creditos mooros por documento y nombre
def get_deudores(fecdes, fechas):
    sql="""
    SELECT  
    '11',
    CAST(credixware.datos_clientes.documento AS CHAR) bcraiden,
    UPPER(credixware.datos_clientes.nombre) bcradenom,
    CASE 
    WHEN MAX(creditosmorosos.max_dias_vencido_cred) < 31 THEN '01'
    WHEN MAX(creditosmorosos.max_dias_vencido_cred) > 30 AND MAX(creditosmorosos.max_dias_vencido_cred) < 61 THEN '21'
    WHEN MAX(creditosmorosos.max_dias_vencido_cred) > 60 AND MAX(creditosmorosos.max_dias_vencido_cred) < 91 THEN '03'
    WHEN MAX(creditosmorosos.max_dias_vencido_cred) > 90 AND MAX(creditosmorosos.max_dias_vencido_cred) < 121 THEN '04'
    ELSE '05'
     END situacion,
    '09' tipoasis,
    round(sum(credixware.creditosmorosos.deuda_cred),0) deuda,
    '0' deuenc,
    '0' recat,
    case 
    when max(creditosmorosos.max_dias_vencido_cred) < 31 then '00'
    else creditosmorosos.max_dias_vencido_cred
    end atraso,
    max(creditosmorosos.tasa_interes_cred) tasa,
    '0' situsin,
    '1' presper,
    UPPER(SUBSTRING(credixware.datos_clientes.nombre,1,3)) letras     
    FROM credixware.creditosmorosos 
    INNER JOIN credixware.datos_clientes ON credixware.creditosmorosos.idPersona = credixware.datos_clientes.Id 
    INNER JOIN infobcra.bcra_factura  ON credixware.creditosmorosos.NumeroCredito = infobcra.bcra_factura.nro_credito 
    WHERE infobcra.bcra_factura.fec_factura BETWEEN '{}' AND '{}'
    AND credixware.datos_clientes.Documento NOT IN 
    (SELECT infobcra.bcra_judi.Documento 
    FROM infobcra.bcra_judi 
    WHERE credixware.datos_clientes.documento = infobcra.bcra_judi.Documento)
    AND credixware.datos_clientes.documento NOT IN 
    (SELECT infobcra.indi_bolp.nro_documento 
    FROM infobcra.indi_bolp 
    WHERE infobcra.indi_bolp.nro_documento = credixware.datos_clientes.Documento)
    AND credixware.datos_clientes.documento NOT IN 
    (SELECT infobcra.bcra_bajas.Documento
     FROM infobcra.bcra_bajas
     WHERE infobcra.bcra_bajas.Documento = credixware.datos_clientes.Documento)
    and credixware.creditosmorosos.deuda_cred > 1000
    GROUP BY 2,3
    """.format(fecdes, fechas)

    return sql

# Cruzo los datos de la tabla cendeu.deudor - cendeu.padronafip con los datos de los deudores calculados
# en infobcra.bcra_deudores_uni
# para obtener la situacion de cendeu y poder reclasificar
def get_cendeu_bcra():
    sql = """
    select d.tipoIdentificacion ,
    d.numeroIdentificacion ,
    d.situacion ,
    p.CUIT ,
    p.denominacion ,
    bdu.numeroIdentificacion ,
    bdu.denominacion ,
    bdu.situacion 
    from deudor d 
    left join padronafip p on d.tipoIdentificacion = p.CUIT 
    inner join infobcra.bcra_deudores_uni bdu on d.numeroIdentificacion = bdu.numeroIdentificacion 
    """

    return sql



# Busco por cada CUIL obtenido en el proceso si esta en cendeu 
# Estos datos pueden servir para recategorizar como saber si las denominaciones son iguales
def get_cendeu(cuil,  letras):
    
    cuil=str(cuil)
    sql = """
    SELECT cendeu.padronafip.CUIT cuit,
    cendeu.padronafip.denominacion denominacion,
    IFNULL(MAX(cendeu.deudor.situacion), '0') situacion
    FROM cendeu.deudor
    INNER JOIN cendeu.padronafip ON cendeu.deudor.numeroIdentificacion = cendeu.padronafip.CUIT
    WHERE cendeu.deudor.numeroIdentificacion = '{}'
    AND SUBSTRING(cendeu.padronafip.denominacion,1,3) = '{}'
    """.format(cuil, letras)

    try:
        errores.logger.info('Buscando maxima situacion en cendeu.deudor de los Deudores')
        cursor = connbcra.execute(sql)
        for row in cursor:  
            if int(row['cuit']) != None:
                datos=[row['cuit'],row['denominacion'],row['situacion']]
                return datos
            #else:
            #    datos=['0',' ','0']
            
    except Exception as e:
        #print('ERROR EN CONSULTA DE DEUDORES CENDEU: ', e)
        errores.logger.error('ERROR EN CONSULTA DE DEUDORES CENDEU: ')
        
    datos = ['0', ' ', '0']

    return datos  

# calculo cinco años hacia atras de la fecha de proceso para armar la presentacion
def calcular_fecha_proceso(fecha_presentacion):
    fecha_presentacion = datetime.strptime(fecha_presentacion, '%Y-%m-%d')
    fecha_desde_presentacion = fecha_presentacion + relativedelta(years=-5)
    return fecha_desde_presentacion


# Busco los datos en infobcra.padron_persona 
# Recibe como parametro el nro de documento y las tres letras del apellido para buscar en las bases de padron
# esto lo hago asi porque en las bases no esta el sexo y los documentos estan duplicados
def get_cuil_padron(documento, letras):
    nrodoc = int(documento)

    print(documento,'  ', letras)
    sql_info = """
    SELECT infobcra.`padron_personas`.`cuil` cuil
    FROM infobcra.`padron_personas`
    WHERE SUBSTRING(infobcra.`padron_personas`.`denominacion`,1,3) = '{}'
    AND infobcra.`padron_personas`.`nro_documento` = {}
    """.format(letras, nrodoc)

    try:
        cursor = connbcra.execute(sql_info)
        nro_cuil=str(documento)
        for row in cursor:
             nro_cuil = row['cuil']
             if nro_cuil == None:
                 nro_cuil = str(documento)

    except Exception as e:
        #print('ERROR EN CONSULTA DEL PADRON PERSONAS: ', e)
        errores.logger.error('ERROR EN CONSULTA DEL PADRON PERSONAS: ')
    #finally:
    #    connbcra.close()
    return nro_cuil
    

        
# En esta funcion recalculo la situacion a reclacificar, solo la uso en caso que el max_Situaacion del cendeu sea
# igual a la situacion informada, de ser asi, recalculo la situacion anterior, es para los casos de deudores generados en 
# la migracion
# Modifico la funcion cuando la situacion = 21 reclasifico situacion anterior en 1 22-08-2023
def cal_situacion(situacion):
    situacion=int(situacion)
    if situacion == 5:
        situacion = 4
    elif situacion == 4:
        situacion = 3
    elif situacion == 3:
        situacion = 21
    elif situacion == 21:
        situacion = 1


    return str(situacion)

# Proceso principal donde se armar el DF y se recalcula la situación
# Verifica nombre, caracteres especiales y se separan los deudores sin cuil o con nombres mal informados
def exec_proccess(fecdes, fechas):
    
    pd.options.display.max_columns = None

    # Preparo la consulta que arma los deudores unificados
    sql = get_deudores(str(fecdes), str(fechas))

    # Ejecuto el codigo que carga los deudores en infobcra.bcra_deudores_uni
    #connbcra.execute(sql)

    # Leo los datos consolidados de los deudores de la tabla bcra_deudores_uni
    pd.options.display.max_columns = None
    df = pd.read_sql_query(sql, connbcra)

    errores.logger.info('Analizando para recategorizar deudores')
    try:
        # path = configuraciones.RUTA + configuraciones.ARCHIVO_TRABAJO
        # deudoresproc = df
        # deudoresproc.columns = ['Empresa', 'Identificacion', 'Denominacion', 'Situacion', 'Tipo.Asis.Crediticia', 'Saldo Capital', 'Deudor Encuadrado',
        #                'Recat. Obligatoria', 'Dias Atraso', 'Tasa Efec. Mensual', 'Sit.Sin.Reclasificar', 'Pre.Pers.Sin Gtia', 'Cod']


        # Antes de comenzar a iterar modifico el DataFrame en la columna bcradenom, cambio la Ü por U, esto es por una normativa del BCRA
        df['bcradenom']=df['bcradenom'].str.replace('Ü','U')

        # deudoresproc.to_csv(path, index=False, sep=';', encoding='utf-8')
        for index, row in df.iterrows():           
            
            # Llamo a la funcion RE para verificar que el nombre no contenga caracteres raros
            # si es verdadero marco la linea y continuo
            if(verificore.reg_ex(row['bcradenom']))==True:
                df.at[index, 'letras'] = '0'
                continue

            df.at[index,'bcradenom']=row['bcradenom'].strip()

            numrero_cuil = get_cuil_padron(row['bcraiden'], row['letras'])

            # Verifico la cantidad de caracteres del nro de identidad
            # debe ser 11 para ser CUIL, de lo contrario es que no se encontro en el padron
            cantidad=len(str(numrero_cuil))
            if cantidad < 11:
                df.at[index, 'letras'] = '0'
                continue
            
            #df.at[index, 'bcraiden'] = get_cuil_padron(row['bcraiden'], row['letras'])
            df.at[index, 'bcraiden'] = numrero_cuil
            
            if int(row['bcraiden']) !=0:

                #lista_cendeu=get_cendeu(int(row['bcraiden']), row['letras'])
                lista_cendeu = get_cendeu(numrero_cuil, row['letras'])
                # Obtengo de la consulta con el padron
                max_Situacion=int(lista_cendeu[2])
                nombre=str(lista_cendeu[1]).strip()            

                # Verifico si los nombres que estan en las bases de CF son iguales a BCRA
                # si son distintos asumo el que está en el padron de CENDEU
                if nombre !='':
                    if nombre!=str(row['bcradenom']).strip():
                        df.at[index,'bcradenom']=nombre

                # Ajusto la situacion y la recategorizacion    
                if max_Situacion > int(row['situacion']):
                    df.at[index,'recat']=1
                    df.at[index,'situsin']=str(row['situacion']).rjust(2,"0")

                    if max_Situacion == 2:
                        max_Situacion = 21

                    df.at[index,'situacion']=str(max_Situacion).rjust(2,"0")

                # Dependiendo de la igualdad de la posicion en nuestros datos como en cendeu es lo que hace
                # veo la situacion a reclacificar siempre que ambas no sean 1
                elif max_Situacion == int(row['situacion']):
                    # Revisar este bloque if, por los dias de atraso si no tengo reclasificacion
                    if max_Situacion == 1:
                        df.at[index,'recat']=0
                        max_Situacion="0"
                        
                        df.at[index,'situsin']=str(row['situsin']).rjust(2,"0")

                        df.at[index,'situacion']=str(row['situacion']).rjust(2,"0")
                       
                    else:
                        df.at[index,'recat']=1
                        df.at[index,'situsin']=str(cal_situacion(row['situacion'])).rjust(2,"0")
                        df.at[index,'situacion']=str(max_Situacion).rjust(2,"0")

                elif max_Situacion < int(row['situacion']):
                        # Agrego esta condicion el 300323
                        #if max_Situacion == 0:

                        if int(row['situacion']) > 1:
                            row['recat']=0
                            df.at[index,'recat']=1
                            #situacion_sin_recla=cal_situacion(row['situacion'])
                            df.at[index,'situsin']=str(cal_situacion(row['situacion'])).rjust(2,"0")
                            df.at[index,'situacion']=str(row['situacion']).rjust(2,"0")

                            #df.at[index, 'situsin'] = str(row['situacion']).rjust(2, "0")
                            #df.at[index,'situacion']=str(max_Situacion).rjust(2,"0")
                        else:
                            df.at[index,'recat']=0
                            df.at[index,'situsin']=str(max_Situacion).rjust(2,"0")
                            df.at[index,'situacion']=str(row['situacion'].rjust(2,"0"))

                else:
                    df.at[index,'recat']=1
                    df.at[index,'situsin']="0".rjust(2,"0")     
                    df.at[index,'situacion']=str(row['situacion']).rjust(2,"0")
            
    except Exception as e:
        errores.logger.error("Error en la generacion de DataFrame Salida", e)
        
   
    # archivo = configuraciones.RUTA + configuraciones.ARCHIVO_TRABAJO
    # fecha_hora = datetime.now()
    # fecha = time.strftime("%d-%m-%y")
    # hora = time.strftime("%H:%M:%S")
    # archivo = archivo+'-'+fecha+'-'+hora

    path=configuraciones.RUTA + configuraciones.ARCHIVO_SIN_CUIL

    # Genercion de archivo con deudores que no se encontraron los CUIL
    nocuils=df.loc[df['letras']=='0']
    nocuils.columns=['Empresa','Identificacion','Denominacion','Situacion','Tipo.Asis.Crediticia','Saldo Capital','Deudor Encuadrado',
                     'Recat. Obligatoria','Dias Atraso','Tasa Efec. Mensual','Sit.Sin.Reclasificar','Pre.Pers.Sin Gtia','Cod']
    #nocuils.to_csv("f:/Archivos/PruebaWS/deudores_sin_cuil.csv", index=False, sep=';', encoding='utf-8')

    nocuils.to_csv(path, index=False, sep=';', encoding='utf-8')

    # Elimino las filas del DF que contengan un 1 en la columna letras, esto es para sacar del DF final las filas 
    # de los deudores que no se encotraron los nros de CUIL
    df.drop(df[(df['letras']=='0')].index, inplace=True)

    # Elimino la columa letras
    df = df.drop(columns=['letras']) 

   #path = configuraciones.RUTA + configuraciones.ARCHIVO_DEUDORES

    # Con este path creo un archivo deudores_a_procesar.csv con formato UTF-8, para luego convertirlo a ANSI
    path = configuraciones.RUTA + configuraciones.ARCHIVO_TRABAJO

    # Generacion del archivo de deudores para enviar a WorldSys
    #df.to_csv("f:/Archivos/PruebaWS/cendeu_credifacil.csv", header=None, index=False, sep=';', encoding='utf-8')
    df.to_csv(path, header=None, index=False, sep=';', encoding='utf-8', line_terminator="\r\n")






