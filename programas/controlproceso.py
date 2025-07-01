# Este modulo verifica la ultima fecha de procveso guardada en el archivo fechaproc.json
# Toma la fecha actual y calcula el mes a procesar restando un mes 
# En fechaproc.json debe estar el ultimo mes procesado "MMAAAA"

import sys
import os
import datetime
import dateutil
import json
from programas import errores
from programas import proceso
from programas import configuraciones
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import timedelta

# Tomo del archivo fechaproc.json la ultima fecha del periodo presentado
# Leo la fecha actual y calculo el mes a procesar 
def proceso_fechas():
    path=configuraciones.PATH
    try:
        if os.path.isfile(path):
            with open(path, 'r') as f:
                dic_fecha = json.load(f)
        else:
            errores.logger.error("FALTA ARCHIVO fechaproc.json")
            sys.exit()

        fecha_ultimo_proceso = dic_fecha['fechaup']
        dia = '01'
        mes_anterior = '0'
        anio_anterior = '0'
        if (len(fecha_ultimo_proceso) < 5) | (len(fecha_ultimo_proceso) > 6):
            print('ERROR EN FECHA ULTIMO PROCESO REVISE ARCHIVO fechaproc.json')
            sys.exit()
        elif (len(fecha_ultimo_proceso) == 5):
            mes_anterior = fecha_ultimo_proceso[0:1].rjust(2, "0")
            anio_anterior = fecha_ultimo_proceso[1:5]
        
        else:
            mes_anterior = fecha_ultimo_proceso[0:2]
            anio_anterior = fecha_ultimo_proceso[2:6]
        
        fecha_proceso_anterior = dia+'/'+mes_anterior+'/'+anio_anterior
        fecha = datetime.strptime(fecha_proceso_anterior, '%d/%m/%Y')

        fecha = datetime.strftime(fecha, '%Y-%m-%d')
    except ValueError:
        errores.logger.error("LA FECHA QUE INTENTA PROCESAR EN EL fechaproc.json ES INVALIDA")
        sys.exit()
    except FileNotFoundError:
        errores.logger.error("FALTA EL ARCHIVO DE CONFIGURACION DE FECHA ULTIMO PROCESO fechaproc.json: ")
        sys.exit()
    else:
        # Tomo la fecha actual y le resto un mes
        fecha_hoy = datetime.today().strftime('%Y-%m-%d')
       
        fecha_hoy = datetime.strptime(fecha_hoy, '%Y-%m-%d')

        fecha_proc = fecha_hoy + relativedelta(months=-1)
        
        if(fecha_proc.month >= int(mes_anterior)) & (fecha_proc.year >= int(anio_anterior)):
            # aca viene la llamada el proceso y convierto la fecha en un string
            
            fecha=fecha_proc.strftime('%Y-%m-%d')

            # LLamo al proceso donde se verifican los datos de las facturas a procesar y llama a proceso princial
            proceso.proc(fecha, dic_fecha)

# Una vez terminado el proceso se actualiza la fecha en el archivo fechaproc.json
# La fecha que recibe debe ser un string
# el dic fecha contiene los datos a modificar en el archivo fechaproc.json
def actualizo_json(fecha, dic_fecha):
    path=configuraciones.PATH
    # Actualizo el diccionario para poder usarlo en la actualizacion del fechaproc.json
    dic_fecha.update({'fechaup':fecha})

    with open(path, 'w') as f:
        json.dump(dic_fecha, f)
