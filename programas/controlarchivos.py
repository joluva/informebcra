# Este modulo verifica si ya existen los archivos antes de comenzar el proceso 
# y les cambia el nombre agregando fecha actual y hora AAA-MM-DD-HH-MM-SS

from datetime import datetime
from programas import configuraciones
from programas import errores
import time
import os
import os.path as path
import platform

# Cambio el directorio para buscar los archivos anteriores
def cambio_nombre():

    try:
        os.chdir(configuraciones.DIRECTORY)
        # print(os.getcwd())
        fecha = datetime.now()
        for r in os.listdir():
            if r == configuraciones.ARCHIVO_DEUDORES:
                nombre=configuraciones.ARCHIVO_DEUDORES[0:17]
                scr=r
                dest=nombre+'-'+time.strftime('%Y-%m-%d_%H%M%S')+'.csv'
                #print(scr, dest)
                os.rename(scr,dest)
                errores.logger.info("SE CAMBIO CON EXITO EL NOMBRE DE cendeu_credifacil.csv")
            elif r == configuraciones.ARCHIVO_SIN_CUIL:
                nombre=configuraciones.ARCHIVO_SIN_CUIL[0:17]
                scr=r
                dest=nombre+'-'+time.strftime('%Y-%m-%d_%H%M%S')+'.csv'
                #print(scr, dest)
                os.rename(scr,dest)
                errores.logger.info("SE CAMBIO CON EXITO EL NOMBRE DE deudores_sin_cuil.csv")
    except FileExistsError:
        errores.logger.warning("NO SE PUEDE CREAR EL ARCHIVO SOLICITADO, EL MISMO YA EXISTE")
    except FileNotFoundError:
        errores.logger.warning("NO EXISTE EL ARCHIVO O DIRECTORIO AL QUE SE DESE ACCEDER, VERIFIQUE LAS RUTAS EN configuracions.py")
    except NotADirectoryError:
        errores.logger.warning("EL DIRECTORIO NO EXISTE O ESTA MAL ESCRITO")
    except PermissionError:
        errores.logger.warning("LOS ARCHIVOS O DIRECTORIOS A MODIFICAR NO TIENEN PERMISO DE LECTURA O ESCRITURA")