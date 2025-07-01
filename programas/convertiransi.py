# En este modulo convierto el archivo de salida de deudores que esta en formato utf-8
# A un formato ANSI que es el que requiere WorldSys
import os
import os.path as path
from programas import errores
from programas import verificore
from programas import configuraciones

# Esta funcion toma el archivo generado con pandas deudores_a_procesar.csv que esta en formato UTF-8
# y genera el archivo final cendeu_credifacil.csv con formato ANSI para WorldSys
def convertirFormato():
    # Armo el path con el archivo deudores_a_procesar.csv de formato UTF-8 para poder convertirlo a ANSI
    path = configuraciones.RUTA + configuraciones.ARCHIVO_TRABAJO

    try:
        with open(path,'r',encoding='utf-8') as utf8_file:
            # leo el contenido 
            contenido=utf8_file.read()
    except Exception as e:
        errores.logger.info("NO SE ENCONTRO EL ARCHIVO "+path+" ", e)

    # Abro un archivo nuevo en modo escritura como ANSI

    path = configuraciones.RUTA + configuraciones.ARCHIVO_DEUDORES

    try:
        with open(path,'w',encoding='ansi') as ansi_file:
            # escribo el contenido del archivo generado pero ahora en formato ansi
            ansi_file.write(contenido)
    except Exception as e:
        errores.logger.info("NO SE PUDO CONVERTIR EL ARCHIVO A FORMATO ANSI ", e)
