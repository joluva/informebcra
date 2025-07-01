# Desde aca se ejecuta el Script, completo
# Primero verifica la fecha ingresada
# Consulta que la facturacion este lista y carga la tabla correspondiente
# Ejecuta el proceso de deudores y genera el archivo a presentar
#import pdb; pdb.set_trace()
import sys
import datetime
import dateutil
import os
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import timedelta
from programas import consulta
from programas import factura
from programas import errores
from programas import configuraciones
from programas import controlproceso
from programas import convertiransi


#consulta.exec_proccess()
# try:
#     fecha_procesar = sys.argv[1]
    
# except Exception as e:
#     #print('NO SE INGRESO LA FECHA DE PROCESO - INTENTE NUEVAMENTE', e)
#     errores.logger.error('NO SE INGRESO LA FECHA DE PROCESO - INTENTE NUEVAMENTE', e)
#     sys.exit()

# # Verifico que la fecha sea valida
# # Armo la fecha desde el periodo ingresado
# while True:
#     dia='01'
#     mes=fecha_procesar[0:2]
#     anio=fecha_procesar[2:6]
#     fecha_procesar=dia+'/'+mes+'/'+anio
#     #print(fecha_procesar)

#     try:
#         fecha = datetime.strptime(fecha_procesar, '%d/%m/%Y')
#         break
#     except  Exception as e:
#         #print('LA FECHA INGRESADA ES INCORRECTA, VERIFIQUE ', e)
#         errores.logger.error('LA FECHA INGRESADA ES INCORRECTA, VERIFIQUE ', e)
#         sys.exit()

# fecha = datetime.strftime(fecha,'%Y-%m-%d')

# # Hago el calculo de dinco años hacia atras de la fecha de proceso
# try:
#     fecha = datetime.strptime(fecha, '%Y-%m-%d')
#     fecha_desde_presentacion = fecha + relativedelta(years=-5, day=+31)        
    
#     #fecha = '2018-12-31'
#     #fecha_desde_presentacion='2018-01-01'
#     print(fecha," ",fecha_desde_presentacion)
# except Exception as e:
#     #print(e)
#     errores.logger.error('Error en el calculo de fechas', e)
#     sys.exit()

# print(datetime.strftime(fecha, '%Y-%m-%d'))
# print(datetime.strftime(fecha_desde_presentacion, '%Y-%m-%d'))

# Separo el mes y año para la funcion get_factura que revisa si ya está la facturacion mensual
# Recibe un string con la fecha
def proc(fecha_procesar, dic_fecha):
    fecha=datetime.strptime(fecha_procesar, '%Y-%m-%d')
    fecha_desde_presentacion = fecha + relativedelta(years=-configuraciones.ANIOS_PROCESO_ATRAS, day=+31)
    fyear=fecha.year
    fmonth=fecha.month

    # Verifico si ya esta la facturacion lista dentro de la vista de cendeu
    cantidad = factura.get_factura(fyear, fmonth)

    if cantidad == 0:
        print('NO HAY FACTURAS A PROCESAR PARA EL PERIODO SOLICITADO')
        errores.logger.info("NO HAY FACTURAS A PROCESAR PARA EL PERIODO SOLICITADO")
        sys.exit()

    # Verifico si la facturacion del mes a presentar esta dentro de infobcra.bcra_factura
    cantidad = factura.get_factura_bcra(fyear, fmonth)
    if cantidad == 0:
        factura.put_factura(fyear, fmonth)
    else:
        print('LA FACTURACION YA ESTA CARGADA, PARA EL PERIODO SOLICITADO')

    
    consulta.exec_proccess(fecha_desde_presentacion, fecha)

    # Llamo a la funcion para convertir el archivo UTF-8 a formato ANSI

    convertiransi.convertirFormato()

    path=configuraciones.PATH
    # Verifico nuevamente que existe el archivo json para modificar la fecha y si se ve el cendeu_credifacil.csv
    # Si falta alguno de los archivos no se actualiza el fechaproc.json
    if os.path.isfile(configuraciones.PATH):
        if os.path.isfile(configuraciones.PATH_CENDEU):
            dia=str(fecha.day)
            mes=str(fecha.month).rjust(2,"0")
            anio=str(fecha.year)
            fecha_json=mes+anio
            controlproceso.actualizo_json(fecha_json, dic_fecha)
        else:
            errores.logger.info("NO SE GENERO EL ARCHIVO cendeu_credifacil.csv")
    else:
        errores.logger.info("FALTA EL ARCHIVO fechaproc.json")
            



