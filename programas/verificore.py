# Modulo de REGULAR EXPRESSION
# Verifico que los nombres no contengan caracteres raros
# En caso de contener algun caracter raro marco la linea para sacarla

import pandas as pd
import numpy as np
import re


# Compilo los patrones para que sea mas eficiente la busqueda
def compilar_reg():
    pass

#Valido que el dato o nombre no contenga caracteres raros
def reg_ex(nombre):
    reg_obj = re.compile("[-*/|@'´ïÇÄ!%&=éâäàåçêëèïîìÅÉæÆôöòûùÿÖø£Øƒ0-9.:_{}~]")
    if reg_obj.search(nombre):
        return True
    else:
        return False
    
#Busco la ü si existe envio true para modificar la misma
def validoU(nombre):
    if re.search('Ü', nombre):
        return True
    else:
        return False

#Busco si en el nombre exite una comilla simple y la cambio por espacio
def validoComilla(nombre):
    if re.search("'", nombre):
        return True
    else:
        return False