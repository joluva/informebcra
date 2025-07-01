import mysql.connector
from mysql.connector import Error
import os.path
from  config.setting_production import *


# diccionario de configuracion, con los datos de conexion
cfg = {}

cfg = config.setting.production.configuracion

def connection_infobcra():
    connectionbcra = None

    try:
        #connectionbcra = mysql.connector.connect (
        #    host=cfg.get("HOST_INFOBCRA"),
        #    user=cfg.get("USER_INFOBCRA"),
        #    passwd=cfg.get("PASSWORD_INFOBCRA"),
        #    database=cfg.get("DB_INFOBCRA")

        #)
                
        SQLALCHEMY_DATABASE_INFOBCRA = f'mysql+pymysql://{cfg.get("USER_INFOBCRA")}:{cfg.get("PASSWORD_INFOBCRA")}@{cfg.get("HOST_INFOBCRA")}/{cfg.get("DB_INFOBCRA")}'
        
        print("La conexion a la Base INFOBCRA fue OK")
    except Error as e:
        print(f"A ocurrido un error en la conexion a INFOBCRA: '(e)'")
    return

def connection_credixware():
    connectioncware = None

    try:
        connectioncware = mysql.connector.connect (
            host=cfg.get("HOST_INFOBCRA"),
            user=cfg.get("USER_INFOBCRA"),
            passwd=cfg.get("PASSWORD_INFOBCRA"),
            database=cfg.get("DB_CREDIXWARE")

        )

        SQLALCHEMY_DATABASE_INFOBCRA = f'mysql+pymysql://{cfg.get("USER_INFOBCRA")}:{cfg.get("PASSWORD_INFOBCRA")}@{cfg.get("HOST_INFOBCRA")}/{cfg.get("DB_CREDIXWARE")}'

        print("La conexion a la Base CREDIXWARE fue OK")
    except Error as e:
        print(f"A ocurrido un error en la conexion a CREDIXWARE: '(e)'")

def connection_cendeu():
    connectioncen = None

    try:
        connectioncen = mysql.connector.connect (
            host=cfg.get("HOST_INFOBCRA"),
            user=cfg.get("USER_INFOBCRA"),
            passwd=cfg.get("PASSWORD_INFOBCRA"),
            database=cfg.get("DB_CENDEU")

        )

        SQLALCHEMY_DATABASE_INFOBCRA = f'mysql+pymysql://{cfg.get("USER_INFOBCRA")}:{cfg.get("PASSWORD_INFOBCRA")}@{cfg.get("HOST_INFOBCRA")}/{cfg.get("DB_CENDEU")}'

        print("La conexion a la Base CENDEU fue OK")
    except Error as e:
        print(f"A ocurrido un error en la conexion a CENDEU: '(e)'")