from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
#from config.development import SQLALCHEMY_DATABASE_BCRA

from development import *




conexion = create_engine(SQLALCHEMY_DATABASE_BCRA)

print(SQLALCHEMY_DATABASE_BCRA)

sql = 'SELECT Documento, Nombre FROM bcra_cliente_p WHERE Documento < 2000000'

df = pd.read_sql_query(sql, conexion)

print(df)