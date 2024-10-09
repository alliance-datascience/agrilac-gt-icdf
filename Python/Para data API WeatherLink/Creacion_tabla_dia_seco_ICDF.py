import datetime as dt
import pandas as pd
import string as str
import numpy as np
import pymysql


import mysql.connector
from mysql.connector import Error


# Función para conectarse a la base de datos y obtener resultados
def obtener_datos():
    try:
        # Establecer la conexión a la base de datos
        conexion = pymysql.connect(
            host='',   # Dirección del servidor
            user='',     # Usuario de la base de datos
            password='',# Contraseña del usuario
            database='icdf'  # Nombre de la base de datos
        )

        # Crear un cursor
        cursor = conexion.cursor()

        # Ejecutar una consulta SQL
        consulta_sql = "SELECT * FROM icdf_historico"
        cursor.execute(consulta_sql)

        # Obtener los resultados de la consulta
        icdf_historicos_mysql = cursor.fetchall()

        # Obtener los nombres de las columnas desde el cursor
        columnas = [desc[0] for desc in cursor.description]

        # Crear un DataFrame a partir de los resultados
        df = pd.DataFrame(icdf_historicos_mysql, columns=columnas)

        return df

    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")

    finally:
        if conexion:
            # Cerrar la conexión
            conexion.close()
            print("Conexión a la base de datos cerrada.")

# Llamar a la función y generar el DataFrame
df_data_all_stations = obtener_datos()

# Mostrar el DataFrame resultante
print(df_data_all_stations)

df_lluvia = df_data_all_stations

# Convertir 'ts' a formato datetime manejando diferentes formatos de fecha
df_lluvia['ts'] = pd.to_datetime(df_lluvia['ts'], errors='coerce', dayfirst=True)

# Filtrar el DataFrame para conservar solo las columnas necesarias
df_lluvia2 = df_lluvia[['ts', 'station_id', 'rainfall_mm']].copy()

# Asegurarse de que 'rainfall_mm' es de tipo float para poder sumar
df_lluvia2['rainfall_mm'] = pd.to_numeric(df_lluvia2['rainfall_mm'], errors='coerce')

# Crear una columna con la fecha
df_lluvia2['Dia'] = df_lluvia2['ts'].dt.to_period('D')

# Agrupar por 'station_id' y 'Dia' y sumar la lluvia diaria
df_lluvia_diaria = df_lluvia2.groupby(['station_id', 'Dia'])['rainfall_mm'].sum().reset_index()

# Convertir 'Dia' a formato datetime para la columna 'ts'
df_lluvia_diaria['ts'] = df_lluvia_diaria['Dia'].dt.to_timestamp()

#no. de dias secos
#Día seco = 1 (sí)
#Día lluvioso = 0(no)

dia_seco = df_lluvia_diaria

# Crear la columna 'dia_seco' con la condición especificada
dia_seco['dia_seco'] = dia_seco['rainfall_mm'].apply(lambda x: 1 if x <= 1 else 0)

import pandas as pd
from sqlalchemy import create_engine

# Guardar el dataframe filtrado en un archivo CSV
def guardar_csv(dia_seco, ruta_csv):
    try:
        dia_seco.to_csv(ruta_csv, index=False)  # Guardar el dataframe como CSV sin el índice
        print(f'CSV guardado exitosamente en {ruta_csv}.')
    except Exception as e:
        print(f'Error al guardar el CSV: {e}')

# Cargar el CSV a la base de datos MySQL
def cargar_csv_a_mysql(ruta_csv, db_url, tabla_destino):
    try:
        # Leer el archivo CSV antes de cargarlo a la base de datos
        df_csv_diaseco = pd.read_csv(ruta_csv)
        
        # Crear el motor de conexión a MySQL
        engine = create_engine(db_url)

        # Cargar el dataframe a la tabla en MySQL
        df_csv_diaseco.to_sql(name=tabla_destino, con=engine, if_exists='append', index=False)
        print(f'Datos del CSV cargados exitosamente en la tabla {tabla_destino}.')
    except Exception as e:
        print(f'Error al cargar los datos del CSV: {e}')

# Ruta de la base de datos MySQL en formato: mysql+pymysql://usuario:contraseña@host:puerto/nombre_base_datos
db_url = 'mysql+pymysql://usuario:contraseña@host:puerto/icdf'


# Ruta donde se guardará el CSV
ruta_csv = 'ruta/ICDF/tabla_dia_seco_icdf.csv'

# Guardar el resultado filtrado en un archivo CSV
guardar_csv(dia_seco, ruta_csv)

# Cargar los datos del archivo CSV a la base de datos MySQL
cargar_csv_a_mysql(ruta_csv, db_url, 'icdf_dia_seco')
