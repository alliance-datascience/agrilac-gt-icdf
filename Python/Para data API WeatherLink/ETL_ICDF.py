import json
import numpy as np

import requests
import time
from datetime import datetime

# API v2 Base URL
BASE_URL = "https://api.weatherlink.com/v2/"

# Your WeatherLink Account API Credentials
API_KEY = ""
API_SECRET = ""

# API v2 Endpoint URL
# https://weatherlink.github.io/v2-api/api-reference
endpoint = "historic"

# API Path Parameters
# Add the necessary _path_ parameters necessary for the API endpoint that you are querying
pathParameters = [
  169484,
  169523,
  169524,
  169529,
  178223
]

# API Query String Parameters
# Add the necessary _query string_ parameters necessary for the API endpoint that you are querying
queryParameters = [
  ("start-timestamp", str(int(time.time()) - 3600)),
  ("end-timestamp", str(int(time.time())))
]

# Loop through each path parameter and make API requests
for pathParam in pathParameters:
    # Create final API URL for each path parameter
    api_url = BASE_URL + endpoint 
    api_url += f"/{pathParam}" 
    api_url += "?api-key=" + API_KEY 
    api_url += ''.join("&" + str(x) + "=" + str(y) for (x, y) in queryParameters)[0:]

    # Make call to API and pretty-print returned data
    api_results = requests.get(
      headers={
        "X-Api-Secret": API_SECRET
      },
      url=api_url,
      verify=True,
    )

    # Parse the API response
    api_data = json.loads(api_results.text)

    # Define the output path for the JSON file
    output_path = f"ruta/ICDF/DATOS_DIARIOS/{pathParam}.json"

    # Save the JSON data to the specified path
    with open(output_path, 'w') as json_file:
        json.dump(api_data, json_file, indent=4)

    # Print a success message with the current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Script executed successfully for station {pathParam} at {current_time}")

import json
import pandas as pd
from datetime import datetime

# Lista de archivos JSON que ya existen
pathParameters = [
    169484,
    169523,
    169524,
    169529,
    #178223
]

# Ruta base para los archivos JSON y CSV
base_path = "ruta/ICDF/DATOS_DIARIOS/"

# Diccionario para almacenar los DataFrames
dfs = {}

# Procesar cada archivo JSON
for pathParam in pathParameters:
    json_file_path = f"{base_path}{pathParam}.json"

    # Cargar el archivo JSON
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Inicializar una lista para almacenar los datos
    records = []

    # Iterar a través de cada sensor y sus datos
    for sensor in data['sensors']:
        sensor_id = sensor['lsid']
        sensor_type = sensor['sensor_type']
        data_structure_type = sensor['data_structure_type']
        
        for entry in sensor['data']:
            entry_record = {
                'station_id': data['station_id'],
                'sensor_id': sensor_id,
                'sensor_type': sensor_type,
                'data_structure_type': data_structure_type,
                'generated_at': data['generated_at']
            }
            entry_record.update(entry)
            records.append(entry_record)

    # Convertir la lista de registros a un DataFrame de Pandas
    df = pd.DataFrame(records)

    # Convertir las columnas de marca de tiempo a un formato legible
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df['generated_at'] = pd.to_datetime(df['generated_at'], unit='s')

    # Guardar el DataFrame en un archivo CSV
    csv_file_path = f"{base_path}{pathParam}.csv"
    df.to_csv(csv_file_path, index=False, sep=';')

    # Asignar el DataFrame al diccionario usando el código como clave
    dfs[f"df_{pathParam}"] = df

    # Print a success message with the current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"CSV file created for station {pathParam} at {current_time}")

import datetime

# Datos de ejemplo
data = {
    "last_gps_reading_timestamp": 1717170300,
    "tz_offset": -21600
}

# Convertir el timestamp a una fecha y hora legible
last_gps_reading_timestamp = data["last_gps_reading_timestamp"]
gps_date_time = datetime.datetime.utcfromtimestamp(last_gps_reading_timestamp)

# Calcular la zona horaria a partir del desplazamiento
tz_offset_hours = data["tz_offset"] / 3600  # Convertir segundos a horas

print(f"La última lectura de GPS fue: {gps_date_time} UTC")
print(f"Desplazamiento de la zona horaria (tz_offset): {tz_offset_hours} horas (UTC{tz_offset_hours:+})")





# El timestamp en cuestión
timestamp = 1717549200

# Convertir el timestamp a una fecha legible
date_time = datetime.datetime.utcfromtimestamp(timestamp)

print(date_time)


#Creación de nuevos dataframes a partir de los existentes por estación
df_169484 = dfs['df_169484']
df_169523 = dfs['df_169523']
df_169524 = dfs['df_169524']
df_169529 = dfs['df_169529']
#df_178223 = dfs['df_178223']

# Lista de columnas a seleccionar
columns_to_select = [
    'station_id','ts', 'sensor_type', 'wind_speed_avg', 'uv_dose', 'wind_speed_hi', 'wind_dir_of_hi',
    'wind_chill', 'solar_rad_hi', 'deg_days_heat', 'thw_index', 'bar', 'hum_out',
    'tz_offset', 'uv_index_hi', 'temp_out', 'temp_out_lo', 'wet_bulb', 'temp_out_hi',
    'solar_rad_avg', 'bar_alt', 'arch_int', 'wind_run', 'solar_energy', 'dew_point_out',
    'rain_rate_hi_clicks', 'wind_dir_of_prevail', 'et', 'air_density', 'rainfall_in',
    'heat_index_out', 'thsw_index', 'rainfall_mm', 'night_cloud_cover', 'deg_days_cool',
    'rain_rate_hi_in', 'uv_index_avg', 'wind_num_samples', 'emc', 'rain_rate_hi_mm',
    'rev_type', 'rainfall_clicks', 'abs_press','moist_soil_last','bar_trend_3_hr','pressure_last'
]

columns_to_select_jeronimo = [
    'station_id','ts', 'sensor_type', 'wind_speed_avg', 'uv_dose', 'wind_speed_hi', 'wind_dir_of_hi',
    'wind_chill', 'solar_rad_hi', 'deg_days_heat', 'thw_index', 'bar', 'hum_out',
    'tz_offset', 'uv_index_hi', 'temp_out', 'temp_out_lo', 'wet_bulb', 'temp_out_hi',
    'solar_rad_avg', 'bar_alt', 'arch_int', 'wind_run', 'solar_energy', 'dew_point_out',
    'rain_rate_hi_clicks', 'wind_dir_of_prevail', 'et', 'air_density', 'rainfall_in',
    'heat_index_out', 'thsw_index', 'rainfall_mm', 'night_cloud_cover', 'deg_days_cool',
    'rain_rate_hi_in', 'uv_index_avg', 'wind_num_samples', 'emc', 'rain_rate_hi_mm',
    'rev_type', 'rainfall_clicks', 'abs_press','bar_trend_3_hr','pressure_last'
]

columns_to_select_GRANADOS = [
    'station_id','ts', 'sensor_type','wind_speed_avg',
    'uv_dose','wind_speed_hi','wind_dir_of_hi','wind_chill',
    'solar_rad_hi','deg_days_heat','thw_index','bar',
    'hum_out','uv_index_hi','temp_out','temp_out_lo',
    'wet_bulb','temp_out_hi','solar_rad_avg','bar_alt',
    'arch_int','wind_run','solar_energy','dew_point_out',
    'rain_rate_hi_clicks','wind_dir_of_prevail','et',
    'air_density','rainfall_in','heat_index_out',
    'thsw_index','rainfall_mm','night_cloud_cover',
    'deg_days_cool','rain_rate_hi_in','uv_index_avg',
    'wind_num_samples','emc','rain_rate_hi_mm',
    'rev_type','rainfall_clicks','abs_press', 'bar_trend_3_hr','pressure_last'
]

# Crear el nuevo dataframe con solo las columnas seleccionadas
df_JERONIMO = df_169484[columns_to_select_jeronimo]
df_SALAMA = df_169523[columns_to_select]
df_CUNBAV = df_169524[columns_to_select]
df_CUBULCO = df_169529[columns_to_select]
#df_GRANADOS = df_178223[columns_to_select_GRANADOS]

# Filtrar el dataframe para dejar solo las filas donde sensor_type es 30 o 108
df_JERONIMO_filtrado = df_JERONIMO[(df_JERONIMO['sensor_type'] == 30) | (df_JERONIMO['sensor_type'] == 108)]
# Restar 6 horas a la columna ts
df_JERONIMO_filtrado['ts'] = pd.to_datetime(df_JERONIMO_filtrado['ts']) - pd.Timedelta(hours=6)
df_JERONIMO_filtrado.info()
df_JERONIMO_filtrado.head()

# Filtrar el dataframe para dejar solo las filas donde sensor_type es 30 o 108
df_SALAMA_filtrado = df_SALAMA[(df_SALAMA['sensor_type'] == 30) | (df_SALAMA['sensor_type'] == 108)]
# Restar 6 horas a la columna ts
df_SALAMA_filtrado['ts'] = pd.to_datetime(df_SALAMA_filtrado['ts']) - pd.Timedelta(hours=6)
df_SALAMA_filtrado.info()
df_SALAMA_filtrado.head()


# Filtrar el dataframe para dejar solo las filas donde sensor_type es 30 o 108
df_CUNBAV_filtrado = df_CUNBAV[(df_CUNBAV['sensor_type'] == 30) | (df_CUNBAV['sensor_type'] == 108)]
# Restar 6 horas a la columna ts
df_CUNBAV_filtrado['ts'] = pd.to_datetime(df_CUNBAV_filtrado['ts']) - pd.Timedelta(hours=6)
df_CUNBAV_filtrado.info()
df_CUNBAV_filtrado.head()


# Filtrar el dataframe para dejar solo las filas donde sensor_type es 30 o 108
df_CUBULCO_filtrado = df_CUBULCO[(df_CUBULCO['sensor_type'] == 30) | (df_CUBULCO['sensor_type'] == 108)]
# Restar 6 horas a la columna ts
df_CUBULCO_filtrado['ts'] = pd.to_datetime(df_CUBULCO_filtrado['ts']) - pd.Timedelta(hours=6)
df_CUBULCO_filtrado.info()
df_CUBULCO_filtrado.head()

# Filtrar el dataframe para dejar solo las filas donde sensor_type es 30 o 108
#df_GRANADOS_filtrado = df_GRANADOS[(df_GRANADOS['sensor_type'] == 30) | (df_GRANADOS['sensor_type'] == 3)]
# Restar 6 horas a la columna ts
#df_GRANADOS_filtrado['ts'] = pd.to_datetime(df_GRANADOS_filtrado['ts']) - pd.Timedelta(hours=6)
#df_GRANADOS_filtrado.info()
#df_GRANADOS_filtrado.head()

#Despivoteo de dataframes y eliminación de columna sensor_type
# Eliminar la columna 'sensor_type'
#df_JERONIMO_filtrado.drop(columns=['sensor_type','tz_offset'], inplace=True)
#df_SALAMA_filtrado.drop(columns=['sensor_type','tz_offset'], inplace=True)
#df_CUNBAV_filtrado.drop(columns=['sensor_type','tz_offset'], inplace=True)
#df_CUBULCO_filtrado.drop(columns=['sensor_type','tz_offset'], inplace=True)
#df_GRANADOS_filtrado.drop(columns=['sensor_type'], inplace=True)

# Despivoteando las columnas seleccionadas, manteniendo 'station_id' y 'ts' sin despivotear
df_JERONIMO_unpivot = pd.melt(df_JERONIMO_filtrado, id_vars=['station_id', 'ts'], var_name='variable', value_name='value')
df_SALAMA_unpivot = pd.melt(df_SALAMA_filtrado, id_vars=['station_id', 'ts'], var_name='variable', value_name='value')
df_CUNBAV_unpivot = pd.melt(df_CUNBAV_filtrado, id_vars=['station_id', 'ts'], var_name='variable', value_name='value')
df_CUBULCO_unpivot = pd.melt(df_CUBULCO_filtrado, id_vars=['station_id', 'ts'], var_name='variable', value_name='value')
#df_GRANADOS_unpivot = pd.melt(df_GRANADOS_filtrado, id_vars=['station_id', 'ts'], var_name='variable', value_name='value')


# Lista de dataframes
dfs_unpivot = [df_JERONIMO_unpivot, df_SALAMA_unpivot, df_CUNBAV_unpivot, df_CUBULCO_unpivot] #, df_GRANADOS_unpivot]

# Unir todos los dataframes
df_todas_estaciones= pd.concat(dfs_unpivot, ignore_index=True)


# Eliminar los NaN de la columna 'value'
df_todas_estaciones_sin_nan = df_todas_estaciones.dropna(subset=['value'])


# Definir función para transformar los valores
def transformar_valor(row):
    if row['variable'] == 'bar':
        return row['value'] * 33.8639  # convertir inHg a mbar
    elif row['variable'] == 'temp_out':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'temp_out_hi':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'temp_out_lo':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'dew_point_out':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'wet_bulb':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'heat_index_out':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'thw_index':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'thsw_index':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'wind_chill':
        return (row['value'] - 32) * 5.0/9.0  #convertir de Fahrenheit a Celsius
    elif row['variable'] == 'wind_speed_avg':
        return row['value'] * 1.60934  # convertir millas/h a Km/h
    elif row['variable'] == 'wind_speed_hi':
        return row['value'] * 1.60934  # convertir millas/h a Km/h
    elif row['variable'] == 'wind_run':
        return row['value'] * 1.60934  # convertir millas/h a Km/h
    elif row['variable'] == 'et':
        return row['value'] * 25.4  # convertir in/día a mm/día
    else:
        return row['value']

# Aplicar la función al DataFrame y crear la nueva columna 'valor'
df_todas_estaciones_sin_nan['valor'] = df_todas_estaciones_sin_nan.apply(transformar_valor, axis=1)



df_limites = pd.read_excel('ruta/ICDF/LIMITES_GENERALES.xlsx')

# Eliminar la columna 'value'
df_todas_estaciones_sin_nan.drop(columns=['value'], inplace=True)

# Renombrar las columnas
df_todas_estaciones_sin_nan.rename(columns={
    'station_id': 'ID',
    'ts': 'FECHA',
    'variable': 'VARIABLE',
    'valor': 'VALOR'
}, inplace=True)


df_valores_para_comparar = df_todas_estaciones_sin_nan
# Realizar el merge
df_merged = pd.merge(df_valores_para_comparar, df_limites, on='VARIABLE', how='left')


df_merged2 = df_merged
# Realiza la validación de rango
df_merged2['VALOR_final'] = np.where(
    (df_merged2['VALOR'] <= df_merged2['MAX']) & 
    (df_merged2['VALOR'] >= df_merged2['MIN']),
    df_merged2['VALOR'],
    np.nan
)

df_datos_validados_rangos = df_merged2
# Redondear la columna "VALOR" a 2 decimales
df_datos_validados_rangos['VALOR_final'] = df_datos_validados_rangos['VALOR_final'].round(2)
df_datos_validados_rangos.to_csv('ruta/ICDF/DATOS_DIARIOS/df_datos_validados_rangos_1oct2024.csv', index=False, sep=';')


df_datos_validados_rangos_v2 =df_datos_validados_rangos

# Eliminar las columnas "VALOR", "MAX" y "MIN"
df_datos_validados_rangos_v2.drop(columns=['VALOR', 'MAX', 'MIN'], inplace=True)

# Pivoteo del DataFrame
df_pivot = df_datos_validados_rangos_v2.pivot_table(index=['ID', 'FECHA'], columns='VARIABLE', values='VALOR_final').reset_index()

df_pivot.to_csv('ruta/ICDF/df_pivot_1oct2024.csv', index=False, sep=';')

df_revision_temps = df_pivot

# Comprobación de temp_out_lo no mayor que temp_out_hi
comprobacion = df_revision_temps['temp_out_lo'] > df_revision_temps['temp_out_hi']
df_revision_temps.loc[comprobacion, 'temp_out_lo'] = None

df_revision_temps.to_csv('ruta/ICDF/df_revision_temps1oct2024.csv', index=False, sep=';')

df_correccion_radiacion = df_revision_temps

# Función para verificar el rango de horas
def es_hora_noche(hora):
    return (hora >= 19) or (hora < 4)

# Aplicar la función y obtener una máscara
correcion = df_correccion_radiacion['FECHA'].dt.hour.apply(es_hora_noche)

# Volver nulos los valores en las columnas específicas donde la máscara es verdadera
df_correccion_radiacion.loc[correcion, ['solar_energy', 'solar_rad_avg', 'solar_rad_hi']] = 0

from sqlalchemy import create_engine

# Copiar y filtrar el dataframe según las columnas especificadas
def filtrar_dataframe(df):
    columnas_filtradas = [
        'FECHA', 'ID', 'bar', 'wet_bulb', 'deg_days_heat', 'deg_days_cool',
        'et', 'solar_energy', 'hum_out', 'rainfall_mm', 'dew_point_out',
        'solar_rad_avg', 'solar_rad_hi', 'thsw_index', 'thw_index',
        'rain_rate_hi_mm', 'temp_out', 'temp_out_lo', 'temp_out_hi',
        'wind_speed_avg', 'wind_run', 'wind_chill', 'heat_index_out'
    ]
    df_filtrado = df_correccion_radiacion[columnas_filtradas].copy()  # Crear una copia del dataframe filtrado
    
    # Renombrar columnas: ts -> FECHA, station_id -> ID
    df_filtrado.rename(columns={'FECHA':'ts','ID':'station_id'}, inplace=True)
    
    return df_filtrado

# Guardar el dataframe filtrado en un archivo CSV
def guardar_csv(df_filtrado, ruta_csv):
    try:
        df_filtrado.to_csv(ruta_csv, index=False)  # Guardar el dataframe como CSV sin el índice
        print(f'CSV guardado exitosamente en {ruta_csv}.')
    except Exception as e:
        print(f'Error al guardar el CSV: {e}')

# Cargar el CSV a la base de datos MySQL
def cargar_csv_a_mysql(ruta_csv, db_url, tabla_destino):
    try:
        # Leer el archivo CSV antes de cargarlo a la base de datos
        df_csv = pd.read_csv(ruta_csv)
        
        # Crear el motor de conexión a MySQL
        engine = create_engine(db_url)

        # Cargar el dataframe a la tabla en MySQL
        df_csv.to_sql(name=tabla_destino, con=engine, if_exists='append', index=False)
        print(f'Datos del CSV cargados exitosamente en la tabla {tabla_destino}.')
    except Exception as e:
        print(f'Error al cargar los datos del CSV: {e}')

# Ruta de la base de datos MySQL en formato: mysql+pymysql://usuario:contraseña@host:puerto/nombre_base_datos
db_url = 'mysql+pymysql://usuario:contraseña@host:3306/icdf'

# Filtrar el dataframe
df_filtrado = filtrar_dataframe(df_correccion_radiacion)

# Ruta donde se guardará el CSV
ruta_csv = 'ruta/septiembre/ICDF/datos_diarios_icdf.csv'

# Guardar el resultado filtrado en un archivo CSV
guardar_csv(df_filtrado, ruta_csv)

# Cargar los datos del archivo CSV a la base de datos MySQL
cargar_csv_a_mysql(ruta_csv, db_url, 'icdf_historico')
