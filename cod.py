import requests
import pandas as pd
import sqlite3
from typing import Set
import os


def ej_1_cargar_datos_demograficos() -> pd.DataFrame:
    url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    data = pd.read_csv(url, sep=';')

    # Elimina las columnas: Race, Count y Number of Veterans.
    data = data.drop(['Race', 'Count', 'Number of Veterans'], axis=1)
    # Elimina las filas duplicadas.
    data = data.drop_duplicates()    
    # Restablecer los indices depues de eliminar filas
    data = data.reset_index(drop=True)

    return data

df = ej_1_cargar_datos_demograficos()



def ej_2_cargar_calidad_aire(ciudades: Set[str]) -> None:
    # Crear un diccionario para almacenar los datos
    data = {'city': [], 'CO': [], 'NO2': [], 'O3': [], 'SO2': [], 'PM2.5': [], 'PM10': [], 'overall_aqi': []}
    
    # Iterar sobre las ciudades
    for city in ciudades:
        api_url = 'https://api.api-ninjas.com/v1/airquality?city={}'.format(city)
        
        try:
            response = requests.get(api_url, headers={'X-Api-Key': 'J2aRTfhArBRr7EnSIt8P8Q==M8cI6YvXHB4GvK2o'})
            response.raise_for_status()  # Verificar si la solicitud fue exitosa
            json_data = response.json()
            
            print(f"Datos para la ciudad {city}: {json_data}")  # Agregado para depuración
            
            # Almacenar los datos en el diccionario
            data['city'].append(city)
            data['CO'].append(json_data['CO']['concentration'])  # Ajustado según la nueva estructura de datos
            data['NO2'].append(json_data['NO2']['concentration'])
            data['O3'].append(json_data['O3']['concentration'])
            data['SO2'].append(json_data['SO2']['concentration'])
            data['PM2.5'].append(json_data['PM2.5']['concentration'])
            data['PM10'].append(json_data['PM10']['concentration'])
            data['overall_aqi'].append(json_data['overall_aqi'])
        
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos para la ciudad {city}: {e}")
    
    # Crear un DataFrame con los datos
    calidad_air_df = pd.DataFrame(data)
    
    # Guardar el DataFrame en un archivo CSV
    calidad_air_df.to_csv("ciudades.csv", index=False)

# Test
df = ej_1_cargar_datos_demograficos()
ej_2_cargar_calidad_aire(set(df["City"].tolist()))
ciudades_df = pd.read_csv("ciudades.csv")
print(ciudades_df.head(10).to_dict())



# Cargar datos demográficos y calidad del aire
df_demograficos = ej_1_cargar_datos_demograficos()
ej_2_cargar_calidad_aire(set(df_demograficos["City"].tolist()))

conn = sqlite3.connect('dat_demog_air')

df_demograficos.to_sql('demograficos', conn, index=False, if_exists='replace')
ciudades_df.to_sql('Calidad_aire', conn, index=False, if_exists='replace')

query = '''
SELECT *
FROM demograficos
JOIN Calidad_aire ON demograficos.City = Calidad_aire.city
ORDER BY demograficos."Total Population" DESC
LIMIT 10;
'''

rest = pd.read_sql_query(query, conn)
print(rest)