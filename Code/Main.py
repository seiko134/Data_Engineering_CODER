#importe de Librerias
import pandas as pd
import sqlalchemy
import os
import sqlalchemy as sa
import requests
from datetime import datetime,timedelta

#Extraccion de Hora tanto de inicio como actual(depende del endpoint que se utilice)
hora_actual = datetime.now()
fecha_actual= datetime(hora_actual.year,hora_actual.month,hora_actual.day,hora_actual.hour,hora_actual.minute,hora_actual.second)

fecha_inicial = fecha_actual - timedelta(days=30)
fecha_inicial = fecha_inicial.date()
fecha_actual = fecha_actual.date()

#Endpoint con fecha de inicio y final, para navegar entre las fechas que ofrece el endpoint
url_base = f'https://api.nasa.gov/planetary/apod?api_key=7oNCkWUAqsODsa7LWMdaDiZwQ07z7lyYh3xTLrHo&start_date={fecha_inicial}&end_date={fecha_actual}'

#endpoint que obtiene info solo del dia
#url_base = f'https://api.nasa.gov/planetary/apod?api_key=7oNCkWUAqsODsa7LWMdaDiZwQ07z7lyYh3xTLrHo&start_date={fecha_actual}'
response = requests.get(url_base)

#Normalizacion de json
datos = response.json()
data = pd.json_normalize(datos)

# Modificacion de nombres de Columnas
data = data.rename(columns={
    "copyright": "Autor",
    "date": "fecha",
    "explanation": "explicacion",
    "hdurl": "Url_Contenido",
    "media_type": "Tipo_Contenido",
    "service_version": "Version_Servicio",
    "title": "Titulo_Contenido",
    "url": "url",
    })
#Hora de carga en chile
horaCarga= datetime(hora_actual.year,hora_actual.month,hora_actual.day,hora_actual.hour,hora_actual.minute,hora_actual.second) - timedelta(hours=4)
fechaCarga_str = horaCarga.strftime('%Y-%m-%d %H:%M')

# Añadir la fecha como tipo object al DataFrame
data['Fecha_Carga'] = fechaCarga_str
print(url_base)
print(data)
#print(data.info())

#Conexion y carga a la Base de datos
connection_url = f"postgresql+psycopg2://sebastian_medel01_coderhouse:p8658bXK6I@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database"
db_engine = sa.create_engine(connection_url)
print(data)
print(data.info())
data.to_sql("Api_NASA",db_engine,if_exists = 'append',schema = 'sebastian_medel01_coderhouse',  index = False, method="multi")


#if __name__ == "_main_":
 #   main()