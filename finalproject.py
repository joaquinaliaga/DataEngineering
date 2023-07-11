from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import smtplib
from datetime import datetime
from email import message
from airflow.models import DAG, Variable
from airflow.operators.python import get_current_context


#Elegí la API "Alpha Vantage" que tiene datos de acciones a nivel global, indicadores económicos, cryptomonedas, etc
# Eligo los datos de acciones de Microsoft (MSFT)
symbol = 'MSFT'
with open('C:/Users/PC/airflow_docker/dags/api_key.txt', 'r') as file:
    api_key = file.read()

# Aca defino los parámetros de conexión con Redshift
redshift_endpoint = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
with open('C:/Users/PC/airflow_docker/dags/redshift_user.txt', 'r') as file:
    redshift_user = file.read()
with open('C:/Users/PC/airflow_docker/dags/redshift_password.txt', 'r') as file:
    redshift_pass = file.read()
redshift_db = 'dev'
redshift_port = '5439'


# Conexión con Redshift
conn = psycopg2.connect(
    host=redshift_endpoint,
    user=redshift_user,
    password=redshift_pass,
    dbname=redshift_db,
    port=redshift_port
)


#EMPIEZO A DEFINIR LAS FUNCIONES QUE VAN A CORRER EN LAS DIFERENTES TASKS
def enviar_alerta(asunto, mensaje):
    # Datos de configuración del servidor SMTP
    smtp_host = 'smtp.gmail.com'
    smtp_port = 587
    smtp_usuario = 'joaquinaliagaf@gmail.com'
    with open('C:/Users/PC/airflow_docker/dags/smtp_password.txt', 'r') as file:
        smtp_contraseña = file.read()
    

    # Direcciones de correo electrónico
    remitente = 'joaquinaliagaf@gmail.com'
    destinatario = 'joaquinaliagaf@gmail.com'

    # Crea el objeto SMTP
    servidor_smtp = smtplib.SMTP(smtp_host, smtp_port)

    # Inicia la conexión y autenticación con el servidor SMTP
    servidor_smtp.starttls()
    servidor_smtp.login(smtp_usuario, smtp_contraseña)

    # Crea el mensaje de correo electrónico
    mensaje_email = f'From: {remitente}\nTo: {destinatario}\nSubject: {asunto}\n\n{mensaje}'

    # Envía el correo electrónico
    servidor_smtp.sendmail(remitente, destinatario, mensaje_email)

    # Cierra la conexión con el servidor SMTP
    servidor_smtp.quit()

# funcion de extraccion de datos
def extraer_datos():
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&apikey={api_key}'

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Lanza una excepción si hay un error en la solicitud HTTP

        data = response.json()
        df = pd.DataFrame(data['Time Series (Daily)']).transpose().reset_index()
        
        # Convertir DataFrame a lista
        df_list = df.values.tolist()
        
        return df_list
    
    except requests.exceptions.RequestException as e:
        print('Error al realizar la solicitud HTTP:', e)
        asunto = 'Error en extracción de los datos'
        mensaje = f'La extracción de los datos de la API no se pudo realizar debido a un error en el proceso'
        enviar_alerta(asunto, mensaje)

# Funcion de transformacion en tabla
def transformar_datos(**context):
  df_list = context['ti'].xcom_pull(task_ids='extraer_datos')
  
  df = pd.DataFrame(df_list)
#Le pongo nombre a las columnas
  df.columns=['date', 'open_price', 'high', 'low', 'close','adj_close', 'volume', 'dividend_amount', 'split_coefficient']
#convierto el dato date a formato fecha
  df['date'] = pd.to_datetime(df['date'])
#ordeno todos los valores en torno a la fecha
  df = df.sort_values('date')
#convierto todos los datos de números en formato numérico
  numeric_columns = ['open_price', 'high', 'low', 'close', 'adj_close', 'dividend_amount']
  df[numeric_columns] = df[numeric_columns].astype(float)
#algunos datos de adj_close tienen muchos decimales entonces redondeo todo en dos decimales
  df['adj_close'] = df['adj_close'].round(2)
#elimino las columnas dividend_amount porque es siempre 0 y split_coefficient porque es siempre 1 (datos irrelevantes)
  df = df.drop(['dividend_amount', 'split_coefficient'], axis=1)

  df['date'] = df['date'].dt.strftime('%Y-%m-%d')
  df_list = df.values.tolist()
        
  return df_list

#Funcion de subir los datos a Redshift (carga)
def cargar_datos(**context):
   
    df_list = context['ti'].xcom_pull(task_ids='transformar_datos')

    df = pd.DataFrame(df_list)
    df.columns=['date', 'open_price', 'high', 'low', 'close','adj_close', 'volume']
    df['date'] = pd.to_datetime(df['date'])

    with conn.cursor() as cursor:
        cursor.execute('SELECT MAX(date) FROM microsoft_daily_prices')
        latest_date = cursor.fetchone()[0]
    latest_date = pd.to_datetime(latest_date)
    # Llamada a transformar_datos pasando df como argumento
    new_data = df[df['date'] > latest_date]
    
    # Verificar thresholds y enviar alertas si se superan
    threshold_valor = 100 
# Inserto los datos nuevos en la tabla de Redshift
    with conn.cursor() as cursor:
      for _, row in new_data.iterrows():
        cursor.execute(f"INSERT INTO microsoft_daily_prices (date, open_price, high, low, close, adj_close, volume) VALUES ('{row['date']}', {row['open_price']}, {row['high']}, {row['low']}, {row['close']}, {row['adj_close']}, {row['volume']})")
    conn.commit()    
    # Verificar si el valor supera el threshold
    if new_data['close'].max() > threshold_valor:
        asunto = 'Alerta de valor máximo'
        mensaje = f'El valor máximo ({new_data["close"].max()}) ha superado el threshold de {threshold_valor}.'
        enviar_alerta(asunto, mensaje)


dag = DAG(
    'joaquinaliaga_',
    start_date=datetime(2023, 6, 18),
    schedule_interval='0 0 * * *'  # Programa la ejecución diaria a la medianoche
)

t1 = PythonOperator(
    task_id='extraer_datos',
    python_callable=extraer_datos,
    dag=dag
)

t2 = PythonOperator(
    task_id='transformar_datos',
    python_callable=transformar_datos,
    dag=dag
)

t3 = PythonOperator(
    task_id='cargar_datos',
    python_callable=cargar_datos,
    dag=dag
)

t1 >> t2 >> t3
