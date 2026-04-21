import json
import sqlite3
from datetime import datetime
from urllib.request import urlopen

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = '/home/malik/airflow/data'
DB_PATH = '/home/malik/airflow/data/weather.db'

API_URL = (
    "https://api.open-meteo.com/v1/forecast?latitude=19.07&longitude=72.87&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Asia/Kolkata&past_days=7"
)

my_dag = DAG(
    'weather_etl_pipeline',
    description ='API to SQLite: Mumbai weather data',
    schedule = '0 6 * * *',
    start_date= datetime(2026, 4, 5),
    catchup= False,
    tags=['etl', 'api', 'weather'],
)

def extract():
    response = urlopen(API_URL)
    raw_data = json.loads(response.read().decode())
    with open(f"{DATA_DIR}/raw_weather.json", "w") as f:
        json.dump(raw_data, f)
    days = len(raw_data['daily']['time'])
    print(f"Extracted {days} days of weather data")

def transform():
    with open(f"{DATA_DIR}/raw_weather.json", "r") as f:
        raw = json.load(f)
    daily = raw['daily']
    df = pd.DataFrame({
        'date': daily['time'],
        'temp_max': daily['temperature_2m_max'],
        'temp_min': daily['temperature_2m_min'],
        'precipitation': daily['precipitation_sum']
    })
    df['temp_range'] = df['temp_max'] - df['temp_min']
    df['date'] = pd.to_datetime(df['date'])
    df.to_json(f"{DATA_DIR}/transformed_weather.json", orient= 'records')
    print(f"Transformed {len(df)} rows")
    print(f"Avg max temp: {df['temp_max'].mean()}C")
    print(f"Total precipitation: {df['precipitation'].sum()}mm")

def load():
    df = pd.read_json(f'{DATA_DIR}/transformed_weather.json')
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('weather', conn, if_exists='replace', index=False)
    count = conn.execute('SELECT COUNT(*) FROM weather').fetchone()[0]
    conn.close()
    print(f"Loaded {count} rows into weather table")

def validate():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql('SELECT * FROM weather', conn)
    conn.close()
    nulls = df.isnull().sum().sum()
    print(f"NUll values: {nulls} {'PASS' if nulls == 0 else 'FAIL'}")
    worst_temps = df[(df['temp_max'] > 50) | (df['temp_min'] < 10)]
    print(f"worst temperature rows: {len(worst_temps)} {'PASS' if len(worst_temps) == 0 else 'FAIL'}")
    print(f"Hottest day: {df.loc[df['temp_max'].idxmax(),'date']} at {df['temp_max'].max()}C")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")


extract_task = PythonOperator(task_id='extract', python_callable=extract, dag=my_dag)
transform_task = PythonOperator(task_id='transform', python_callable=transform, dag=my_dag)
load_task = PythonOperator(task_id='load', python_callable=load, dag=my_dag)
validate_task = PythonOperator(task_id='validate', python_callable=validate, dag=my_dag)

extract_task >> transform_task >> load_task >> validate_task

