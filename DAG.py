from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_etl_pipeline',
    default_args=default_args,
    description= 'Simple ETL pipeline',
    schedule= '0 2 * * *', #Run daily at 2AM
    start_date = datetime(2026, 4,5),
    catchup = False,
    tags =['tutorial', 'etl'],
)

def extract_data():
    print("Extracting data from DB....")
    #Your extract logic here
    return "Data extracted"

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag = dag,
)

def transform_data():
    print("Transforming data....")
    #Your transform logic here
    return "Data transformed"

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag = dag,
)

def load_data():
    print("Loading data to warehouse....")
    return "Data loaded"

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag = dag,
)

validate_task = BashOperator(
    task_id='validate',
    bash_command= 'echo "Data validated"',
    dag = dag,
)


