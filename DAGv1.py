from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(schedule='0 2 * * *', start_date=datetime(2026, 4, 5), catchup=False, tags=['tutorial', 'etl'])
def my_first_pipeline():

    @task
    def extract():
        print("Extracting...")
        return {"data": [1, 2, 3]}

    @task
    def transform(raw_data):
        print("Transforming...")
        return {"transformed": raw_data}

    @task
    def load(transformed_data):
        print("Loading...")

    raw = extract()
    transformed = transform(raw)
    load(transformed)

my_first_pipeline()

