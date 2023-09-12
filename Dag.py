from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from data_pipeline import extract_data, transfrom_data, load_data

default_args = {
    'owner': 'longmadlife',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 9),
    'email': ['airflow@hodinhlong.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'load_covid_data',
    catchup = False,
    default_args=default_args,
    description='covid_data',
    schedule=timedelta(days=1),
    ) 

extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag, 
    )

transfrom_task = PythonOperator(
        task_id='transfrom_data',
        python_callable=transfrom_data,
        dag=dag, 
    )

load_task = PythonOperator(
        task_id = "load_data",
        python_callable=load_data,
        op_kwargs={
            "dataset_name": "covid_19",
            "table_name": "covid_data"
        },
        dag=dag

    )
    # set the task
extract_task >> transfrom_task >> load_task