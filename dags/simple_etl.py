from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.s3 import (S3CreateObjectOperator)
from datetime import datetime, timedelta
import pandas as pd
import json

def calculate_age(birthdate):
    today = datetime.today()
    birth_date = datetime.strptime(birthdate, "%Y-%m-%d")
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl', default_args=default_args, schedule=None)

def process_data(**kwargs):
    ti = kwargs['ti']
    users = json.loads(ti.xcom_pull(task_ids='get_data'))
    for user in users:
        user['age'] = calculate_age(user['birthDate'])
    return json.dumps(users)

def convert_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='process_data')
    df = pd.DataFrame(json.loads(data))
    csv_content = df.to_csv(index=False)
    return csv_content


get_data_task = SimpleHttpOperator(
    task_id='get_data',
    method='GET',
    http_conn_id='http_default',
    endpoint='/users',
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

convert_json_to_csv_task = PythonOperator(
    task_id='convert_json_to_csv',
    python_callable=convert_data,
    dag=dag,
)

print_csv = BashOperator(
    task_id='print_csv',
    bash_command='echo "{{ ti.xcom_pull(task_ids="convert_json_to_csv") }}"',
    dag=dag,
)

upload_csv = S3CreateObjectOperator(
    task_id="upload_csv",
    s3_bucket='02-document',
    s3_key='people.csv',
    aws_conn_id='aws',
    data="{{ ti.xcom_pull(task_ids='convert_json_to_csv', key='return_value') }}",  
    replace=True,
)

get_data_task >> process_data_task >> convert_json_to_csv_task >> print_csv >> upload_csv
