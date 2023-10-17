# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments

default_args = {
'owner': 'lokal lampung',
'start_date': days_ago(0),
'email': ['lokallampung@rocketmail.com'],
'email_on_failure': True,
'email _on_retry': True,
'retries': 1,
'retry_delay': timedelta(minutes=1),
}


# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

unzip_data = BashOperator(
task_id='unzip_data',
bash_command='tar -xvzf /home/project/airflow/dags/tolldata.tgz -C /home/project/airflow/dags/',
dag=dag,
)

tsv_permission = BashOperator(
task_id='tsv_permission',
bash_command='chmod +x /home/project/airflow/dags/tollplaza-data.tsv',
dag=dag,
)

csv_permission = BashOperator(
task_id='csv_permission',
bash_command='chmod +x /home/project/airflow/dags/vehicle-data.csv',
dag=dag,
)

fixed_width_permission = BashOperator(
task_id='fixed_width_permission',
bash_command='chmod +x /home/project/airflow/dags/payment-data.txt',
dag=dag,
)

extract_data_from_csv = BashOperator(
task_id='extract_data_from_csv',
bash_command='cut -d"," -f1-4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_file.csv',
dag=dag,
)

extract_data_from_tsv = BashOperator(
task_id='extract_data_from_tsv',
bash_command="cut -d$'\\t' -f5-7 /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv",
dag=dag,
)

extract_data_from_fixed_width = BashOperator(
task_id='extract_data_from_fixed_width',
bash_command='cut -c59-67 /home/project/airflow/dags/payment-data.txt| tr " " "," > /home/project/airflow/dags/fixed_width_data.csv',
dag=dag,
)

consolidate_data = BashOperator(
task_id='consolidate_data',
bash_command='paste -d"," /home/project/airflow/dags/csv_file.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
dag=dag,
)

transform_load_data = BashOperator(
task_id='transform_load_data',
bash_command='cat /home/project/airflow/dags/extracted_data.csv | tr [a-z] [A-Z] > /home/project/airflow/dags/transformed_data.csv',
dag=dag,
)

# task pipeline
unzip_data >> tsv_permission 
unzip_data >> csv_permission
unzip_data >> fixed_width_permission

tsv_permission		>> extract_data_from_tsv
csv_permission		>> extract_data_from_csv
fixed_width_permission	>> extract_data_from_fixed_width

extract_data_from_csv		>> consolidate_data
extract_data_from_tsv		>> consolidate_data
extract_data_from_fixed_width	>> consolidate_data

consolidate_data >> transform_load_data