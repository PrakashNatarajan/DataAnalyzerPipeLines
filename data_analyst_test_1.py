from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

default_args = {'start_date': datetime(2022, 11, 7)}

def _check_source_file_status():
  fileAvailability = True
  if fileAvailability:
    return 'download_source_file'
  return 'source_file_exception'

def _validate_source_file():
  validationPassed = True
  if validationPassed:
    return 'drop_destination_data'
  return 'validation_failed_exception'
  
dat_dag = DAG(dag_id='data_analyst_test_1', schedule='@daily', default_args=default_args, catchup=False)

build_file_paths = EmptyOperator(task_id='build_file_paths', dag=dat_dag)

check_source_status = BranchPythonOperator(task_id='check_source_status', python_callable=_check_source_file_status, do_xcom_push=False, dag=dat_dag)
source_file_exception = EmptyOperator(task_id='source_file_exception', dag=dat_dag)

download_source_file = EmptyOperator(task_id='download_source_file', dag=dat_dag)

validate_source_file = BranchPythonOperator(task_id='validate_source_file', python_callable=_validate_source_file, do_xcom_push=False, dag=dat_dag)
validation_failed_exception = EmptyOperator(task_id='validation_failed_exception', dag=dat_dag)

drop_destination_data = EmptyOperator(task_id='drop_destination_data', trigger_rule='none_failed_or_skipped', dag=dat_dag)

transform_load_destination = EmptyOperator(task_id='transform_load_destination', trigger_rule='none_failed_or_skipped', dag=dat_dag)


build_file_paths >> check_source_status >> [download_source_file, source_file_exception]
download_source_file >> validate_source_file >> [drop_destination_data, validation_failed_exception]
drop_destination_data >> transform_load_destination
