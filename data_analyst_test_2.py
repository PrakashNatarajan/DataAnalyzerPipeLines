from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
import configs_worker
import file_paths_worker


default_args = {'start_date': datetime(2022, 11, 7)}

def _build_file_paths(loader_configs):
  loader_configs['src_file_path'] = file_paths_worker.build_source_file_path(configs=loader_configs)
  loader_configs['dst_file_path'] = file_paths_worker.build_destination_file_path(configs=loader_configs)
  #return context

def _check_source_file_status(loader_configs):
  print("\n\n\n")
  print(loader_configs)
  print("\n\n\n")
  fileAvailability = True
  if fileAvailability:
    return 'download_source_file'
  return 'source_file_exception'

def _source_file_exception(loader_configs):
  print("\n\n\n")
  print("Sent Mail")
  print(loader_configs)
  print("\n\n\n")

def _download_source_file(loader_configs):
  print("\n\n\n")
  print("Downloaded File")
  print(loader_configs)
  print("\n\n\n")

def _validate_source_file():
  validationPassed = True
  if validationPassed:
    return 'drop_destination_data'
  return 'validation_failed_exception'

def _validation_failed_exception(loader_configs):
  print("\n\n\n")
  print("Sent Mail")
  print(loader_configs)
  print("\n\n\n")

def _drop_destination_data(loader_configs):
  print("\n\n\n")
  print("Dropped Existing Data")
  print(loader_configs)
  print("\n\n\n")

def _transform_load_destination(loader_configs):
  print("\n\n\n")
  print("Transform and Loading New Data")
  print(loader_configs)
  print("\n\n\n")
  
dat_dag = DAG(dag_id='data_analyst_test_2', schedule='@daily', default_args=default_args, catchup=False)

loader_configs = configs_worker.fetch_loader_configs(loader_name="QGP_USER_LEVEL")
build_file_paths = PythonOperator(task_id='build_file_paths', python_callable=_build_file_paths, op_kwargs = {"loader_configs" : loader_configs}, dag=dat_dag)

check_source_status = BranchPythonOperator(task_id='check_source_status', python_callable=_check_source_file_status, op_kwargs = {"loader_configs" : loader_configs}, do_xcom_push=False, dag=dat_dag)
source_file_exception = PythonOperator(task_id='source_file_exception', python_callable=_source_file_exception, op_kwargs = {"loader_configs" : loader_configs}, dag=dat_dag)

download_source_file = PythonOperator(task_id='download_source_file', python_callable=_download_source_file, op_kwargs = {"loader_configs" : loader_configs}, dag=dat_dag)

validate_source_file = BranchPythonOperator(task_id='validate_source_file', python_callable=_validate_source_file, do_xcom_push=False, dag=dat_dag)
validation_failed_exception = PythonOperator(task_id='validation_failed_exception', python_callable=_validation_failed_exception, op_kwargs = {"loader_configs" : loader_configs}, dag=dat_dag)

drop_destination_data = PythonOperator(task_id='drop_destination_data', trigger_rule='none_failed_or_skipped', python_callable=_drop_destination_data, op_kwargs = {"loader_configs" : loader_configs}, dag=dat_dag)

transform_load_destination = PythonOperator(task_id='transform_load_destination', trigger_rule='none_failed_or_skipped', python_callable=_transform_load_destination, op_kwargs = {"loader_configs" : loader_configs}, dag=dat_dag)


build_file_paths >> check_source_status >> [download_source_file, source_file_exception]
download_source_file >> validate_source_file >> [drop_destination_data, validation_failed_exception]
drop_destination_data >> transform_load_destination
