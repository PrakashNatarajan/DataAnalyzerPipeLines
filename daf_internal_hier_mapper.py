from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
import configs_worker
import file_paths_worker
import source_worker
import exception_worker
import validation_worker
import database_worker
import trans_load_worker


default_args = {'start_date': datetime(2022, 11, 7)}
loader_configs = {}

def _build_file_paths():
  loader_configs['src_file_path'] = file_paths_worker.build_source_day_file_path(configs=loader_configs)
  loader_configs['dst_file_path'] = file_paths_worker.build_destination_file_path(configs=loader_configs)
  loader_configs['current_time'] = datetime.now().strftime("%Y%m%d%H%M")
  #tsk_intx.xcom_push(key='loader_configs', value=loader_configs)
  return loader_configs

def _check_source_file_status(**kwargs):
  print("Checking Source File Status")
  #loader_configs = tsk_intx.xcom_pull(key='loader_configs', task_ids='build_file_paths')
  tsk_intx = kwargs['ti'] ##Task Instance
  loader_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  fileAvailability = source_worker.check_file_status(loader_configs)
  if fileAvailability:
    return 'download_source_file'
  return 'source_file_exception'

def _source_file_exception(**kwargs):
  tsk_intx = kwargs['ti'] ##Task Instance
  loader_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  exception_worker.source_not_available(loader_configs)


def _download_source_file(**kwargs):
  print("Downloaded File")
  tsk_intx = kwargs['ti'] ##Task Instance
  loader_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  source_worker.download_data_file(loader_configs)

def _validate_source_file(**kwargs):
  tsk_intx = kwargs['ti'] ##Task Instance
  load_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  validColumns = validation_worker.right_column_names(load_configs)
  if validColumns:
    return 'transform_load_destination'
  return 'validation_failed_exception'

def _validation_failed_exception(**kwargs):
  tsk_intx = kwargs['ti'] ##Task Instance
  load_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  loader_configs['source_columns'] = validation_worker.fetch_column_names(load_configs)
  exception_worker.source_valid_failed(load_configs)

def _transform_load_destination(**kwargs):
  print("Transform and Loading New Data")
  tsk_intx = kwargs['ti'] ##Task Instance
  load_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  trans_load_worker.internal_role_hierarchy(loader_configs)

  
dat_dag = DAG(dag_id='daf_internal_hier_mapper', schedule='@daily', default_args=default_args, catchup=False)

loader_configs = configs_worker.fetch_loader_configs(loader_name="INTERNAL_HIERARCHY")
build_file_paths = PythonOperator(task_id='build_file_paths', python_callable=_build_file_paths, dag=dat_dag)

check_source_status = BranchPythonOperator(task_id='check_source_status', provide_context=True, python_callable=_check_source_file_status, do_xcom_push=False, dag=dat_dag)
source_file_exception = PythonOperator(task_id='source_file_exception', python_callable=_source_file_exception, dag=dat_dag)

download_source_file = PythonOperator(task_id='download_source_file', python_callable=_download_source_file, dag=dat_dag)

validate_source_file = BranchPythonOperator(task_id='validate_source_file', python_callable=_validate_source_file, do_xcom_push=False, dag=dat_dag)
validation_failed_exception = PythonOperator(task_id='validation_failed_exception', python_callable=_validation_failed_exception, dag=dat_dag)

transform_load_destination = PythonOperator(task_id='transform_load_destination', trigger_rule='none_failed_or_skipped', python_callable=_transform_load_destination, dag=dat_dag)

build_file_paths >> check_source_status >> [download_source_file, source_file_exception]
download_source_file >> validate_source_file >> [transform_load_destination, validation_failed_exception]
#transform_load_destination >> add_missed_user_data >> assign_user_grouped_data >> remove_previous_data
