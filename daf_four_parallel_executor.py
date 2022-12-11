from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import configs_worker
import database_worker


default_args = {'start_date': datetime(2022, 12, 11)}
loader_configs = {}

def _add_missed_user_data(**kwargs):
  print("Transform and Loading New Data")
  tsk_intx = kwargs['ti'] ##Task Instance
  load_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  database_worker.add_missed_user_data(load_configs['TABLE'])

def _assign_user_grouped_data(**kwargs):
  print("Transform and Loading New Data")
  tsk_intx = kwargs['ti'] ##Task Instance
  load_configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  database_worker.assign_user_group(load_configs['TABLE'])

def _remove_previous_data(**kwargs):
  print("Dropped Existing Data")
  tsk_intx = kwargs['ti'] ##Task Instance
  configs = tsk_intx.xcom_pull(task_ids='build_file_paths')
  configs['values'] = ""
  database_worker.delete_record_query(configs['TABLE'], configs['COLUMNS'], configs['values'])
  
dat_dag = DAG(dag_id='daf_four_parallel_executor', schedule='@daily', default_args=default_args, catchup=False)

loader_configs = configs_worker.fetch_loader_configs(loader_name="FOUR_EXTERNAL_LEVEL")
for file_part in loader_configs['FILE_PARTS']:
  trigger_dag = TriggerDagRunOperator(task_id="trigger_four_ext_level", trigger_dag_id="daf_four_external_level", conf={"file_part": file_part})
  trigger_dag >> add_missed_user_data

add_missed_user_data = PythonOperator(task_id='add_missed_user_data', trigger_rule='none_failed_or_skipped', python_callable=_add_missed_user_data, dag=dat_dag)
assign_user_grouped_data = PythonOperator(task_id='assign_user_grouped_data', trigger_rule='none_failed_or_skipped', python_callable=_assign_user_grouped_data, dag=dat_dag)
remove_previous_data = PythonOperator(task_id='remove_previous_data', trigger_rule='none_failed_or_skipped', python_callable=_remove_previous_data, dag=dat_dag)


add_missed_user_data >> assign_user_grouped_data >> remove_previous_data
