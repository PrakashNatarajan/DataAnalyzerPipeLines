import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import configs_worker
import database_worker


default_args = {'start_date': datetime(2022, 12, 11)}
loader_configs = {}

def _add_missed_user_data():
  print("Transform and Loading New Data")
  database_worker.add_missed_user_data(loader_configs['TABLE'])

def _assign_user_grouped_data():
  print("Transform and Loading New Data")
  database_worker.assign_user_group(loader_configs['TABLE'])

def _remove_previous_data():
  print("Dropped Existing Data")
  loader_configs['values'] = ""
  database_worker.delete_record_query(loader_configs['TABLE'], loader_configs['COLUMNS'], loader_configs['values'])


loader_configs = configs_worker.fetch_loader_configs(loader_name="SIX_EXTERNAL_LEVEL")

#trgr_dag = DAG(dag_id="daf_four_parallel_executor", start_date=pendulum.datetime(2022, 12, 11, tz="UTC"), catchup=False, schedule="@once", tags=['example'])
trgr_dag = DAG(dag_id="daf_six_parallel_executor", start_date=pendulum.datetime(2022, 12, 11, tz="UTC"), catchup=False, schedule="@once")

start_execution = EmptyOperator(task_id='start_parallel_execution', dag = trgr_dag)
end_execution = EmptyOperator(task_id='end_parallel_execution', dag = trgr_dag)

add_missed_user_data = PythonOperator(task_id='add_missed_user_data', trigger_rule='none_failed_or_skipped', python_callable=_add_missed_user_data, dag=trgr_dag)
assign_user_grouped_data = PythonOperator(task_id='assign_user_grouped_data', trigger_rule='none_failed_or_skipped', python_callable=_assign_user_grouped_data, dag=trgr_dag)
remove_previous_data = PythonOperator(task_id='remove_previous_data', trigger_rule='none_failed_or_skipped', python_callable=_remove_previous_data, dag=trgr_dag)


for file_part in loader_configs['FILE_PARTS']:
  trigger_task = "trigger_six_external_{file_part}".format(file_part = file_part)
  trigger_dag = TriggerDagRunOperator(task_id=trigger_task, trigger_dag_id="daf_six_external_level", conf={"file_part": file_part})
  start_execution >> trigger_dag >> end_execution


end_execution >> add_missed_user_data >> assign_user_grouped_data >> remove_previous_data
