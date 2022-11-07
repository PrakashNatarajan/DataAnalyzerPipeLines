from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {'start_date': datetime(2022, 11, 7)}

def _choose_best_model():
  accuracy = 6
  if accuracy > 5:
    return 'accurate'
  return 'inaccurate'
  
dag = DAG(dag_id='branch_operator_test', schedule_interval='@daily', default_args=default_args, catchup=False)

choose_best_model = BranchPythonOperator(task_id='choose_best_model', python_callable=_choose_best_model, dag=dag)

accurate = EmptyOperator(task_id='accurate', dag=dag)

inaccurate = EmptyOperator(task_id='inaccurate', dag=dag)

choose_best_model >> [accurate, inaccurate]
