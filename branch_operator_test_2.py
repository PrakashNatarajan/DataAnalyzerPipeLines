from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {'start_date': datetime(2022, 11, 7)}

def _choose_best_model():
  accuracy = 9
  if accuracy > 7:
    return ['super_accurate', 'accurate']
  elif accuracy > 5:
    return 'accurate'
  return 'inaccurate'

dag = DAG(dag_id='branch_operator_test_2', schedule='@daily', default_args=default_args, catchup=False)

choose_best_model = BranchPythonOperator(task_id='choose_best_model', python_callable=_choose_best_model, do_xcom_push=False, dag=dag)

accurate = EmptyOperator(task_id='accurate', dag=dag)

inaccurate = EmptyOperator(task_id='inaccurate', dag=dag)

super_accurate = EmptyOperator(task_id='super_accurate', dag=dag)

storing = EmptyOperator(task_id='storing', trigger_rule='none_failed_or_skipped', dag=dag)

choose_best_model >> [super_accurate, accurate, inaccurate] >> storing
