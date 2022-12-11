from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="test_trigger_controller_dag",
    start_date=pendulum.datetime(2022, 12, 11, tz="UTC"),
    catchup=False,
    schedule="@once",
    tags=['example'],
) as dag:
    start_execution = EmptyOperator(task_id='start_parallel_execution')
    end_execution = EmptyOperator(task_id='end_parallel_execution')
    for file_part in [1, 2, 3, 4, 5, 6, 7]:
        trigger = TriggerDagRunOperator(
            task_id="test_trigger_dagrun_{task_id}".format(task_id = file_part),
            trigger_dag_id="test_trigger_target_dag",  # Ensure this equals the dag_id of the DAG to trigger
            conf={"file_part": file_part},
            )
        start_execution >> trigger >> end_execution