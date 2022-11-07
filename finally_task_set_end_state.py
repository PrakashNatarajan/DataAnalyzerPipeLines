import airflow
from airflow import AirflowException
from airflow.models import DAG, TaskInstance, BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

default_args = {"owner": "airflow", "start_date": airflow.utils.dates.days_ago(3)}

dag = DAG(
    dag_id="finally_task_set_end_state",
    default_args=default_args,
    schedule="0 0 * * *",
    description="Answer for question https://stackoverflow.com/questions/51728441",
)

start = BashOperator(task_id="start", bash_command="echo start", dag=dag)
failing_task = BashOperator(task_id="failing_task", bash_command="exit 1", dag=dag)


@provide_session
def _finally(task, execution_date, dag, session=None, **_):
    upstream_task_instances = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.execution_date == execution_date,
            TaskInstance.task_id.in_(task.upstream_task_ids),
        )
        .all()
    )
    upstream_states = [ti.state for ti in upstream_task_instances]
    fail_this_task = State.FAILED in upstream_states

    print("Do logic here...")

    if fail_this_task:
        raise AirflowException("Failing task because one or more upstream tasks failed.")


finally_ = PythonOperator(
    task_id="finally",
    python_callable=_finally,
    trigger_rule=TriggerRule.ALL_DONE,
    provide_context=True,
    dag=dag,
)

succes_task = EmptyOperator(task_id="succes_task", dag=dag)

start >> [failing_task, succes_task] >> finally_