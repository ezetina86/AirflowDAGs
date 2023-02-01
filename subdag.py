from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator

from subdags.subdag_factory import subdag_factory


@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "duralast"
    partner_path = "/partners/duralast"
    return {"partner_name": partner_name, "partner_path": partner_path}


default_args = {"start_date": datetime(2023, 1, 31)}


@dag(description="Using subdag",
     default_args=default_args,
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=1),
     tags=["test", "certification", "az"],
     catchup=False,
     max_active_runs=1)
def subdag():
    process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory("subdag", "process_tasks", default_args),
        poke_interval=15
    )

    extract() >> process_tasks


dag = subdag()
