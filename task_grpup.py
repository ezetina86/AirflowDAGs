from datetime import datetime, timedelta
from airflow.decorators import task, dag
from groups.process_tasks import process_tasks


@task.python(task_id="extract", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "autozone"
    partner_path = "/partner/autozone"
    return {"partner_name": partner_name, "partner_path": partner_path}


default_args = {
    "start_date": datetime(2023, 1, 1)
}


@dag(description="DAG to work with Task Groups",
     default_args=default_args,
     dagrun_timeout=timedelta(minutes=1),
     tags=["exam-preparation"],
     catchup=False,
     max_active_runs=1)
def task_group_dag():
    partner_settings = extract()

    process_tasks(partner_settings)


dag = task_group_dag()
