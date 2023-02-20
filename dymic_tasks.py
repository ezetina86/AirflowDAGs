from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from groups.process_task_dynamic import process_tasks

partners = {
    "partner_epam":
        {
            "name": "epam",
            "path": "/partner/epam"
        },
    "partner_gcp":
        {
            "name": "gcp",
            "path": "/partner/gcp"
        },
    "partner_lucidworks":
        {
            "name": "lucidworks",
            "path": "/partner/lucidworks"
        },
}

default_args = {
    "start_date": datetime(2023, 1, 1)
}


@dag(description="DAG to work with dynamic tasks",
     default_args=default_args,
     dagrun_timeout=timedelta(minutes=1),
     tags=["exam-preparation"],
     catchup=False,
     max_active_runs=1)
def dynamic_tasks_dag():

    start = EmptyOperator(task_id="start")

    for partner, details in partners.items():

        @task.python(task_id=f"extract_{partner}", do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}

        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values)


dag = dynamic_tasks_dag()
