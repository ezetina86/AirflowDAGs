from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
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


def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    if day == 1:
        return 'extract_partner_epam'
    if day == 2:
        return 'extract_partner_lucidworks'
    if day == 3:
        return 'extract_partner_autozone'
    return 'stop'


@dag(description="DAG to work with branching",
     default_args=default_args,
     dagrun_timeout=timedelta(minutes=1),
     tags=["exam-preparation"],
     catchup=False,
     max_active_runs=1)
def branching_dag():
    start = EmptyOperator(task_id="start")

    choosing_partner_based_on_day = BranchPythonOperator(
        task_id='choosing_partner_based_on_day',
        python_callable=_choosing_partner_based_on_day

    )

    stop = EmptyOperator(task_id="stop")

    storing = EmptyOperator(task_id="storing",
                            trigger_rule='none_failed_or_skipped')

    choosing_partner_based_on_day >> stop
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}

        extracted_values = extract(details['name'], details['path'])
        start >> choosing_partner_based_on_day >> extracted_values
        process_tasks(extracted_values) >> storing


dag = branching_dag()
