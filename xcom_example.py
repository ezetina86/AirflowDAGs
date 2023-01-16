from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

from datetime import datetime, timedelta


def _my_partner(ti):
    partner_name = "AutoZone"
    partner_path = "/parteners/AutoZone"
    return {"partner_name": partner_name, "partner_path":partner_path}
    ##ti.xcom_push(key="partner_name", value=partner_name)


def _process(ti):
    partner_name = ti.xcom_pull(task_ids="_my_partner")
    print(partner_name)


@dag(dag_id="xcom_example",
     description="DAG to show how to use XCOM to pass data",
     start_date=datetime(2023, 1, 1),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=10),
     tags=["dag-authoring", "astro"],
     default_args=dict(owner="Enrique Zetina",
                       retries=2,
                       retry_delay=timedelta(minutes=3)),
     catchup=False)
def xcom_dag():
    @task()
    def partner():
        PythonOperator(
            task_id="extract",
            python_callable=_process
        )

    @task()
    def process():
        PythonOperator(
            task_id="process",
            python_callable=_my_partner
        )

    partner() >> process()


dag = xcom_dag()
