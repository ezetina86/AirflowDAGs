from datetime import datetime, timedelta
from airflow.decorators import task, dag


@task.python
def extract():
    partner_name = "duralast"
    return partner_name


@task.python
def process(partner_name):
    print(partner_name)


@dag(description="A new way to write DAGs",
     start_date=datetime(2023, 1, 31),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=1),
     tags=["test", "certification", "az"], default_args=dict(owner="Enrique Zetina"),
     catchup=False,
     max_active_runs=1)
def new_dag():
    process(extract())


dag = new_dag()
