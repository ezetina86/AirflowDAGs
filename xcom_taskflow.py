from datetime import datetime, timedelta

from airflow.decorators import task, dag


@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
# Defining the kind of parameter I'm returning using  -> Dict[str, str].
def extract():
    partner_name = "duralast"
    partner_path = "/partners/duralast"
    return {"partner_name": partner_name, "partner_path": partner_path}


@task.python
def process(partner_name, partner_path):
    print(partner_name)
    print(partner_path)


@dag(description="A new way to write DAGs",
     start_date=datetime(2023, 1, 31),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=1),
     tags=["test", "certification", "az"],
     default_args=dict(owner="Enrique Zetina"),
     catchup=False,
     max_active_runs=1)
def xcom_taskflow():
    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])


dag = xcom_taskflow()
