from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _test_variables(name):
    my_variable = Variable.get("test_variable")
    print(my_variable)
    my_secret = Variable.get("secret_variable")
    print(my_secret)
    multiple_variables = Variable.get("multiple_variables", deserialize_json=True)
    email = multiple_variables["email"]
    print(email)
    print(name)
    return my_secret


@dag(dag_id="my_dag",
     description="DAG used during DAG Authoring preparation course",
     start_date=datetime(2023, 1, 1),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=10),
     tags=["dag-authoring", "astro"],
     default_args=dict(owner="Enrique Zetina",
                       retries=2,
                       retry_delay=timedelta(minutes=3)),
     catchup=False)
def my_dag():
    @task()
    def hello():
        BashOperator(task_id="hello",
                     bash_command="echo DAG started")

    @task()
    def test_variables():
        PythonOperator(task_id="test_variables",
                       python_callable=_test_variables,
                       op_args=["{{ var.json.multiple_variables.name }}"])

    @task()
    def bye():
        BashOperator(task_id="bye",
                     bash_command="echo DAG finished")

    hello() >> test_variables() >> bye()


dag = my_dag()
