from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator


class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')


@dag(dag_id="postgres_example",
     description="DAG to connect with postgres ans use runtime parameters",
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
    def fetching_data():
        CustomPostgresOperator(
            task_id="fetching_data",
            sql="sql/request.sql",
            parameters={
                'next_execution_date': '{{ next_ds }}',
                'previous_execution_date': '{{ prev_ds }}',
                'var2': '{{ var.json.newvariable.value2 }}'
            }
        )

    fetching_data()


dag = my_dag()
