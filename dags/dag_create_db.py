try:
    from datetime import timedelta
    from airflow.models import Variable
    from airflow import DAG
    from airflow.operators.postgres_operator import PostgresOperator
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    from utils import api as api
    from utils.params import currency_link
    import os

    print("All Dag utils are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

vrs = Variable.get("variables", deserialize_json=True)
key = vrs["key"]


def procedures_init():
    for file in os.listdir('dags/sql'):
        pg = PostgresOperator(
            task_id='init',
            postgres_conn_id='postgres_localhost',
            sql=f'sql/{file}'
        )
        pg.execute(dict())


def currency_data_fill_up():
    link = currency_link + key
    request = api.ApiCall()
    request.get_data(link)


with DAG(
        dag_id="create_db",
        schedule_interval='@once',
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime.today(),
        },
        catchup=False) as f:
    procedures_init = PythonOperator(
        task_id='procedures_init',
        python_callable=procedures_init
    )
    create_db = PostgresOperator(
        task_id="create_db",
        sql="""call create_db()""",
        postgres_conn_id="postgres_localhost",
    )
    currency_data_fill_up = PythonOperator(
        task_id='currency_data_fill_up',
        python_callable=currency_data_fill_up
    )

procedures_init >> create_db >> currency_data_fill_up
