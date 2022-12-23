try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.postgres_operator import PostgresOperator
    from datetime import date, datetime

    print("All Dag utils are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


with DAG(
        dag_id="drop_db",
        schedule_interval='@once',
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime.today(),
        },
        catchup=False) as f:
    drop_db = PostgresOperator(
        task_id="drop_db",
        sql="""call drop_db()""",
        postgres_conn_id="postgres_localhost",
    )

drop_db
