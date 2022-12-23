try:

    from datetime import timedelta
    from airflow.models import Variable
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.operators.postgres_operator import PostgresOperator
    from datetime import datetime
    import pandas as pd
    from utils import api, features_cal

    print("All Dag utils are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

vrs = Variable.get("variables", deserialize_json=True)
key = vrs["key"]


def get_currency_list(**context):
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    df = hook.get_pandas_df(sql="SELECT currency FROM currency_data;")
    currency_list = list(df['currency'])
    context['ti'].xcom_push(key='cur_list', value=currency_list[:3])


def get_currency_rates(**context):
    currency_list = context.get('ti').xcom_pull(key='cur_list')
    today = context['ds']
    for currency in currency_list:
        link = f"https://api.apilayer.com/fixer/{today}?symbols={currency}&base=USD&apikey={key}"
        request = api.ApiCall()
        request.get_data(link)


def features_collection(**context):
    currency_list = context.get('ti').xcom_pull(key='cur_list')
    features = features_cal.Feature()
    features.feature_calc(currency_list)


with DAG(
        dag_id="data_collection",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2022, 11, 4),
            "depends_on_past": True,
        },
        max_active_runs=1,
        catchup=True) as f:
    get_currency_list = PythonOperator(
        task_id='get_currency_list',
        python_callable=get_currency_list
    )
    get_currency_rates = PythonOperator(
        task_id='get_currency_rates',
        python_callable=get_currency_rates,
    )
    features_collection = PythonOperator(
        task_id='features_collection',
        python_callable=features_collection,
    )
    populate_currency_table = PostgresOperator(
        task_id="populate_currency_table",
        sql="""call populate_currency_table()""",
        postgres_conn_id="postgres_localhost",
    )
    forcasting = PostgresOperator(
        task_id="forcasting",
        sql="""call forcasting()""",
        postgres_conn_id="postgres_localhost",
    )
    metrics = PostgresOperator(
        task_id="metrics",
        sql="""call metrics()""",
        postgres_conn_id="postgres_localhost",
    )

get_currency_list >> get_currency_rates >> populate_currency_table >> forcasting >> metrics
populate_currency_table >> features_collection
