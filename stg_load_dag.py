import logging
import pendulum
from airflow.decorators import dag, task
from typing import Dict, List, Optional
from stg_load.currenciesClass import CurrenciesLoader
from stg_load.transactionsClass import TransactionsLoader
from stg_load.martClass import MartDataLoader
from stg_load.lib.pg_connect import ConnectionBuilder
from stg_load.lib.vertica_connect import ConnectionVerticaBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
    catchup=True,
    tags=['final_project'],
    is_paused_upon_creation=True
)
def transactions_dag_final():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_TRANSACTIONS_CONNECTION")
    dwh_vertica_connect = ConnectionVerticaBuilder.vertica_conn("VERTICA_TRANSACTIONS_CONNECTION")

    # Таск, который загружает данные transactions во временный файл.
    @task(task_id="transactions_load_to_file", provide_context=True)
    def transactions_load_to_file(**context):
        transactions_loader = TransactionsLoader(log, context['prev_execution_date'])
        transactions_loader.transactions_load_stg_to_file(dwh_pg_connect)

    # Таск, который загружает данные currencies во временный файл.
    @task(task_id="currencies_load_to_file", provide_context=True)
    def currencies_load_to_file(**context):
        currencies_loader = CurrenciesLoader(log, context['prev_execution_date'])
        currencies_loader.currencies_load_stg_to_file(dwh_pg_connect)  

    # Таск, который загружает данные transactions из временного файла в вертику.
    @task(task_id="transactions_load_to_vertica", provide_context=True)
    def transactions_load_to_vertica(**context):
        transactions_loader = TransactionsLoader(log, context['prev_execution_date'])
        transactions_loader.load_transactions_file_to_vertica(dwh_vertica_connect)

    # Таск, который загружает данные currencies из временного файла в вертику.
    @task(task_id="currencies_load_to_vertica", provide_context=True)
    def currencies_load_to_vertica(**context):
        currencies_loader = CurrenciesLoader(log, context['prev_execution_date'])
        currencies_loader.load_currencies_file_to_vertica(dwh_vertica_connect)

    # Таск, который загружает данные currencies из стейджа в витрину.
    @task(task_id="data_mart_load_vertica", provide_context=True)
    def data_mart_load_vertica(**context):
        mart_loader = MartDataLoader(log, context['prev_execution_date'], dwh_vertica_connect)
        mart_loader.mart_load()

    # Инициализируем объявленные таски.
    transactions_load_file = transactions_load_to_file()
    currencies_load_file = currencies_load_to_file()
    transactions_load_vertica = transactions_load_to_vertica()
    currencies_load_vertica = currencies_load_to_vertica()
    data_mart_load = data_mart_load_vertica()

    currencies_load_file >> currencies_load_vertica
    transactions_load_file >> transactions_load_vertica
    (currencies_load_vertica, transactions_load_vertica) >> data_mart_load
    
transactions_dag_final = transactions_dag_final()