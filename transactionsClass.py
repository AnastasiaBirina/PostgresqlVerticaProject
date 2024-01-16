from logging import Logger
from typing import Dict
import csv
import os

from stg_load.lib.pg_connect import PgConnect
from psycopg import Connection
import pandas as pd
import vertica_python
import contextlib
from datetime import timedelta, date


class TransactionsLoader:

    def __init__(self, log: Logger, prev_execution_date) -> None:
        
        self.log = log
        self.previous_date = str(prev_execution_date)

        TRANSACTIONS_FILE_PATH = "/lessons/dags/stg_load/temporary_data/transactions_{}.csv".format(self.previous_date)
        self.filename = TRANSACTIONS_FILE_PATH

    def transactions_load_stg_to_file(self, pg_origin: PgConnect):
        
        # Удаляем файл, если он уже существует
        self.delete_file()

        self.log.info('LOG CUSTOM: FILE TRANSACTIONS_{} IS DELETED'.format(self.previous_date)) 

        # Получаем данные из Postgresql
        with pg_origin.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT *
                        FROM public.transactions
                        WHERE transaction_dt::date = %s
                    """, 
                (self.previous_date,)  
                )
                data = cur.fetchall()
                column_names = [desc[0] for desc in cur.description] 

        # Записываем данные в файл CSV
        with open(self.filename, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(column_names)
            writer.writerows(data)

        self.log.info('LOG CUSTOM: FILE TRANSACTIONS_{} IS LOADED'.format(self.previous_date))
   
    def load_transactions_file_to_vertica(self, vertica_conn):
        
        df = pd.read_csv(self.filename)
        num_rows = len(df)
        copy_expr = """
                    COPY STV2023060622__STAGING.transactions_copy (operation_id,account_number_from,account_number_to,currency_code,country,status,transaction_type,amount,transaction_dt) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
                    """

        with contextlib.closing(vertica_conn.cursor()) as cur:
            
            # На всякий случай удаляем партицию за указанную дату
            self.log.info('LOG CUSTOM: TRANSACTIONS DELETE PARTITION. START FOR ' + str(self.previous_date))
            cur.execute(
                        """
                        SELECT DROP_PARTITIONS(
                            'STV2023060622__STAGING.transactions_copy', 
                            '{}', '{}'
                        );
                        """.format(self.previous_date, self.previous_date) 
                    )
            
            vertica_conn.commit()

            # Проверяем, все ли удалилось
            cur.execute(
                        """
                        SELECT COUNT(*) FROM STV2023060622__STAGING.transactions_copy WHERE transaction_dt::DATE = '{}'
                        ;
                        """.format(self.previous_date) 
                    )

            self.log.info('LOG CUSTOM: TRANSACTIONS DELETE PARTITION. END FOR ' + str(self.previous_date) + '. CHECK COUNT: ' + str(cur.fetchone()[0]))
            
            # Загружаем данные в вертику
            self.log.info('LOG CUSTOM: TRANSACTIONS_{} IS LOADED TO VERTICA. START'.format(self.previous_date))
            
            with open(self.filename, 'rb') as file:
                cur.copy(copy_expr, file, buffer_size=65536)

            vertica_conn.commit()
        
        vertica_conn.close()

        # Удаляем файл
        self.delete_file()
        
        self.log.info('LOG CUSTOM: TRANSACTIONS_{} IS LOADED TO VERTICA. END'.format(self.previous_date))

    def delete_file(self):
        if os.path.exists(self.filename):
            try:
                os.remove(self.filename)
            except FileNotFoundError:
                pass  # Файл уже удален или не существует 
        