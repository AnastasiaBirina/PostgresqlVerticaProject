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


class MartDataLoader:

    def __init__(self, log: Logger, prev_execution_date, vertica_conn) -> None:
        
        MART_SCRIPT_PATH = "/lessons/dags/stg_load/sql/martScript.sql"
        self.sql_file = MART_SCRIPT_PATH
        self.log = log
        self.previous_date = str(prev_execution_date)
        self.vertica_conn = vertica_conn

    def mart_load(self):
        
        
        with self.vertica_conn.cursor() as cur:

            # На всякий случай удаляем партицию за указанную дату
            self.log.info('LOG CUSTOM: MART DELETE PARTITION. START FOR ' + str(self.previous_date))
            cur.execute(
                        """
                        SELECT DROP_PARTITIONS(
                            'STV2023060622__DWH.global_metrics_copy', 
                            '{}', '{}'
                        );
                        """.format(self.previous_date, self.previous_date) 
                    )
            
            self.vertica_conn.commit()

            # Проверяем, все ли удалилось
            cur.execute(
                        """
                        SELECT COUNT(*) FROM STV2023060622__DWH.global_metrics_copy WHERE date_update::DATE = '{}'
                        ;
                        """.format(self.previous_date) 
                    )
            self.log.info('LOG CUSTOM: MART DELETE PARTITION. END FOR ' + str(self.previous_date) + '. CHECK COUNT: ' + str(cur.fetchone()[0]))
            
            # Открываем файл и читаем его содержимое
            with open(os.path.abspath(self.sql_file), "r") as f:
                sql_script = f.read()

            # Загружаем данные в витрину
            self.log.info('LOG CUSTOM: MART LOAD DATA START')
            cur.execute(sql_script.replace(":date_param", "'" + self.previous_date + "'"))

            self.vertica_conn.commit()
            self.log.info('LOG CUSTOM: MART LOAD DATA END')
            self.vertica_conn.close()