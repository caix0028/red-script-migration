import logging

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

db_conn = None


def get_db_conn():
    global db_conn
    if db_conn is None:
        # db_engine = create_engine(
        #     f"postgresql+psycopg2://postgres:zsgX6j0YYgg7m9d@database-1.cfexsvaj9glb.ap-southeast-1.rds.amazonaws.com:5432/red",
        # )
        db_engine = create_engine(
            f"mssql+pymssql://iotdatabrick:APAC_IoT_MonitoringDB%400309@sqls-sa-p-iotsolardg-001.database.windows.net/sqldb-p-iotsolardg"
        )
        db_conn = db_engine.connect()
        logging.info("Database connection opened.")
    return db_conn


def close_db_conn():
    global db_conn
    if db_conn is not None:
        db_conn.close()
        db_conn = None
        logging.info("Database connection closed.")


def f_get_table(query):
    return pd.read_sql_query(query, get_db_conn())


def f_append_to_table(df: pd.DataFrame, table_name):
    affected_rows = df.to_sql(
        table_name, con=get_db_conn(), if_exists="append", index=False
    )
    if affected_rows == 0 or affected_rows is None:
        logging.warn(f"No data appended to {table_name}")
    else:
        logging.info(f"FusionSolar {affected_rows} data rows appended to {table_name}")
