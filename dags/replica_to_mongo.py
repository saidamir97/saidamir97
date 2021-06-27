from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.providors.mysql.operators.mysql import MySqlOperator
from datetime import datetime
import urllib.parse
import numpy as np
import pandas as pd
from pymongo import MongoClient

from sqlalchemy import create_engine

default_args = {
    'start_date': datetime(2021, 1, 1)
}

db_name = 'services_calls'
db_user = 'saidamir'
db_password = '''4Kfl23kjDKjh32kjhKDkjasdhlkj@sdfjhkj%dkjsfhlkjDKJhcu4bd2'''

replica_URI = f"mysql://{db_user}:{db_password}@localhost:3306/{db_name}"
replica_engine = create_engine(replica_URI)
mongo_client = MongoClient()

def _importing_from_replica():
    table_names: Series = pd.read_sql("show tables;", replica_engine).squeeze()
    db = mongo_client[db_name]
    for tb_name in table_names:
        db.drop_collection(tb_name)
        tb_meta = pd.read_sql(f"DESCRIBE {tb_name};", replica_engine)
        date_fields = tb_meta[tb_meta.Type.isin(['date', 'datetime', 'timestamp'])]['Field'].to_list()
        query = f"SELECT * FROM {tb_name}"
        data = pd.read_sql(query, replica_engine, parse_dates=date_fields)
        data.rename(columns={'id': '_id'}, inplace=True)
        data.replace({pd.NaT: None, np.NaN: None}, inplace=True)
        try:
            if data.empty:
                db.create_collection(tb_name)
            else:
                data_dict = data.to_dict(orient='records')
                db[tb_name].insert_many(data_dict, ordered=False)
        except Exception as e:
            print(tb_name, ':   ', e)


with DAG(
    'replica_to_mongo',
    schedule_interval = '@hourly',
    default_args = default_args,
    catchup = False) as dag:

    importing_from_mysql = PythonOperator(
        task_id = "importing_from_replica",
        python_callable = _importing_from_replica
    )

    
