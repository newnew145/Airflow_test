import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

import pandas as pd
import string
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import os
import shutil
import datetime as dt

random.seed(0)
def generate_pq():
    def gen_text(number_of_item, long_of_text, text_list):
        item_set = set()
        while len(item_set) < number_of_item :
            item_name = ''.join(random.choices(text_list, k = long_of_text))
            item_set.add(item_name)
        return list(item_set)

    # folder_name = 'data_sample'
    # if  folder_name in os.listdir() :
    #     shutil.rmtree(folder_name)
    # os.mkdir(folder_name)

    list_date = pd.date_range("2023-01-01", end='2023-01-31', freq="min")
    print('list_date : ',len(list_date))
    list_char = list(string.ascii_lowercase)
    column_template = ['department_name','sensor_serial','create_at','product_name','product_expire']
    number_of_department = 100
    sensor_in_department = [random.choices(range(5,30))[0] for i in range(number_of_department)]
    number_of_sensor = sum(sensor_in_department)
    number_of_product = 1000
    department_list = gen_text(number_of_item = number_of_department, long_of_text = 32, text_list = list_char)
    sensor_list = gen_text(number_of_item = number_of_sensor, long_of_text = 64, text_list = list_char)
    product_list = gen_text(number_of_item = number_of_product, long_of_text = 16, text_list = list_char)

    data_template = pd.DataFrame(columns=['department_name','sensor_serial'])
    for i in range(number_of_department):
        d_name = department_list[i]
        n_sensor = sensor_in_department[i]
        check_list = list(data_template['sensor_serial'].unique())
        a = [s for s in sensor_list if s not in check_list]
        data_department = pd.DataFrame({
            'department_name' : [d_name] * n_sensor
            , 'sensor_serial' : random.choices(
                [s for s in sensor_list if s not in check_list]
                , k = n_sensor
                )
            })
        data_template = pd.concat([data_template,data_department], ignore_index=True)
    del data_department

    buffer_size = 2296801
    buffer = []

    file_index = 0
    # for i in tqdm(range(len(list_date))):
    for i in range(len(list_date)):
        create_at = list_date[i]
        data_template['create_at'] = create_at
        data_template['product_name'] = random.choices(product_list, k = number_of_sensor)
        data_template['product_expire'] = list(map(lambda x : x + dt.timedelta(days = 90 - random.choices([1,2,3])[0]), data_template['create_at']))

        buffer.append(data_template.copy())
        if len(buffer) * number_of_sensor >= buffer_size:
            combined_df = pd.concat(buffer, ignore_index=True)
            # path=f"data_sample\{str(create_at.strftime('%Y%m%d_%H%M%S'))}.parquet"
            path=f"/opt/airflow/logs/{str(create_at.strftime('%Y%m%d_%H%M%S'))}.parquet"
            table = pa.Table.from_pandas(combined_df)
            pq.write_table(table, path, compression='snappy')
            buffer = []
            file_index += 1
    if buffer:
        combined_df = pd.concat(buffer, ignore_index=True)
        # path=f"data_sample\{str(create_at.strftime('%Y%m%d_%H%M%S'))}.parquet"
        path=f"/opt/airflow/logs/{str(create_at.strftime('%Y%m%d_%H%M%S'))}.parquet"
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, path, compression='snappy')

default_args = {
    'owner': 'who',
    'start_date': dt.datetime(2024, 4, 13),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This is our first dag that we write',
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(
        task_id='first_task',
    )

    task2 = PythonOperator(
        task_id='second_task',
        python_callable=generate_pq
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )

    task1 >> task2 >> task3