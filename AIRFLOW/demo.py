import datetime as dt
import pandas as pd
import pymongo
import random
import time


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def insertMongoDB():
    client = pymongo.MongoClient('mongodb://admin:admin@mongodb:27017')
    db = client['collection']
    collection1 = db['collection1']
    df = pd.read_csv("/opt/airflow/dags/employees.csv")

    dfObjects = df.to_dict('records')
    
    try_count = 0
    success = False
    while try_count < 3 and not success:
        data = random.choice(dfObjects)

        try:
            collection1.insert_one(data)
            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%H"), "Chèn dữ liệu thành công")
            success = True
        except pymongo.errors.DuplicateKeyError:
            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%H"), "Đã tồn tại bản ghi trong hệ thống")
            try_count += 1
            time.sleep(180)
    
    if not success:
        print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%H"), "Đã đạt tới số lần thử tối đa. Không thể chèn dữ liệu.")
    client.close()

default_args = {
    'owner': 'Van Manh',
    'start_date': dt.datetime.now() - dt.timedelta(minutes=2),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(
    'ManhDAG',
    default_args=default_args,
    tags=['LAB4'],
    schedule=dt.timedelta(minutes=1)         
) as dag:
    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "Chuẩn bị thêm dữ liệu vào hệ thống"'
    )

    insertData = PythonOperator(
        task_id='insert_data',
        python_callable=insertMongoDB
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Hoàn thành việc chèn dữ liệu"'
    )

print_starting >> insertData >> end