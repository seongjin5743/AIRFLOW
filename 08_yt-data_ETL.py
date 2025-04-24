# Airflow 라이브러리에서 DAG 및 PythonOperator를 가져옵니다.
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# JSON 데이터를 CSV로 변환하는 유틸리티 함수를 가져옵니다.
from utils.json_to_csv import *

# DAG(Directed Acyclic Graph)를 정의합니다.
with DAG(
    dag_id='08_yt_data_ETL',  # DAG의 고유 ID
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule=timedelta(minutes=10),  # 10분마다 실행
    catchup=False,  # 과거 날짜의 작업을 실행하지 않음
) as dag:
    # PythonOperator를 사용하여 JSON 데이터를 CSV로 변환하는 태스크를 정의합니다.
    t1 = PythonOperator(
        task_id='convert',  # 태스크 ID
        python_callable=convert_json_to_csv  # 실행할 Python 함수
    )
    t1  # DAG에 태스크 추가