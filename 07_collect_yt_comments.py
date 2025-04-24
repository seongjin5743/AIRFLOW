# Airflow 라이브러리에서 DAG 및 PythonOperator를 가져옵니다.
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# 유틸리티 함수(get_handle_to_comments, save_to_hdfs 등)를 가져옵니다.
from utils.yt_data import *

# YouTube 댓글을 수집하고 HDFS에 저장하는 작업을 정의합니다.
def my_task():
    target_handle = 'NBA'  # 수집할 YouTube 채널 핸들
    data = get_handle_to_comments(youtube, target_handle)  # 댓글 데이터를 가져옵니다.
    save_to_hdfs(data, '/input/yt-data')  # 데이터를 HDFS에 저장합니다.

# DAG(Directed Acyclic Graph)를 정의합니다.
with DAG(
    dag_id='07_collect_yt_comments',  # DAG의 고유 ID
    schedule=timedelta(minutes=10),  # 10분마다 실행
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 과거 날짜의 작업을 실행하지 않음
) as dag:
    # PythonOperator를 사용하여 my_task를 실행하는 태스크를 정의합니다.
    t1 = PythonOperator(
        task_id='collect_yt_comments',  # 태스크 ID
        python_callable=my_task,  # 실행할 Python 함수
    )