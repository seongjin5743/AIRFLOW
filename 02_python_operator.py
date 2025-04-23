from airflow import DAG  # Airflow DAG 모듈 임포트
from airflow.operators.python_operator import PythonOperator  # Python 작업을 실행하기 위한 Operator 임포트
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈 임포트

# 첫 번째 작업으로 실행할 함수 정의
def first_task():
    print('hello world')  # 'hello world' 출력

# 두 번째 작업으로 실행할 함수 정의
def second_task():
    print('bye world')  # 'bye world' 출력

# DAG 정의
with DAG(
    dag_id='02_python_operator',  # DAG의 고유 ID
    description='python test',  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
    schedule=timedelta(minutes=1),  # DAG 실행 주기 (1분마다 실행)
) as dag:
    # 첫 번째 작업 정의
    t1 = PythonOperator(
        task_id='first_task',  # 작업 ID
        python_callable=first_task  # 실행할 Python 함수
    )

    # 두 번째 작업 정의
    t2 = PythonOperator(
        task_id='second_task',  # 작업 ID
        python_callable=second_task  # 실행할 Python 함수
    )