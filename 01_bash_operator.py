from airflow import DAG  # Airflow DAG 모듈 임포트
from airflow.operators.bash import BashOperator  # Bash 명령어 실행을 위한 Operator 임포트
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈 임포트

# DAG 정의
with DAG(
    dag_id='01_bash_operator',  # DAG의 고유 ID
    description='bash',  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
    schedule=timedelta(minutes=1),  # DAG 실행 주기 (1분마다 실행)
) as dag:
    # 첫 번째 작업 정의
    t1 = BashOperator(
        task_id='first_task',  # 작업 ID
        bash_command='date'  # 실행할 Bash 명령어 (현재 날짜 출력)
    )

    # 반복 작업을 위한 Bash 명령어 정의
    my_command = '''
        {% for i in range(5) %}  # Jinja 템플릿을 사용하여 5번 반복
            echo {{ ds }}  # 실행 날짜 출력
            echo {{ i }}  # 반복 변수 출력
        {% endfor %}
    '''

    # 두 번째 작업 정의
    t2 = BashOperator(
        task_id='for',  # 작업 ID
        bash_command=my_command  # 반복 작업을 수행할 Bash 명령어
    )