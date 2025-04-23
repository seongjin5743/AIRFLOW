from airflow import DAG  # Airflow DAG(워크플로우) 생성 모듈 임포트
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈 임포트
from airflow.operators.bash import BashOperator  # Bash 명령어 실행을 위한 Operator 임포트

# DAG 정의
with DAG(
    dag_id='00_intro',  # DAG의 고유 ID
    description='first DAG',  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
    schedule=timedelta(minutes=1)  # DAG 실행 주기 (1분마다 실행)
    # schedule='* * * * *',  # 크론 표현식으로 실행 주기 설정 (주석 처리됨)
) as dag:
    # 첫 번째 작업 정의
    t1 = BashOperator(
        task_id='first_task',  # 작업 ID
        bash_command='date'  # 실행할 Bash 명령어 (현재 날짜 출력)
    )

    # 두 번째 작업 정의
    t2 = BashOperator(
        task_id='second_task',  # 작업 ID
        bash_command='echo hello world'  # 실행할 Bash 명령어 (문자열 출력)
    )

    # 작업 간의 의존성 설정 (t1 실행 후 t2 실행)
    t1 >> t2