from airflow import DAG  # Airflow DAG 모듈 임포트
from airflow.operators.python_operator import PythonOperator  # Python 작업을 실행하기 위한 Operator 임포트
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈 임포트
import os, subprocess  # 파일 작업 및 서브프로세스 실행을 위한 모듈 임포트

# HDFS에 파일을 업로드하는 함수 정의
def upload_to_hdfs():
    local_dir = os.path.expanduser('~/damf2/data/review_data')  # 로컬 디렉토리 경로
    hdfs_dir = '/input/review_data'  # HDFS 디렉토리 경로

    # HDFS 디렉토리 생성 (존재하지 않을 경우)
    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir], check=True)

    files = []  # 업로드할 파일 목록 초기화

    # 로컬 디렉토리에서 파일 목록 가져오기
    for file in os.listdir(local_dir):
        files.append(file)

    # 파일을 HDFS로 업로드
    for file in files:
        local_file_path = os.path.join(local_dir, file)  # 로컬 파일 경로
        hdfs_file_path = f'{hdfs_dir}/{file}'  # HDFS 파일 경로

        # HDFS에 파일 업로드 명령 실행
        subprocess.run(['hdfs', 'dfs', '-put', local_file_path, hdfs_file_path], check=True)
        os.remove(local_file_path)  # 업로드 후 로컬 파일 삭제

# DAG 정의
with DAG(
    dag_id="04_upload_to_hdfs",  # DAG의 고유 ID
    description="Upload files to HDFS",  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
    schedule=timedelta(minutes=1),  # DAG 실행 주기 (1분마다 실행)
) as dag:
    # HDFS 업로드 작업 정의
    t1 = PythonOperator(
        task_id="upload_to_hdfs",  # 작업 ID
        python_callable=upload_to_hdfs,  # 실행할 Python 함수
    )