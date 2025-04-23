from airflow import DAG  # Airflow DAG 모듈 임포트
from airflow.operators.python_operator import PythonOperator  # Python 작업을 실행하기 위한 Operator 임포트
from datetime import timedelta, datetime  # 날짜 및 시간 관련 모듈 임포트
import boto3  # AWS S3 작업을 위한 boto3 라이브러리 임포트
from dotenv import load_dotenv  # 환경 변수 로드를 위한 dotenv 라이브러리 임포트
import os  # 파일 및 디렉토리 작업을 위한 os 모듈 임포트

# .env 파일에서 환경 변수 로드
load_dotenv('/home/ubuntu/airflow/.env')

# S3에 파일을 업로드하는 함수 정의
def upload_to_s3():
    # S3 클라이언트 생성
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_KEY'),  # AWS 액세스 키
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),  # AWS 시크릿 키
        region_name='ap-northeast-2'  # S3 리전 설정
    )

    local_dir = os.path.expanduser('~/damf2/data/bitcoin')  # 로컬 디렉토리 경로
    bucket_name = 'damf2-hsj'  # S3 버킷 이름
    s3_prefix = 'bitcoin-hsj/'  # S3에 저장될 경로(prefix)

    files = []  # 업로드할 파일 목록 초기화
    for file in os.listdir(local_dir):  # 로컬 디렉토리에서 파일 목록 가져오기
        files.append(file)

    for file in files:  # 파일을 S3로 업로드
        local_file_path = os.path.join(local_dir, file)  # 로컬 파일 경로
        s3_path = f'{s3_prefix}{file}'  # S3 파일 경로

        # S3에 파일 업로드
        s3.upload_file(local_file_path, bucket_name, s3_path)

        os.remove(local_file_path)  # 업로드 후 로컬 파일 삭제

# DAG 정의
with DAG(
    dag_id='06_upload_to_s3',  # DAG의 고유 ID
    description='s3',  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
    schedule=timedelta(minutes=5)  # DAG 실행 주기 (5분마다 실행)
) as dag:
    # S3 업로드 작업 정의
    t1 = PythonOperator(
        task_id='upload_to_s3',  # 작업 ID
        python_callable=upload_to_s3  # 실행할 Python 함수
    )

    t1  # DAG에 작업 추가