from airflow import DAG  # Airflow DAG 모듈 임포트
from airflow.operators.python_operator import PythonOperator  # Python 작업을 실행하기 위한 Operator 임포트
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈 임포트
import os  # 파일 및 디렉토리 작업을 위한 os 모듈 임포트
import random  # 랜덤 값 생성을 위한 random 모듈 임포트
import csv  # CSV 파일 작성을 위한 csv 모듈 임포트

# 랜덤 리뷰 데이터를 생성하는 함수 정의
def generate_random_review():
    now = datetime.now()  # 현재 시간 가져오기
    file_name = now.strftime('%H%M%S') + '.csv'  # 현재 시간을 기반으로 파일 이름 생성
    BASE = os.path.expanduser('~/damf2/data/review_data')  # 리뷰 데이터를 저장할 디렉토리 경로

    file_path = f'{BASE}/{file_name}'  # 파일 전체 경로 생성

    review_data = []  # 리뷰 데이터를 저장할 리스트 초기화
    for _ in range(20):  # 20개의 리뷰 데이터 생성
        user_id = random.randint(1, 100)  # 랜덤 사용자 ID 생성
        movie_id = random.randint(1, 1000)  # 랜덤 영화 ID 생성
        rating = random.randint(1, 5)  # 랜덤 평점 생성 (1~5)
        review_data.append([user_id, movie_id, rating])  # 생성된 데이터를 리스트에 추가

    os.makedirs(BASE, exist_ok=True)  # 디렉토리가 없으면 생성

    # CSV 파일에 리뷰 데이터 저장
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)  # CSV 작성 객체 생성
        writer.writerow(['user_id', 'movie_id', 'rating'])  # 헤더 작성
        writer.writerows(review_data)  # 리뷰 데이터 작성

# DAG 정의
with DAG(
    dag_id='03_generate_review',  # DAG의 고유 ID
    description='movie review',  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
    schedule=timedelta(minutes=1),  # DAG 실행 주기 (1분마다 실행)
) as dag:
    # 리뷰 데이터를 생성하는 작업 정의
    t1 = PythonOperator(
        task_id='review_generate',  # 작업 ID
        python_callable=generate_random_review  # 실행할 Python 함수
    )