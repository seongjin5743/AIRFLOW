from airflow import DAG  # Airflow DAG 모듈 임포트
from airflow.operators.python_operator import PythonOperator  # Python 작업을 실행하기 위한 Operator 임포트
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈 임포트
import os  # 파일 및 디렉토리 작업을 위한 os 모듈 임포트
import requests  # HTTP 요청을 위한 requests 라이브러리 임포트
import csv  # CSV 파일 작성을 위한 csv 모듈 임포트
import time  # 시간 관련 작업을 위한 time 모듈 임포트

# 업비트에서 비트코인 데이터를 수집하는 함수 정의
def collect_upbit_data():
    upbit_url = 'https://api.upbit.com/v1/ticker'  # 업비트 API URL
    params = {'markets': 'KRW-BTC'}  # 요청 파라미터 (비트코인 마켓)

    collected_data = []  # 수집된 데이터를 저장할 리스트 초기화

    start_time = time.time()  # 데이터 수집 시작 시간 기록
    while time.time() - start_time < 60:  # 60초 동안 데이터 수집
        res = requests.get(upbit_url, params=params)  # 업비트 API 요청
        data = res.json()[0]  # 응답 데이터를 JSON으로 변환 후 첫 번째 항목 가져오기

        # 필요한 데이터 추출 및 리스트에 추가
        csv_data = [data['market'], data['trade_date'], data['trade_time'], data['trade_price']]
        collected_data.append(csv_data)

        time.sleep(5)  # 5초 대기 후 다음 요청

    # 파일 저장
    now = datetime.now()  # 현재 시간 가져오기
    file_name = now.strftime('%H%M%S') + '.csv'  # 현재 시간을 기반으로 파일 이름 생성
    BASE = os.path.expanduser('~/damf2/data/bitcoin')  # 데이터를 저장할 디렉토리 경로
    file_path = f'{BASE}/{file_name}'  # 파일 전체 경로 생성

    os.makedirs(BASE, exist_ok=True)  # 디렉토리가 없으면 생성

    # 수집된 데이터를 CSV 파일로 저장
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)  # CSV 작성 객체 생성
        writer.writerows(collected_data)  # 수집된 데이터 작성

# DAG 정의
with DAG(
    dag_id='05_bitcoin',  # DAG의 고유 ID
    description='Bitcoin',  # DAG에 대한 설명
    start_date=datetime(2025, 1, 1),  # DAG 시작 날짜
    schedule=timedelta(minutes=1),  # DAG 실행 주기 (1분마다 실행)
    catchup=False,  # 이전 실행 날짜의 작업을 실행하지 않음
) as dag:
    # 비트코인 데이터를 수집하는 작업 정의
    t1 = PythonOperator(
        task_id='collect_bitcoin',  # 작업 ID
        python_callable=collect_upbit_data,  # 실행할 Python 함수
    )
    t1  # DAG에 작업 추가