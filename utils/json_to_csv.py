from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 주어진 댓글의 감정을 분석하는 함수
def analyze_sentiment(comment):
    analyzer = SentimentIntensityAnalyzer()  # 감정 분석 도구 초기화
    result = analyzer.polarity_scores(comment)  # 댓글에 대한 감정 점수 계산
    return result  # 감정 점수를 딕셔너리 형태로 반환

# JSON 파일을 CSV로 변환하는 함수
def convert_json_to_csv():
    # HDFS에서 JSON 파일이 저장된 경로와 CSV 파일을 저장할 경로 설정
    hdfs_json_path = '/input/yt-data'
    hdfs_csv_path = '/input/yt-data-csv'

    # HDFS 클라이언트 라이브러리 임포트 및 클라이언트 초기화
    from hdfs import InsecureClient
    client = InsecureClient('http://localhost:9870', user='ubuntu')

    # HDFS 경로에서 JSON 파일 목록 가져오기
    json_files = client.list(hdfs_json_path)

    import json
    for json_file in json_files:
        # 현재 JSON 파일의 전체 경로 생성
        json_file_path = f'{hdfs_json_path}/{json_file}'

        # HDFS에서 JSON 파일 읽기
        with client.read(json_file_path) as reader:
            data = json.load(reader)  # JSON 데이터를 파이썬 딕셔너리로 로드

        csv_data = []  # CSV 데이터를 저장할 리스트 초기화

        # JSON 데이터에서 모든 댓글을 순회
        for video_id, comments in data['all_comments'].items():
            for comment in comments:
                text = comment['text']  # 댓글 텍스트 추출
                sentiment = analyze_sentiment(text)  # 댓글의 감정 분석
                # 분석 결과와 댓글 정보를 CSV 데이터에 추가
                csv_data.append({
                    'video_id': video_id,
                    'positive': sentiment['pos'],  # 긍정 점수
                    'negative': sentiment['neg'],  # 부정 점수
                    'neutral': sentiment['neu'],  # 중립 점수
                    'compound': sentiment['compound'],  # 종합 점수
                    'likeCount': comment['likeCount'],  # 댓글 좋아요 수
                    'author': comment['author'],  # 댓글 작성자
                })

        # pandas를 사용해 CSV 파일 생성
        import pandas as pd
        df = pd.DataFrame(csv_data)  # CSV 데이터로 데이터프레임 생성

        # JSON 파일 이름을 기반으로 CSV 파일 이름 생성
        json_file_name = json_file.split('.')[0]
        csv_file_name = f'{json_file_name}.csv'
        csv_file_path = f'{hdfs_csv_path}/{csv_file_name}'

        # HDFS에 CSV 파일 저장
        with client.write(csv_file_path, encoding='utf-8', overwrite=True) as writer:
            df.to_csv(writer, index=False, encoding='utf-8')

# JSON -> CSV 변환 함수 실행
convert_json_to_csv()