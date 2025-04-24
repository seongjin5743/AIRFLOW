from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
from pprint import pprint
from hdfs import InsecureClient
from datetime import datetime, timedelta
import json

# 환경 변수 파일(.env) 로드
load_dotenv('/home/ubuntu/airflows/.env')

# YouTube API 키를 환경 변수에서 가져옴
YOUTUBE_KEY = os.getenv('YOUTUBE_KEY')

# YouTube API 클라이언트 생성
youtube = build('youtube', 'v3', developerKey=YOUTUBE_KEY)

# handle(채널 핸들)을 기준으로 채널 ID를 가져오는 함수
def get_channel_id(youtube, handle):
    response = youtube.channels().list(
        part='id',  # 필요한 데이터 필드
        forHandle=handle,  # 채널 핸들
    ).execute()
    # pprint(response)  # 응답 데이터 출력 (디버깅용)
    return response

# 채널 ID를 기준으로 최신 동영상 ID 목록을 가져오는 함수
def get_latest_video_ids(youtube, channel_id):
    response = youtube.search().list(
        part='id',  # 필요한 데이터 필드
        channelId=channel_id,  # 채널 ID
        maxResults=5,  # 최대 5개의 결과 반환
        order='date',  # 최신순 정렬
    ).execute()
    # pprint(response)  # 응답 데이터 출력 (디버깅용)

    video_ids = []
    for item in response['items']:
        video_id = item['id'].get('videoId')  # 동영상 ID 추출
        if video_id:
            video_ids.append(video_id)  # 동영상 ID 추가
    return video_ids

# 동영상 ID를 기준으로 댓글 데이터를 가져오는 함수
def get_comments(youtube, video_id):
    response = youtube.commentThreads().list(
        part='snippet',  # 필요한 데이터 필드
        videoId=video_id,  # 동영상 ID
        maxResults=5,  # 최대 5개의 댓글 반환
        textFormat='plainText',  # 댓글 형식: 일반 텍스트
        order='relevance',  # 관련도 순 정렬
    ).execute()
    # pprint(response)  # 응답 데이터 출력 (디버깅용)

    comments = []
    for item in response['items']:
        comment = {
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],  # 작성자 이름
            'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],  # 댓글 내용
            'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],  # 좋아요 수
            'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'],  # 작성 날짜
            'commentId': item['snippet']['topLevelComment']['id'],  # 댓글 ID
        }
        comments.append(comment)  # 댓글 추가
    return comments

# YouTube 채널 핸들을 기준으로 모든 댓글 데이터를 가져오는 함수
def get_handle_to_comments(youtube, handle):
    channel_id = get_channel_id(youtube, handle)['items'][0]['id']  # 채널 ID 가져오기
    latest_video_ids = get_latest_video_ids(youtube, channel_id)  # 최신 동영상 ID 가져오기
    all_comments = {}
    for video_id in latest_video_ids:
        comments = get_comments(youtube, video_id)  # 동영상 댓글 가져오기
        all_comments[video_id] = comments  # 동영상 ID를 키로 댓글 저장
    return {'handle': handle, 'all_comments': all_comments}  # 최종 데이터 반환

# 데이터를 HDFS에 저장하는 함수
def save_to_hdfs(data, path):
    client = InsecureClient('http://localhost:9870', user='ubuntu')  # HDFS 클라이언트 생성
    current_date = datetime.now().strftime('%y%m%d%H%M')  # 현재 날짜 및 시간 포맷
    file_name = f'{current_date}.json'  # 파일 이름 생성

    hdfs_path = f'{path}/{file_name}'  # HDFS 경로 설정

    json_data = json.dumps(data, ensure_ascii=False)  # 데이터를 JSON 형식으로 변환

    # HDFS에 파일 쓰기
    with client.write(hdfs_path, encoding='utf-8') as writer:
        writer.write(json_data)  # JSON 데이터 저장