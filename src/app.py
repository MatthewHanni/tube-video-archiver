import datetime
import json
import os
import csv
from cryptography.fernet import Fernet
from pytube import YouTube
from collections import Counter
import aws_functions as aws
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import time
import random
import logging

MAX_RESULTS = 50
PART = "contentDetails,id,snippet,status"
tmp_file = '/tmp/tmp.file'
tmp_video_path = '/tmp/tmp.mp4'
tmp_video_enc_path = '/tmp/tmp.enc'
SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
EXECUTION_TAG = None



def list_playlists(youtube_client, part, max_results):
    """
    Returns a list of playlist JSON objects for the authenticated account's channel (mine=True)
            Parameters:
                    youtube_client (service): service resource for interacting with the YouTube API
                    part(str): comma-separated list of resource properties
                    max_results(int): Number of results to return for a single API call.
            Returns:
                    playlists (list): List of playlist JSON objects for the authenticated account's channel
    """
    playlists = []
    next_page_token = None
    page = 1
    while True:
        print(f'Querying for playlists. Page:{page}')
        request = youtube_client.playlists().list(part=part, maxResults=max_results,
                                                  mine=True) if next_page_token is None else youtube_client.playlists().list(
            part=part, maxResults=max_results, mine=True, pageToken=next_page_token)
        response = request.execute()
        if 'items' in response:
            print(f'Found {len(response["items"])} playlists.')
            playlists.extend(response['items'])

        page += 1

        if 'nextPageToken' in response and response['nextPageToken'] is not None:
            next_page_token = response['nextPageToken']
            print(f'nextPageToken found.')
        else:
            break
        time.sleep(1)
    return playlists


def list_playlist_videos(youtube_client, playlists, part, max_results):
    """
    Returns a list of video JSON objects for the authenticated account's channel playlists
            Parameters:
                    youtube_client (service): service resource for interacting with the YouTube API
                    playlists (list): list of playlist JSON objects
                    part(str): comma-separated list of resource properties
                    max_results(int): Number of results to return for a single API call.
            Returns:
                    videos (list): List of video JSON objects for all playlists in the authenticated account's channel
    """

    videos = []


    playlist_count = 0
    print(f'Finding videos in {len(playlists)} playlists.')
    for playlist in playlists:
        next_page_token = None
        page = 1
        playlist_count += 1
        playlist_id = playlist['id']
        while True:
            print(f'Querying for videos. Playlist {playlist_id}: {playlist_count} of {len(playlists)} Page:{page}')
            request = youtube_client.playlistItems().list(part=part, maxResults=max_results,
                                                          playlistId=playlist_id) if next_page_token is None \
                else youtube_client.playlistItems().list(part=part, maxResults=max_results, playlistId=playlist_id,
                                                         pageToken=next_page_token)
            response = request.execute()
            if 'items' in response:
                print(f'Found {len(response["items"])} videos.')
                videos.extend(response['items'])

            page += 1

            if 'nextPageToken' in response:
                print(f'nextPageToken found.')
                next_page_token = response['nextPageToken']
            else:
                break
            time.sleep(1)
    return videos


def refresh_credentials(s3_client, bucket, creds_key, creds):
    """
    Returns refreshed Google service credentials, and uploads those credentials to S3 for future use.
            Parameters:
                    s3_client (object): Helper resource for interacting with AWS S3
                    bucket (str): Name of the project bucket
                    creds_key(str): S3 key within the project bucket where the Google credentials pickle is stored
                    creds (object): Google API credentials object

            Returns:
                    creds (object): Google API credentials object, updated with refreshed credentials
    """
    if creds and creds.expired and creds.refresh_token:
        print('Creds are expired. Refreshing...')
        creds.refresh(Request())
    else:
        print('Entering flow...')
        flow = InstalledAppFlow.from_client_secrets_file(
            tmp_file, SCOPES)
        print('Authenticating...')
        creds = flow.run_local_server(port=0)
    print(f'Dumping creds to {tmp_file}')
    with open(tmp_file, 'wb') as token:
        pickle.dump(creds, token)
    print(f'Re-uploading creds {tmp_file} to s3://{bucket}/{creds_key}')
    s3_client.upload_file(bucket=bucket, key=creds_key, local_path=tmp_file)
    print('Returning refreshed credentials.')
    return creds


def authenticate(s3_client, bucket, creds_key):
    """
    Returns an authenticated service resource for interacting with the YouTube API.
            Parameters:
                    s3_client (object): Helper resource for interacting with AWS S3
                    bucket (str): Name of the project bucket
                    creds_key(str): S3 key within the project bucket where the Google credentials pickle is stored

            Returns:
                    youtube_client (service): authenticated service resource for interacting with the YouTube API
    """

    api_service_name = "youtube"
    api_version = "v3"
    creds = None

    print(f'Downloading creds file s3://{bucket}/{creds_key} to {tmp_file}')
    s3_client.download_file(bucket=bucket, key=creds_key, local_path=tmp_file)

    print(f'Loading creds pickle file at {tmp_file}')
    with open(tmp_file, 'rb') as token:
        creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.

    if not creds.valid:
        print('Refreshing credentials')
        creds = refresh_credentials(s3_client=s3_client, bucket=bucket, creds_key=creds_key, creds=creds)

    print('Building service')
    youtube_client = build(api_service_name, api_version, credentials=creds)
    print('Authenticated')
    return youtube_client


def encrypt_video(fernet_str):
    """
    Creates an encrypted version of the local, temporary video file
            Parameters:
                    fernet_str (object): The string representation of our Fernet encryption key

    """
    fernet_encryption_key = fernet_str.encode()
    fernet = Fernet(fernet_encryption_key)
    with open(tmp_video_path, 'rb') as file:
        original = file.read()
    encrypted = fernet.encrypt(original)
    with open(tmp_video_enc_path, 'wb') as encrypted_file:
        encrypted_file.write(encrypted)


def process_video(s3_client, bucket_name, folder_name,video, video_id, video_json_key, fernet_str):

    link = f'https://www.youtube.com/watch?v={video_id}'
    print(f'processing {link}')
    try:
        yt = YouTube(link)
        ys = yt.streams.get_highest_resolution()

        print(f'downloading\t{link}\t{ys.default_filename}')
        ys.download(output_path='/tmp', filename='tmp.mp4')

        encrypt_video(fernet_str=fernet_str)

        out_key = f'{folder_name}/videos/{video_id}.enc'
        print(f'Uploading to {out_key}')
        s3_client.upload_file(bucket=bucket_name, key=out_key, local_path=tmp_video_enc_path)
        print('Writing json')
        with open(tmp_file, 'w') as outfile:
            json.dump(video, outfile)
        print('Uploading json')
        s3_client.upload_file(bucket=bucket_name, key=video_json_key, local_path=tmp_file)

    except Exception as e:
        print(f'Uploading exception {e}')
        video['yta-exception'] = str(e)
        exceptions_json_key = f'{folder_name}/exceptions/{video_id}/{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        with open(tmp_file, 'w') as outfile:
            json.dump(video, outfile)
        s3_client.upload_file(bucket=bucket_name, key=exceptions_json_key, local_path=tmp_file)

def create_captured_video_digest(videos,folder_name,completed_yt_uploads_s3_keys):
    counter = Counter(exception_instances)
    digest = []
    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        _json_key = f'{folder_name}/json/{video_id}.json'
        _link = f'https://www.youtube.com/watch?v={video_id}'

        digest_record = {'video_id':video['snippet']['resourceId']['videoId'],
                         'Captured': _json_key in completed_yt_uploads_s3_keys,
                         'Exceptions':counter[video_id],
                         'Link':_link}
        digest.append(digest_record)


    fieldnames = ["video_id","Captured", "Exceptions","Link"]
    with open(tmp_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(digest)
    s3_client.upload_file(bucket=bucket_name,key=f'{folder_name}/digest.csv',local_path=tmp_file)


def main():
    print('Starting...')
    s3_client = aws.S3Helper()
    secrets = aws.get_secrets()

    bucket_name = secrets[os.getenv('KEY_BUCKET_NAME')]
    folder_name = secrets[os.getenv('KEY_PROJECT_FOLDER_NAME')]
    creds_key = secrets[os.getenv('KEY_YTA_CREDS_PATH')]
    fernet_str = secrets[os.getenv('KEY_FERNET')]
    print(
        f'Retrieved secrets parameters: bucket:{bucket_name} folder:{folder_name} creds(len):{len(creds_key)} fernet(len):{len(fernet_str)}')


    youtube_client = authenticate(creds_key=creds_key, bucket=bucket_name, s3_client=s3_client)
    playlists = list_playlists(youtube_client=youtube_client, part=PART, max_results=MAX_RESULTS)
    print(f'Retrieved playlist count: {len(playlists)}')

    videos = list_playlist_videos(youtube_client=youtube_client, part=PART, max_results=MAX_RESULTS,
                                  playlists=playlists)
    print(f'Retrieved playlist video count:{len(videos)}')
    random.shuffle(videos)

    # We use this list to check videos which have already been captured.
    # Even if the video file is moved off of S3, we should maintain the JSON for the video as a record
    # that it has been captured already.
    completed_yt_uploads_s3_keys = [x['Key'] for x in
                                    s3_client.list_objects(bucket=bucket_name, prefix=f'{folder_name}/json/')]
    print(f'Found {len(completed_yt_uploads_s3_keys)} jsons')

    # Some videos will fail occasionally. Others will fail systemically.
    # The exceptions list can be used to tally the number of fails that occurred for a given video.
    # If an attempt threshold has been exceeded, we will no longer continue to try to capture this video.
    exceptions_s3_keys = [x['Key'] for x in
                                    s3_client.list_objects(bucket=bucket_name, prefix=f'{folder_name}/exceptions/')]
    print('Creating exceptions counter...')
    exception_instances = [exception_key.split('/')[2] for exception_key in exceptions_s3_keys]

    print('Creating Digest...')
    create_captured_video_digest()

    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        video_json_key = f'{folder_name}/json/{video_id}.json'
        if video_json_key not in completed_yt_uploads_s3_keys and counter[video_id] < 3:

            print('Fernet encrypted key encoded.')
            process_video(s3_client=s3_client, bucket_name=bucket_name, folder_name=folder_name,video=video, video_id=video_id,
                          video_json_key=video_json_key, fernet_str=fernet_str)
            break

    return None


def handler(event, context):
    EXECUTION_TAG
    result = main()

    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps({"message": f"result:{result}",
                            "event": event})
    }
