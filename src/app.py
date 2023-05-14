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
import string
import pandas as pd



EXCEPTIONS_THRESHOLD = 3
MAX_RESULTS = 50
PART = "contentDetails,id,snippet,status"
tmp_file = '/tmp/tmp.file'
tmp_video_path = '/tmp/tmp.mp4'
tmp_video_enc_path = '/tmp/tmp.enc'
SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
EXECUTION_TAG = None

LOG_ID = f'{"".join(random.choice(string.ascii_uppercase) for i in range(3))}-{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}'

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
        print_log(f'Querying for playlists. Page:{page}')
        request = youtube_client.playlists().list(part=part, maxResults=max_results,
                                                  mine=True) if next_page_token is None else youtube_client.playlists().list(
            part=part, maxResults=max_results, mine=True, pageToken=next_page_token)
        response = request.execute()
        if 'items' in response:
            print_log(f'Found {len(response["items"])} playlists.')
            playlists.extend(response['items'])

        page += 1

        if 'nextPageToken' in response and response['nextPageToken'] is not None:
            next_page_token = response['nextPageToken']
            print_log(f'nextPageToken found.')
        else:
            break
        time.sleep(1)
    return playlists

def print_log(message):
    """
    Prepends a log message with a log instance ID. AWS suggests use of the simple print statement for logging in Lambda
            Parameters:
                    message (str): log message
    """
    print(f'{LOG_ID} {message}')


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
    print_log(f'Finding videos in {len(playlists)} playlists.')
    for playlist in playlists:
        next_page_token = None
        page = 1
        playlist_count += 1
        playlist_id = playlist['id']
        while True:
            print_log(f'Querying for videos. Playlist {playlist_id}: {playlist_count} of {len(playlists)} Page:{page}')
            request = youtube_client.playlistItems().list(part=part, maxResults=max_results,
                                                          playlistId=playlist_id) if next_page_token is None \
                else youtube_client.playlistItems().list(part=part, maxResults=max_results, playlistId=playlist_id,
                                                         pageToken=next_page_token)
            response = request.execute()
            if 'items' in response:
                print_log(f'Found {len(response["items"])} videos.')
                videos.extend(response['items'])

            page += 1

            if 'nextPageToken' in response:
                print_log(f'nextPageToken found.')
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
        print_log('Creds are expired. Refreshing...')
        creds.refresh(Request())
    else:
        print_log('Entering flow...')
        flow = InstalledAppFlow.from_client_secrets_file(
            tmp_file, SCOPES)
        print_log('Authenticating...')
        creds = flow.run_local_server(port=0)
    print_log(f'Dumping creds to {tmp_file}')
    with open(tmp_file, 'wb') as token:
        pickle.dump(creds, token)
    print_log(f'Re-uploading creds {tmp_file} to s3://{bucket}/{creds_key}')
    s3_client.upload_file(bucket=bucket, key=creds_key, local_path=tmp_file)
    print_log('Returning refreshed credentials.')
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

    print_log(f'Downloading creds file s3://{bucket}/{creds_key} to {tmp_file}')
    s3_client.download_file(bucket=bucket, key=creds_key, local_path=tmp_file)

    print_log(f'Loading creds pickle file at {tmp_file}')
    with open(tmp_file, 'rb') as token:
        creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.

    if not creds.valid:
        print_log('Refreshing credentials')
        creds = refresh_credentials(s3_client=s3_client, bucket=bucket, creds_key=creds_key, creds=creds)

    print_log('Building service')
    youtube_client = build(api_service_name, api_version, credentials=creds)
    print_log('Authenticated')
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


def process_video(s3_client, bucket, folder_name, video, video_id, video_json_key, fernet_str):
    """
    Downloads a video, encrypts it, and uploads it to S3
            Parameters:
                    s3_client (object): Helper resource for interacting with AWS S3
                    bucket (str): Name of the project bucket
                    folder_name(str): Project folder
                    video(dict): dict representation of JSON record from YouTube for the given video
                    video_id(str): Unique identifier for the video on YouTube
                    video_json_key(str): The location where we will upload the video.
                    fernet_str(str): Encryption key used to encrypt the video

    """
    link = f'https://www.youtube.com/watch?v={video_id}'
    print_log(f'Processing {link}')
    try:
        yt = YouTube(link)
        ys = yt.streams.get_highest_resolution()

        print_log(f'Downloading\t{link}\t{ys.default_filename}')
        ys.download(output_path='/tmp', filename='tmp.mp4')

        encrypt_video(fernet_str=fernet_str)

        out_key = f'{folder_name}/videos/{video_id}.enc'
        print_log(f'Uploading to {out_key}')
        s3_client.upload_file(bucket=bucket, key=out_key, local_path=tmp_video_enc_path)
        print_log('Writing json')
        with open(tmp_file, 'w') as outfile:
            json.dump(video, outfile)
        print_log('Uploading json')
        s3_client.upload_file(bucket=bucket, key=video_json_key, local_path=tmp_file)

    except Exception as e:
        print_log(f'Uploading exception {e}')
        video['yta-exception'] = str(e)
        exceptions_json_key = f'{folder_name}/exceptions/{video_id}/{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.json'
        with open(tmp_file, 'w') as outfile:
            json.dump(video, outfile)
        s3_client.upload_file(bucket=bucket, key=exceptions_json_key, local_path=tmp_file)

def create_video_digest(s3_client,bucket,videos,folder_name):
    """
    Creates a digest table so that the latest status of videos
            Parameters:
                    s3_client (object): Helper resource for interacting with AWS S3
                    bucket (str): Name of the project bucket
                    videos(list): List of dict representation of JSON records for YouTube videos in playlists
                    folder_name(str): Project folder
                    exceptions_counter(counter): Counter for video IDs which failed processing

    """


    completed_video_object_list = s3_client.list_objects(bucket=bucket, prefix=f'{folder_name}/json/')
    exceptions_object_list = s3_client.list_objects(bucket=bucket, prefix=f'{folder_name}/exceptions/')


    digest = []
    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        _json_key = f'{folder_name}/json/{video_id}.json'
        _exception_key_prefix = f'{folder_name}/exceptions/{video_id}/'
        _link = f'https://www.youtube.com/watch?v={video_id}'

        video_title = None
        is_captured = False
        size = None
        last_modified = None
        for obj in completed_video_object_list:
            key = obj['Key']
            if key == _json_key:
                is_captured = True
                size = obj['Size']
                last_modified = obj['LastModified']

                s3_client.download_file(bucket=bucket, key=key, local_path=tmp_file)
                with open(tmp_file) as json_file:
                    data = json.load(json_file)
                    video_title = data['snippet']['title']
                break

        exception_list = []
        for obj in exceptions_object_list:
            key = obj['Key']
            if _exception_key_prefix in key  and key.endswith('.json'):
                exception_date = obj['LastModified']
                s3_client.download_file(bucket=bucket, key=key, local_path=tmp_file)
                with open(tmp_file) as json_file:
                    data = json.load(json_file)
                    video_title = video_title if video_title is not None else data['snippet']['title']
                    exception_msg = data['yta-exception']
                exception_list.append(f'{exception_date}--{exception_msg}')


        exceptions = ':::'.join(exception_list)



        digest_record = {'video_id':video['snippet']['resourceId']['videoId'],
                         'Title':video_title,
                         'Captured': is_captured,
                         'Size':size,
                         'LastModified':last_modified,
                         'Exceptions Count':len(exception_list),
                         'Exceptions':exceptions,
                         'Link':_link}
        digest.append(digest_record)


    df = pd.DataFrame(digest)
    df.to_csv(tmp_file,encoding='utf-8')
    s3_client.upload_file(bucket=bucket,key=f'{folder_name}/digest.csv',local_path=tmp_file)


def main():
    print_log('Starting...')
    s3_client = aws.S3Helper()
    secrets = aws.get_secrets()

    bucket = secrets[os.getenv('KEY_BUCKET_NAME')]
    folder_name = secrets[os.getenv('KEY_PROJECT_FOLDER_NAME')]
    creds_key = secrets[os.getenv('KEY_YTA_CREDS_PATH')]
    fernet_str = secrets[os.getenv('KEY_FERNET')]
    print_log(
        f'Retrieved secrets parameters: bucket:{bucket} folder:{folder_name} creds(len):{len(creds_key)} fernet(len):{len(fernet_str)}')

    youtube_client = authenticate(creds_key=creds_key, bucket=bucket, s3_client=s3_client)
    playlists = list_playlists(youtube_client=youtube_client, part=PART, max_results=MAX_RESULTS)
    print_log(f'Retrieved playlist count: {len(playlists)}')

    videos = list_playlist_videos(youtube_client=youtube_client, part=PART, max_results=MAX_RESULTS,
                                  playlists=playlists)
    print_log(f'Retrieved playlist video count:{len(videos)}')
    random.shuffle(videos)

    # We use this list to check videos which have already been captured.
    # Even if the video file is moved off of S3, we should maintain the JSON for the video as a record
    # that it has been captured already.
    completed_yt_uploads_s3_keys = [x['Key'] for x in
                                    s3_client.list_objects(bucket=bucket, prefix=f'{folder_name}/json/')]
    print_log(f'Found {len(completed_yt_uploads_s3_keys)} completed video JSON records')

    # Some videos will fail occasionally. Others will fail systemically.
    # The exceptions list can be used to tally the number of fails that occurred for a given video.
    # If an attempt threshold has been exceeded, we will no longer continue to try to capture this video.
    exceptions_s3_keys = [x['Key'] for x in
                                    s3_client.list_objects(bucket=bucket, prefix=f'{folder_name}/exceptions/')]
    print_log('Creating exceptions counter...')

    # Exceptions are recorded in S3 at project-folder/exceptions/{video_id}/{timestamp}.json
    # The [2] index represents the video ID. This comprehension returns the video ID associated with each individual exception
    exception_instances = [exception_key.split('/')[2] for exception_key in exceptions_s3_keys]
    exceptions_counter = Counter(exception_instances)

    print_log('Creating Digest...')
    create_video_digest(s3_client=s3_client,bucket=bucket,videos=videos,folder_name=folder_name)

    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        video_json_key = f'{folder_name}/json/{video_id}.json'
        if video_json_key not in completed_yt_uploads_s3_keys and exceptions_counter[video_id] < EXCEPTIONS_THRESHOLD:

            process_video(s3_client=s3_client, bucket=bucket, folder_name=folder_name,video=video, video_id=video_id,
                          video_json_key=video_json_key, fernet_str=fernet_str)

            # Because this operates from a lambda, we are limited to a 15-minute run-time.
            # For the vast majority of videos, this is sufficient.
            # To save on compute times, we should end after one try instead of making attempts to
            # download more videos and be prematurely cut when the instance ends.
            break

    return None


def handler(event, context):
    result = main()

    return {
        'headers': {'Content-Type': 'application/json'},
        'statusCode': 200,
        'body': json.dumps({"message": f"result:{result}",
                            "event": event})
    }
