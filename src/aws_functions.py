import boto3
import os
import json
from botocore.exceptions import ClientError


def get_secrets():
    secret_name = os.getenv('SECRET_NAME')
    region_name = os.getenv('REGION_NAME')

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secrets = json.loads(get_secret_value_response['SecretString'])
    return secrets


class S3Helper:
    def __init__(self):
            self.s3_client = boto3.client('s3')


    def list_objects(self,bucket,prefix=None):
        _key_list = []
        continuation_token = None
        while True:
            if prefix is None and continuation_token is None:
                response = self.s3_client.list_objects_v2( Bucket=bucket)
            elif prefix is None and continuation_token is not None:
                response = self.s3_client.list_objects_v2( Bucket=bucket, ContinuationToken=continuation_token)
            elif prefix is not None and continuation_token is None:
                response = self.s3_client.list_objects_v2( Bucket=bucket, Prefix=prefix)
            elif prefix is not None and continuation_token is not None:
                response = self.s3_client.list_objects_v2( Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
            else:
                return None
            if 'Contents' in response:
                _key_list.extend(response['Contents'])

            if 'NextContinuationToken' in response:
                continuation_token = response['NextContinuationToken']
            else:
                return _key_list

    def download_file(self,bucket,key,local_path):
        with open(local_path, 'wb') as f:
            self.s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=f)

    def upload_file(self,bucket,key,local_path):
        with open(local_path, 'rb') as f:
            self.s3_client.upload_fileobj(f,Bucket=bucket, Key=key)