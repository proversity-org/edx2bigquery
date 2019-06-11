"""
This File contains an implmentation of S3 storage, to get the logs file from there,
and then, used them to upload into Google Big Query.
"""
import gzip
import os

import boto3

import edx2bigquery_config


def get_simple_storage_service_bucket():
    """
    Returns the S3 bucket object.
    """
    set_aws_environment_settings()
    bucket_name = getattr(edx2bigquery_config, 'AWS_BUCKET_NAME', '')
    file_name = getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME', '')
    local_file_name = file_name.split('/')[-1]
    local_path_name = '{}/{}'.format(
        getattr(edx2bigquery_config, 'TRACKING_LOGS_DIRECTORY', ''),
        local_file_name,
    )

    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, file_name, local_path_name)

    with gzip.open(local_path_name, 'rb') as f:
        file_content = f.read()
    import ipdb; ipdb.set_trace()
    some = 0


def set_aws_environment_settings():
    """
    Sets the AWS settings from the config file into the current environment.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = getattr(edx2bigquery_config, 'AWS_ACCESS_KEY_ID', '')
    os.environ['AWS_SECRET_ACCESS_KEY'] = getattr(edx2bigquery_config, 'AWS_SECRET_ACCESS_KEY', '')

get_simple_storage_service_bucket()
