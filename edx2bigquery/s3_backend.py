"""
This File contains an implmentation of S3 storage, to get the tracking log files from there,
and then, used them to upload into Google Big Query.
"""
import gzip
import os

import boto3

import edx2bigquery_config


def get_simple_storage_service_client():
    """
    Returns the S3 client object.

    Returns:
        boto3.client object.
    """
    set_aws_environment_settings()

    return boto3.client('s3')


def download_object_and_save(object_key):
    """
    Downloads and saves the provided object name.

    Saves the file with the same object key name inside of the TRACKING_LOGS_DIRECTORY value folder.

    Args:
        object_key: Key name of the object.
    """
    if not object_key:
        return None

    bucket_name = getattr(edx2bigquery_config, 'AWS_BUCKET_NAME', '')
    local_file_name = object_key.split('/')[-1]
    logs_dir = getattr(edx2bigquery_config, 'TRACKING_LOGS_DIRECTORY', '')
    local_path_name = '{}/{}'.format(
        logs_dir,
        local_file_name,
    )

    if not os.path.exists(logs_dir):
        os.mkdir(logs_dir)

    s3_client = get_simple_storage_service_client()
    print('Downloading {} into {}'.format(object_key, local_path_name))
    s3_client.download_file(bucket_name, object_key, local_path_name)


def set_aws_environment_settings():
    """
    Sets the AWS settings from the config file into the current environment.
    """
    if not os.environ.get('AWS_ACCESS_KEY_ID'):
        os.environ['AWS_ACCESS_KEY_ID'] = getattr(edx2bigquery_config, 'AWS_ACCESS_KEY_ID', '')

    if not os.environ.get('AWS_SECRET_ACCESS_KEY'):
        os.environ['AWS_SECRET_ACCESS_KEY'] = getattr(edx2bigquery_config, 'AWS_SECRET_ACCESS_KEY', '')


def get_tracking_log_objects(bucket_name, start_date):
    """
    Finds and gets all the objects matched by the tracking log date string.

    It will search in each folder of the provided TRACKING_LOG_FILE_NAME_PREFIX value.

    Args:
        bucket_name: Name of the bucket to the get the tracking objects.
        start_date: String date to find the tracking log objects.
    Raises:
        Exception: If not bucket name was provided in the configuration file.
    """
    if not bucket_name:
        print('AWS_BUCKET_NAME must be specified in the configuration file.')
        raise Exception('Not bucket name provided')

    aws_client = get_simple_storage_service_client()

    if not getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME_PREFIX', ''):
        print('The TRACKING_LOG_FILE_NAME_PREFIX setting is required.')
        exit()

    all_instance_folder = aws_client.list_objects(
        Bucket=bucket_name,
        Delimiter='/',
        Prefix=getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME_PREFIX', ''),
    )
    folder_paginator = aws_client.get_paginator('list_objects')

    for folder in all_instance_folder.get('CommonPrefixes', []):
        prefix_file_name = '{}{}{}'.format(
            folder.get('Prefix'),
            getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME_PATTERN', ''),
            start_date,
        )
        paginator_result = folder_paginator.paginate(
            Bucket=bucket_name, Prefix=prefix_file_name, PaginationConfig={'MaxItems': 100}
        )

        for object_file in paginator_result.search('Contents'):
            if not object_file:
                continue

            download_object_and_save(object_file.get('Key', None))
