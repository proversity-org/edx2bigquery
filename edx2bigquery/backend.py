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


def get_bucket_object_list(bucket_name, start_date):
    """
    Returns the object list from the provide bucket name.

    Args:
        bucket_name: Name of the bucket to the get the file object list.
        start_date: String date to find the tracking log files.
    """
    aws_client = get_simple_storage_service_client()

    if not getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME_PREFIX', ''):
        print('The TRACKING_LOG_FILE_NAME_PREFIX setting is required.')
        exit()

    prefix_name = '{}{}'.format(
        getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME_PREFIX', ''),
        start_date,
    )

    paginator = aws_client.get_paginator('list_objects')
    result = paginator.paginate(
        Bucket=bucket_name, Delimiter='/', Prefix=getattr(edx2bigquery_config, 'TRACKING_LOG_FILE_NAME_PREFIX', ''), PaginationConfig={'MaxItems': 1}
    )

    for page in result.search('CommonPrefixes'):
        all_objects_macthed = aws_client.list_objects(
            Bucket=bucket_name,
            Prefix=page['Prefix'],
        )

        for bucket_object in all_objects_macthed['Contents']:
            key_name = bucket_object.get('Key', None)
            if '' in key_name:
                print(key_name)

    # if not all_objects_macthed.get('Contents'):
    #     print('No objects were found with the prefix: {}'.format(prefix_name))
    #     exit()

    # for bucket_object in all_objects_macthed['Contents']:
    #     key_name = bucket_object.get('Key', None)

    #     if key_name:
    #         download_object_and_save(key_name)


get_bucket_object_list(getattr(edx2bigquery_config, 'AWS_BUCKET_NAME', ''), '2019')
