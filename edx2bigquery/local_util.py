"""
This file contains util functions to work with local tracking log files.
"""
import json
import os

import edx2bigquery_config


def get_tracking_log_file_list(course_id):
    """
    Yields the absolute tracking log file name.

    It will check inside of the TRACKING_LOGS_DIRECTORY value.
    Then it will check inside of the folder with the provided course_id value.
    Finally it will yield the absolute path for each file in the tracking log-course id folder.

    Args:
        course_id: Course ID string.
    Returns:
        String: Absolute path of the tracking log file.
    Raises:
        Exception: When the TRACKING_LOGS_DIRECTORY setting was not configured
                   in the edx2bigquery_config configuration file.
        Exception: When the folder does not exists.
    """
    tracking_logs_folder = getattr(edx2bigquery_config, 'TRACKING_LOGS_DIRECTORY', None)

    if not tracking_logs_folder:
        raise Exception('Tracking logs folder name, was not provided in the configuration file.')

    abs_path_to_folder = os.path.abspath(
        '{}/{}'.format(tracking_logs_folder, course_id)
    )

    if not os.path.exists(abs_path_to_folder):
        print('Folder with the name: {}, does not exists.'.format(abs_path_to_folder))
        yield None
    else:
        file_list = os.listdir(abs_path_to_folder)

        for file_name in file_list:
            abs_path = '{}/{}'.format(abs_path_to_folder, file_name)
            yield abs_path


def get_schema_from_file(scheme_name):
    """
    Returns the provided schema from the edX2BigQuery schemas folder.

    Args:
        scheme_name: File name of the desired JSON scheme.
    Returns:
        Dict containing the tracking log schema.
    """
    if not scheme_name:
        raise Exception('scheme_name was not provided.')

    my_path = os.path.dirname(os.path.realpath(__file__))

    return json.loads(
        open('{}/schemas/{}.json'.format(my_path, scheme_name)).read()
    )
