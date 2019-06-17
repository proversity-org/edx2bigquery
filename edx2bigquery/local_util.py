"""
This file contains util functions to work with local tracking log files.
"""
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
        raise Exception('Folder with the name: {}, does not exists.'.format(abs_path_to_folder))

    file_list = os.listdir(abs_path_to_folder)

    for file in file_list:
        abs_path = '{}/{}'.format(abs_path_to_folder, file)
        yield abs_path
