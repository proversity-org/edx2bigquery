#!/usr/bin/python
#
# File:   load_daily_tracking_logs.py
# Date:   15-Oct-14
# Author: I. Chuang <ichuang@mit.edu>
#
# load daily tracking logs into BigQuery via Google Cloud Storage
#
# Needs gsutil to be setup and working.
# Uses bqutil to do the interface to BigQuery.
#
# Usage: 
#
#    python load_daily_tracking_logs.py COURSE_ID
#
# Assumes daily tracking log files are already uploaded to google storage.

import datetime
import glob
import json
import os
import re
import sys
import time

import dateutil.parser

import auth
import bqutil
import edx2bigquery_config
import gsutil
import local_util

#-----------------------------------------------------------------------------

def load_all_daily_logs_for_course(course_id, gsbucket="gs://x-data", verbose=True, wait=False,
                                   check_dates=True):
    '''
    Load daily tracking logs for course from google storage into BigQuery.
    
    If wait=True then waits for loading jobs to be completed.  It's desirable to wait
    if subsequent jobs which need these tables (like person_day) are to be run
    immediately afterwards.
    '''

    print "Loading daily tracking logs for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()
    gsroot = gsutil.path_from_course_id(course_id)

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA = json.loads(open('%s/schemas/schema_tracking_log.json' % mypath).read())['tracking_log']

    gsdir = '%s/%s/DAILY/' % (gsbucket, gsroot)

    fnset = gsutil.get_gs_file_list(gsdir)
  
    dataset = bqutil.course_id2dataset(gsroot, dtype="logs")
  
    # create this dataset if necessary
    bqutil.create_dataset_if_nonexistent(dataset)

    tables = bqutil.get_list_of_table_ids(dataset)
    tables = [x for x in tables if x.startswith('track')]
  
    if verbose:
        print "-"*77
        print "current tables loaded:", json.dumps(tables, indent=4)
        print "files to load: ", json.dumps(fnset.keys(), indent=4)
        print "-"*77
        sys.stdout.flush()
  
    for fn, fninfo in fnset.iteritems():

        if int(fninfo['size'])<=45:
            print "Zero size file %s, skipping" % fn
            continue

        m = re.search('(\d\d\d\d-\d\d-\d\d)', fn)
        if not m:
            continue
        date = m.group(1)
        tablename = "tracklog_%s" % date.replace('-','')	# YYYYMMDD for compatibility with table wildcards

        # file_date = gsutil.get_local_file_mtime_in_utc(fn, make_tz_unaware=True)
        file_date = fninfo['date'].replace(tzinfo=None)
  
        if tablename in tables:
            skip = True
            if check_dates:
                table_date = bqutil.get_bq_table_last_modified_datetime(dataset, tablename)
                if not (table_date > file_date):
                    print "Already have table %s, but %s file_date=%s, table_date=%s; re-loading from gs" % (tablename, fn, file_date, table_date)
                    skip = False
                    
            if skip:
                if verbose:
                    print "Already have table %s, skipping file %s" % (tablename, fn)
                    sys.stdout.flush()
                continue

        #if date < '2014-07-27':
        #  continue
  
        print "Loading %s into table %s " % (fn, tablename)
        if verbose:
            print "start [%s]" % datetime.datetime.now()
        sys.stdout.flush()
        gsfn = fninfo['name']
        ret = bqutil.load_data_to_table(dataset, tablename, gsfn, SCHEMA, wait=wait, maxbad=1000)
  
    if verbose:
        print "-" * 77
        print "done with %s [%s]" % (course_id, datetime.datetime.now())
    print "=" * 77
    sys.stdout.flush()


def load_local_logs_to_biqquery(course_id, start_date, end_date, verbose):
    """
    Loads the local tracking logs into Google BigQuery.

    First, it will try to create the dataset if not exist.

    Args:
        course_id: Course id string.
        start_date: Start date string to process the tracking logs.
        end_date: End date string to process the tracking logs.
        verbose: Whether or not the function logging should be verbose.
    """
    dataset_name = bqutil.course_id2dataset(course_id, dtype="logs")
    date_pattern = getattr(edx2bigquery_config, 'TRACKING_LOG_REGEX_DATE_PATTERN', '')
    tracking_start_date, tracking_end_date = get_start_and_end_date(start_date, end_date)
    schema = get_tracking_log_schema()

    bqutil.create_dataset_if_nonexistent(dataset_name)

    for file_name in local_util.get_tracking_log_file_list(course_id):
        if not file_name:
            continue

        file_match = re.findall(date_pattern, file_name)

        if not file_match and verbose:
            logging(
                'The file name: {} does not have the string date at the end of the name.'.format(
                    file_name,
                )
            )
            continue

        file_date = dateutil.parser.parse(file_match[-1])

        if file_date <= tracking_end_date and file_date >= tracking_start_date:
            table_name = 'tracklog_{}'.format(
                file_date.strftime(getattr(edx2bigquery_config, 'TRACKING_LOG_DATE_FORMAT', '%Y-%m-%d'))
            )

            if verbose: logging('Uploading: {} to the table: {}'.format(file_name, table_name))

            upload_local_data = bqutil.upload_local_data_to_big_query(
                dataset_id=dataset_name,
                table_id=table_name,
                schema=schema,
                course_id=course_id,
                file_name=file_name,
            )
        elif verbose:
            logging(
                'The file with name: {} has a date before or after of the start_date and end_date provided.'.format(
                    file_name,
                )
            )
            continue

        if verbose:
            logging(
                'The file with name: {} has been succesufully uploaded to Big Query.'.format(
                    file_name,
                )
            )


def get_start_and_end_date(start_date, end_date):
    """
    Returns the start and end date to process the tracking log.

    If start date was not provided, by default, it will be two days before of the end date.
    If end date was not provided it will be the day today.
    If both were not provided, the start date will be two days before since the day today and
    end day will be the day today.

    Args:
        start_date: Start date string.
        end_date: End date string.
    Returns:
        tracking_start_date: Final start date to process the tracking logs.
        tracking_end_date: Final end date to process the tracking logs.
    """
    two_days_before = datetime.datetime.now() - datetime.timedelta(days=2)

    if end_date and not start_date:
        two_days_before = dateutil.parser.parse(end_date) - datetime.timedelta(days=2)

    tracking_start_date = dateutil.parser.parse(start_date) if start_date else two_days_before
    tracking_end_date = dateutil.parser.parse(end_date) if end_date else datetime.datetime.now()

    return tracking_start_date, tracking_end_date


def get_tracking_log_schema():
    """
    Returns the tracking schema from the edX2BigQuery schemas folder.

    Returns:
        Dict containing the tracking log schema.
    """
    my_path = os.path.dirname(os.path.realpath(__file__))
    schema = json.loads(
        open('{}/schemas/schema_tracking_log.json'.format(my_path)).read()
    )['tracking_log']

    return schema


def logging(message):
    """
    Common function to logging the provided message.

    Args:
        message: The string message to log to.
    """
    print('-' * 77)
    print(message)
    print('-' * 77)
    sys.stdout.flush()


#-----------------------------------------------------------------------------
