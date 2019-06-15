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

import os
import sys
import auth
import json
import time
import glob
import re
import datetime
import bqutil
import gsutil

#-----------------------------------------------------------------------------

def load_all_daily_logs_for_course(
    course_id,
    use_local_files,
    gsbucket="gs://x-data",
    verbose=True,
    wait=False,
    check_dates=True,
):
    '''
    Load daily tracking logs for course from google storage into BigQuery.
    
    If wait=True then waits for loading jobs to be completed.  It's desirable to wait
    if subsequent jobs which need these tables (like person_day) are to be run
    immediately afterwards.
    '''

    print "Loading daily tracking logs for course %s into BigQuery (start: %s)" % (course_id, datetime.datetime.now())
    sys.stdout.flush()

    mypath = os.path.dirname(os.path.realpath(__file__))
    SCHEMA = json.loads(open('%s/schemas/schema_tracking_log.json' % mypath).read())['tracking_log']

    if not use_local_files:
        gsroot = gsutil.path_from_course_id(course_id)
        gsdir = '%s/%s/DAILY/' % (gsbucket, gsroot)
        file_name_set = gsutil.get_gs_file_list(gsdir)

    # if use_local_files::

  
    dataset = bqutil.course_id2dataset(gsroot, dtype="logs")
    import ipdb; ipdb.set_trace()
  
    # create this dataset if necessary
    bqutil.create_dataset_if_nonexistent(dataset)
    tables = bqutil.get_list_of_table_ids(dataset)
    tables = [x for x in tables if x.startswith('track')]
  
    if verbose:
        print "-"*77
        print "current tables loaded:", json.dumps(tables, indent=4)
        print "files to load: ", json.dumps(file_name_set.keys(), indent=4)
        print "-"*77
        sys.stdout.flush()
  
    for fn, fninfo in file_name_set.iteritems():

        if int(fninfo['size'])<=45:
            print "Zero size file %s, skipping" % fn
            continue

        m = re.search('(\d\d\d\d-\d\d-\d\d)', fn)
        # if not m:
        #     continue
        date = '2019-06-13'
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
        import ipdb; ipdb.set_trace()
        upload_local_data = bqutil.upload_local_data_to_big_query(
            dataset_id=dataset,
            table_id=tablename,
            schema=SCHEMA,
            course_id=course_id,
        )
        # ret = bqutil.load_data_to_table(dataset, tablename, gsfn, SCHEMA, wait=wait, maxbad=1000)
  
    if verbose:
        print "-" * 77
        print "done with %s [%s]" % (course_id, datetime.datetime.now())
    print "=" * 77
    sys.stdout.flush()


def load_logs_from_local_to_biqquery(course_id):
    """
    Loads the local tracking logs into Google BigQuery

    Args:
        course_id:
    """
    pass

#-----------------------------------------------------------------------------
