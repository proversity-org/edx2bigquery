#!/usr/bin/python

import os
import sys
import traceback

import axis2bigquery
import edx2bigquery_config
import edx2course_axis
import load_course_sql


def process_course(course_id, basedir, datedir, use_dataset_latest, use_local_files, verbose=False, pin_date=None, stop_on_error=True,):
    if pin_date:
        datedir = pin_date
    sdir = load_course_sql.find_course_sql_dir(course_id, 
                                               basedir=basedir,
                                               datedir=datedir,
                                               use_dataset_latest=(use_dataset_latest and not pin_date),
                                               )
    edx2course_axis.DATADIR = sdir
    edx2course_axis.VERBOSE_WARNINGS = verbose

    fn_to_try = getattr(edx2bigquery_config, 'COURSE_FILES_PREFIX_NAMES', [])

    for fntt in fn_to_try:
        fn = sdir / fntt
        if os.path.exists(fn):
            break

    if not os.path.exists(fn):
        print "---> oops, cannot generate course axis for %s, file %s (or 'course.xml.tar.gz' or 'course-prod-edge-analytics.xml.tar.gz') missing!" % (course_id, fn)
        sys.stdout.flush()
        return

    # TODO: only create new axis if the table is missing, or the course axis is not already created

    try:
        edx2course_axis.process_xml_tar_gz_file(
            fn,
            use_local_files=use_local_files,
            use_dataset_latest=use_dataset_latest,
            force_course_id=course_id,
        )
    except Exception as err:
        print err
        traceback.print_exc()
        if stop_on_error:
            raise
        # raise
