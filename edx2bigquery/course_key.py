"""
Module to manage course ids.

The converter functions were made in order to mantain the current functionality.
"""
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey

CCX_CANONICAL_NAMESPACE = 'ccx-v1'


def from_deprecated_course_id_string(course_id):
    """
    Converts the provided deprecated style course id to new course id style.

    e.g. edX/DemoX.1/2014 -> course-v1:edX+DemoX.1+2014

    Args:
        course_id: Course id string to convert.
    Returns:
        course id string converted or the same course id if it's not deprecated.
    """
    course_key = CourseKey.from_string(course_id)

    if course_key.deprecated:
        return str(course_key)

    return course_id


def to_deprecated_course_id_string(course_id):
    """
    Converts the provided new style course id to deprecated course id style.

    e.g. course-v1:edX+DemoX.1+2014 -> edX/DemoX.1/2014

    Args:
        course_id: Course id string to convert.
    Returns:
        course id string converted or the same course id if it's deprecated.
    """
    course_key = CourseKey.from_string(course_id)

    if course_key.deprecated:
        return course_id

    if course_key.CANONICAL_NAMESPACE == CCX_CANONICAL_NAMESPACE:
        return to_ccx_string(course_key)

    return course_key._to_deprecated_string()


def to_ccx_string(course_key):
    """
    Returns a forward slash style CCX course id string similar to a deprecated course id style.
    This function is required because the _to_deprecated_string method in the CCXLocator class
    is not implemented because the CCX course keys are not supposed to have a deprecated value.

    e.g. ccx-v1:edX+DemoX.1+2014+ccx@10 -> edX/DemoX.1/2014/10

    Args:
        course_key: opaque_keys.edx.keys.CourseKey instance.
    Returns:
        CCX course id string.
    """
    return '{org}/{course_number}/{course_run}/{ccx_id}'.format(
        org=course_key.org,
        course_number=course_key.course,
        course_run=course_key.run,
        ccx_id=course_key.ccx,
    )
