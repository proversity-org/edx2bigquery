"""
Module to manage course ids.

The converter functions were made in order to mantain the current functionality.
"""
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey


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
        return '{}:{}'.format(course_key.CANONICAL_NAMESPACE, course_key._to_string())

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

    if not course_key.deprecated:
        return course_key._to_deprecated_string()

    return course_id
