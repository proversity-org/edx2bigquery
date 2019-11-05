import glob
from setuptools import setup

def findfiles(pat):
    return [x for x in glob.glob('share/' + pat)]

data_files = [
#    ('share/render', findfiles('render/*')),
    ]

# print "data_files = %s" % data_files

setup(
    name='edx2bigquery',
    version='1.3.0',
    author='I. Chuang',
    author_email='ichuang@mit.edu',
    packages=['edx2bigquery', 'edx2bigquery.test'],
    scripts=[],
    url='https://github.com/mitodl/edx2bigquery',
    license='LICENSE',
    description='Import research data from edX dumps into google BigQuery',
    long_description=open('README.md').read(),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'edx2bigquery = edx2bigquery.main:CommandLine',
            ],
        },
    install_requires=[
        'path.py==11.5.2',
        'pygeoip==0.3.2',
        'pytz==2019.1',
        'python-dateutil==2.8.0',
        'geoip2==2.9.0',
        'lxml==4.3.4',
        'BeautifulSoup==3.2.1',
        'unicodecsv==0.14.1',
        'Jinja2==2.10.1',
        'google-api-python-client==1.7.9',
        'edxcut',
        'boto3==1.9.166',
        'oauth2client==4.1.3',
        'google-cloud-bigquery==1.15.0',
        'edx-opaque-keys==1.0.1',
        'pandas==0.24.2',
        'Unidecode==1.1.1',
    ],
    dependency_links = [],
    package_dir={'edx2bigquery': 'edx2bigquery'},
    package_data={'edx2bigquery': ['lib/*', 'bin/*'] },
    # package_data={ 'edx2bigquery': ['python_lib/*.py'] },
    # data_files = data_files,
    test_suite = "edx2bigquery.test",
)
