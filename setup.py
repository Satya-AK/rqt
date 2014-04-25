
from ez_setup import use_setuptools
use_setuptools()

import sys
from setuptools import setup, find_packages


execfile("lib/rqt/version.py")  # brings in __version__


# Maybe switch to using find_packages().
PKGLIST = [
    'rqt',
    ]


REQUIREMENTS = [
   "jinja2",
   "mako",
   "pystache",
   "psycopg2",
   "boto",
]

# argparse is not included with Python < 2.7
# e..g sys.version_info -> (2, 6, 5, 'final', 0)
if sys.version_info[0] == 2 and sys.version_info[1] < 7:
    REQUIREMENTS.append("argparse")

# For development, don't gather requirements on each build.
#REQUIREMENTS = []


setup(
    name='rqt',
    version=__version__,
    package_dir={'': 'lib'},
    packages=find_packages("lib"),
    install_requires=REQUIREMENTS,
    entry_points = {
        "console_scripts": [
            "rqt = rqt.cli:main"
        ]
    },

    # PyPI metadata 
    description='AWS Redshift Templated Queries Package',
    author="Eric Kamm",
    author_email="eric.kamm@accuenmedia.com",
    license="Apache License, Version 2.0",
    keywords="redshift query template",
)


