# -*- coding: utf-8 -*-
"""
superserial  Copyright (C) 2015  Steven Cutting - License AGPL: superserial/LICENSE
"""

from superserial import(__title__, __version__, __status__)

from setuptools import setup, find_packages

with open("README.md") as fp:
    THE_LONG_DESCRIPTION = fp.read()


setup(
    name=__title__,
    version=__version__,
    license='GNU AGPL',
    description="Both abstracts IO, and raises it up. Cleanly splitting IO and Logic.",
    long_description=THE_LONG_DESCRIPTION,
    classifiers=['Topic :: IO',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Data Scientists',
                 'Operating System :: GNU Linux :: Linux',
                 'Operating System :: OSX :: MacOS :: MacOS X',
                 'Operating System :: POSIX',
                 'Development Status :: 3 - Alpha',
                 'Programming Language :: Python :: 2.7',
                 'License :: GNU AGPL',
                 'Status :: ' + __status__
                 ],
    keywords='io postgres s3 aws files storage bulk',
    author='Steven Cutting',
    author_email='steven.e.cutting@linux.com',
    packages=find_packages(exclude=('scripts', 'tests')),
    # zip_safe=False,
    install_requires=['pathlib',
                      'pyyaml',
                      'dill',
                      'six',
                      'sqlalchemy',
                      'dataset',
                      'arrow',
                      'boto3',
                      ],
)
