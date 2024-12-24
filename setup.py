#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, setuptools

def readme():
    with open('README.rst') as f:
        return f.read()

package_data = {'pg_chameleon': ['configuration/config-example.yml','sql/upgrade/*.sql','sql/drop_schema.sql','sql/create_schema.sql', 'LICENSE.txt','lib/*.jar']}

setup(
    name="chameleon",
    version="7.0.0rc1",
    description="MySQL to openGauss replica and migration",
    long_description=readme(),
    author = "Federico Campoli",
    author_email = "the4thdoctor.gallifrey@gmail.com",
    maintainer = "Federico Campoli",
    maintainer_email = "the4thdoctor.gallifrey@gmail.com",
    url="https://github.com/the4thdoctor/pg_chameleon/",
    license="BSD License",
    platforms=[
        "linux"
    ],
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Natural Language :: English",
        "Operating System :: POSIX :: BSD",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Other/Nonlisted Topic"
    ],
    py_modules=[
        "pg_chameleon.__init__",
        "pg_chameleon.lib.global_lib",
        "pg_chameleon.lib.mysql_lib",
        "pg_chameleon.lib.pg_lib",
        "pg_chameleon.lib.sql_util",
        "pg_chameleon.lib.ErrorCode"
    ],
    scripts=[
        "scripts/chameleon.py",
        "scripts/chameleon"
    ],
    install_requires=[
        'PyMySQL>=0.10.0, <1.0.0',
        'argparse==1.2.1',
        'mysql-replication==0.22',
        'py-opengauss==1.3.1',
        'PyYAML==5.1.2',
        'tabulate==0.8.1',
        'daemonize==2.4.7',
        'rollbar==0.13.17',
        'geomet==0.3.0',
        'mysqlclient==2.1.1',
        'kafka-python==2.0.2'
    ],
    include_package_data = True,
    package_data=package_data,
    packages=setuptools.find_packages(),
    python_requires='>=3.5',
    keywords='openGauss mysql replica migration database',

)
