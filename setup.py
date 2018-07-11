# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2018 Intermix Software, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
from pkg_resources import parse_version
from setuptools import setup


def expand_env_var(env_var):
    """
    Copied from Airflow v1.9 as it's not available in older versions.

    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def do_setup():
    from airflow import __version__ as AIRFLOW_VERSION
    # We don't include Airflow as a required package because we don't want to have the plugin installer override
    # the currently installed version. Instead we check for the version compatibility here.
    if parse_version(AIRFLOW_VERSION) < parse_version("1.1.0"):
        raise Exception("Airflow must be version 1.1.0 or greater")

    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    if not AIRFLOW_HOME:
        raise Exception("AIRFLOW_HOME must be defined prior to installation")
    AIRFLOW_HOME = expand_env_var(AIRFLOW_HOME)
    os.environ['PYTHONPATH'] = "{}:{}/plugins".format(os.getenv('PYTHONPATH', ''), AIRFLOW_HOME)

    setup(
        name='intermix-airflow-plugin',
        description='Plugin to add Intermix enrichment to Airflow',
        license='MIT License',
        version='0.2',
        scripts=['intermix.py'],
        zip_safe=True,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Topic :: System :: Monitoring',
        ],
        author='Intermix Software',
        author_email='contact@intermix.io',
        url='http://intermix.io/airflow_plugin',
    )


if __name__ == "__main__":
    do_setup()
