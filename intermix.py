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
#
from __future__ import unicode_literals
from future import standard_library
standard_library.install_aliases()

import base64
from builtins import str
from datetime import datetime
import inspect
import json
import traceback

from airflow import __version__ as AIRFLOW_VERSION
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

__PLUGIN_ID__ = 'intermix-airflow-plugin'
__VERSION__ = '0.2'

# The existing Postgres operator and hook methods
old_pg_execute = PostgresOperator.execute
old_get_first = PostgresHook.get_first
old_get_records = PostgresHook.get_records
old_run = PostgresHook.run


def inspector():
    """ Stack inspector to obtain runtime metadata for annotation """

    the_file = ''
    the_module = '__main__'
    the_class = ''
    the_function = ''
    the_linenumber = ''

    try:
        previous_frame = inspect.currentframe().f_back.f_back
        try:
            the_file, the_linenumber, the_function, lines, index = inspect.getframeinfo(previous_frame)
            the_linenumber = str(the_linenumber)
        finally:
            # Keeping references to frame objects can create reference cycles, so we make removal deterministic
            del previous_frame

        stack = inspect.stack()
        try:
            the_class = stack[2][0].f_locals["self"].__class__
        except KeyError:
            try:
                the_class = stack[2][0].f_locals["cls"]
            except KeyError:
                pass

        if the_class:
            the_module = the_class.__module__
            the_class = the_class.__name__

    except:
        traceback.print_exc()

    return (the_file, the_module, the_class, the_function, the_linenumber)


def annotator(inspected, _self=None):
    """ Top level annotation string creation function """

    the_file, the_module, the_class, the_function, the_linenumber = inspected
    blob = {'plugin': __PLUGIN_ID__, 'plugin_ver': __VERSION__, 'app': 'airflow',
            'app_ver': str(AIRFLOW_VERSION), 'at': datetime.utcnow().isoformat()+'Z',
            'file': the_file, 'module': the_module, 'classname': the_class, 'function': the_function,
            'linenumber': the_linenumber}
    if _self:
        for key in ('owner', 'run_as_user', 'dag_id', 'task_id', 'pool', 'queue'):
            key_attr = getattr(_self, key)
            if key_attr:
                mapped_key = {'dag_id': 'dag', 'task_id': 'task'}.get(key, key)
                blob.update({mapped_key: str(key_attr)})
    return "/* INTERMIX_ID: {} */ ".format(base64.b64encode(json.dumps(blob)))


def pg_execute_appended(self, context):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """

    try:
        blob = annotator(inspector(), self)
        # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
        #   4000000 4-byte characters.
        if len(blob) + len(self.sql) <= 4000000:
            self.sql = '{}{}'.format(blob, self.sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()

    return old_pg_execute(self, context)


def pg_get_first(self, sql, parameters=None):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """

    try:
        if '/* INTERMIX_ID:' not in sql:
            blob = annotator(inspector())
            # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
            #   4000000 4-byte characters.
            if len(blob) + len(sql) <= 4000000:
                sql = '{}{}'.format(blob, sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()

    return old_get_first(self, sql, parameters=parameters)


def pg_get_records(self, sql, parameters=None):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """

    try:
        if '/* INTERMIX_ID:' not in sql:
            blob = annotator(inspector())
            # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
            #   4000000 4-byte characters.
            if len(blob) + len(sql) <= 4000000:
                sql = '{}{}'.format(blob, sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()

    return old_get_records(self, sql, parameters=parameters)


def pg_run(self, sql, autocommit=False, parameters=None):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """
    try:
        if '/* INTERMIX_ID:' not in sql:
            blob = annotator(inspector())
            # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
            #   4000000 4-byte characters.
            if len(blob) + len(sql) <= 4000000:
                sql = '{}{}'.format(blob, sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()

    return old_run(self, sql, autocommit=autocommit, parameters=parameters)


# Monkey patch with the new execution methods
PostgresOperator.execute = pg_execute_appended
PostgresHook.get_first = pg_get_first
PostgresHook.get_records = pg_get_records
PostgresHook.run = pg_run
