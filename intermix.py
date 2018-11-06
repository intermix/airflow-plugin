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
import os
import sys
import re
import traceback

from airflow import __version__ as AIRFLOW_VERSION
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

# Patch S3ToRedshiftOperator in RedshiftPlugin, if it exists
in_path = True
this_parent_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
try:
    # Ensure the local plugin folder is in the path
    if this_parent_path not in sys.path:
        in_path = False
        sys.path.append(this_parent_path)
    from RedshiftPlugin.operators.s3_to_redshift import S3ToRedshiftOperator
    PATCH_S3_TO_REDSHIFT = True
except ImportError:
    PATCH_S3_TO_REDSHIFT = False
finally:
    if not in_path:
        sys.path.remove(this_parent_path)

try:
    basestring
except NameError:
    basestring = str

__PLUGIN_ID__ = 'intermix-airflow-plugin'
__VERSION__ = '0.4'

# The existing Postgres operator and hook methods
old_pg_execute = PostgresOperator.execute
old_get_first = PostgresHook.get_first
old_get_records = PostgresHook.get_records
old_run = PostgresHook.run
if PATCH_S3_TO_REDSHIFT:
    old_s3_rs_execute = S3ToRedshiftOperator.execute


INTERMIX_RE = re.compile(r"(\s*/\* INTERMIX_ID.*?\*/)", re.UNICODE)


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


def annotator(inspected, _self=None, prior_annotation=None):
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

    # If there is already an annotation, keep these values
    if prior_annotation:
        match_str = prior_annotation.groups()[0].strip()[16:-3]
        parsed = json.loads(base64.b64decode(match_str))
        blob.update(parsed)

    # Encode/decode for Python 2/3 compatiblity
    return "/* INTERMIX_ID: {} */ ".format(base64.b64encode(json.dumps(blob).encode()).decode())


def pg_execute_appended(self, context):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """

    try:
        prior_annotation = re.search(INTERMIX_RE, self.sql)
        if prior_annotation:
            self.sql = re.sub(INTERMIX_RE, '', self.sql)
        blob = annotator(inspector(), self, prior_annotation)
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
    sql_is_string = False
    if isinstance(sql, basestring):
        sql_is_string = True
        sql = [sql]

    new_sql = []
    try:
        for _sql in sql:
            if '/* INTERMIX_ID:' not in _sql:
                blob = annotator(inspector())
                # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
                #   4000000 4-byte characters.
                if len(blob) + len(_sql) <= 4000000:
                    new_sql.append('{}{}'.format(blob, _sql))
                else:
                    new_sql.append(_sql)
            else:
                new_sql.append(_sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()
        if len(new_sql) < len(sql):
            new_sql = sql

    if sql_is_string:
        new_sql = new_sql[0]
    return old_get_first(self, new_sql, parameters=parameters)


def pg_get_records(self, sql, parameters=None):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """
    sql_is_string = False
    if isinstance(sql, basestring):
        sql_is_string = True
        sql = [sql]

    new_sql = []
    try:
        for _sql in sql:
            if '/* INTERMIX_ID:' not in _sql:
                blob = annotator(inspector())
                # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
                #   4000000 4-byte characters.
                if len(blob) + len(_sql) <= 4000000:
                    new_sql.append('{}{}'.format(blob, _sql))
                else:
                    new_sql.append(_sql)
            else:
                new_sql.append(_sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()
        if len(new_sql) < len(sql):
            new_sql = sql

    if sql_is_string:
        new_sql = new_sql[0]
    return old_get_records(self, new_sql, parameters=parameters)


def pg_run(self, sql, autocommit=False, parameters=None):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """
    sql_is_string = False
    if isinstance(sql, basestring):
        sql_is_string = True
        sql = [sql]

    new_sql = []
    try:
        for _sql in sql:
            if '/* INTERMIX_ID:' not in _sql:
                blob = annotator(inspector())
                # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case
                # scenario of 4000000 4-byte characters.
                if len(blob) + len(_sql) <= 4000000:
                    new_sql.append('{}{}'.format(blob, _sql))
                else:
                    new_sql.append(_sql)
            else:
                new_sql.append(_sql)
    except:
        # If anything raises an exception, we still want it to continue executing as normal
        traceback.print_exc()
        if len(new_sql) < len(sql):
            new_sql = sql

    if sql_is_string:
        new_sql = new_sql[0]
    return old_run(self, new_sql, autocommit=autocommit, parameters=parameters)


def s3_rs_execute(self, context):
    """ Appends a metadata blob as a comment to the front of the query before it is executed.
    """

    # We need a different 'self' for the correct context for the RedshiftPlugin so we need to use different functions
    #   here. Unlike in the general case, the S3-to-Redshift Operator does not use any list parameters for 'sql'.
    def s3_pg_get_first(_self, sql, parameters=None):
        try:
            blob = annotator(inspector(), self)
            # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
            #   4000000 4-byte characters.
            if len(blob) + len(sql) <= 4000000:
                sql = '{}{}'.format(blob, sql)
        except:
            # If anything raises an exception, we still want it to continue executing as normal
            traceback.print_exc()

        return old_get_first(_self, sql, parameters=parameters)

    def s3_pg_get_records(_self, sql, parameters=None):
        try:
            blob = annotator(inspector(), self)
            # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
            #   4000000 4-byte characters.
            if len(blob) + len(sql) <= 4000000:
                sql = '{}{}'.format(blob, sql)
        except:
            # If anything raises an exception, we still want it to continue executing as normal
            traceback.print_exc()

        return old_get_records(_self, sql, parameters=parameters)

    def s3_pg_run(_self, sql, autocommit=False, parameters=None):
        try:
            blob = annotator(inspector(), self)
            # Redshift has a 16MB query length limit so we won't annotate if the length exceeds a worst case scenario of
            #   4000000 4-byte characters.
            if len(blob) + len(sql) <= 4000000:
                sql = '{}{}'.format(blob, sql)
        except:
            # If anything raises an exception, we still want it to continue executing as normal
            traceback.print_exc()

        return old_run(_self, sql, autocommit=autocommit, parameters=parameters)

    # Patch only for S3ToRedshiftOperator
    if PostgresHook.get_first.__name__ != 's3_pg_get_first':
        PostgresHook.get_first = s3_pg_get_first
        PostgresHook.get_records = s3_pg_get_records
        PostgresHook.run = s3_pg_run

    # Execute with the specially patched PostgresHook
    try:
        return_value = old_s3_rs_execute(self, context)
    except:
        raise
    finally:
        # Undo the patch but leave the existing one in place
        if PostgresHook.get_first.__name__ == 's3_pg_get_first':
            PostgresHook.get_first = pg_get_first
            PostgresHook.get_records = pg_get_records
            PostgresHook.run = pg_run

    return return_value


# Monkey patch with the new execution methods if they haven't already been patched
if old_pg_execute.__name__ != 'pg_execute_appended':
    PostgresOperator.execute = pg_execute_appended
    PostgresHook.get_first = pg_get_first
    PostgresHook.get_records = pg_get_records
    PostgresHook.run = pg_run

    if PATCH_S3_TO_REDSHIFT:
        S3ToRedshiftOperator.execute = s3_rs_execute
