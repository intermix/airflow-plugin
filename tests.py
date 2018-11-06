from __future__ import unicode_literals

import base64
from datetime import datetime
import json
from mock import patch
import os
import psycopg2
import sys
import unittest

# Import to get S3ToRedshiftOperator patch status
try:
    from intermix import PATCH_S3_TO_REDSHIFT
except ImportError:
    # When this file is loaded by the plugin manager the intermix module won't be on the path so this will throw an
    #   error but doesn't have any side effects as the plugin manager doesn't run tests.
    pass

from airflow import __version__ as AIRFLOW_VERSION
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


class TestPatchedExecute(unittest.TestCase):

    def test_is_patched(self):
        self.assertEqual('pg_execute_appended', PostgresOperator.execute.__name__)
        self.assertEqual('pg_get_first', PostgresHook.get_first.__name__)
        self.assertEqual('pg_get_records', PostgresHook.get_records.__name__)
        self.assertEqual('pg_run', PostgresHook.run.__name__)
        if PATCH_S3_TO_REDSHIFT:
            this_parent_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
            sys.path.append(this_parent_path)
            from RedshiftPlugin.operators.s3_to_redshift import S3ToRedshiftOperator
            self.assertEqual('s3_rs_execute', S3ToRedshiftOperator.execute.__name__)

    @patch.object(psycopg2, 'connect')
    def test_prepends_blob_in_hook(self, psycopg2_connect):
        """ Test patching the PostgresHook
        """
        def capture(sql, *args, **kwargs):
            return sql
        execute = psycopg2_connect.return_value.cursor.return_value.execute
        execute.side_effect = capture
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run(sql=['select * from foousers;'])
        hook.get_records(sql='select * from barusers;')
        sql_executed = []
        for call in execute.call_args_list:
            args, kwargs = call
            sql_executed.append(args[0])

        # Original query is intact
        self.assertEqual('select * from foousers;', sql_executed[0][-23:])
        self.assertEqual('select * from barusers;', sql_executed[1][-23:])

        self.assertEqual('/* INTERMIX_ID: ', sql_executed[0][:16])
        self.assertEqual('/* INTERMIX_ID: ', sql_executed[1][:16])
        self.assertEqual('*/', sql_executed[0][-26:-24])
        self.assertEqual('*/', sql_executed[1][-26:-24])

        base64_blob = sql_executed[0][16:-26]
        deserialized_blob = json.loads(base64.b64decode(base64_blob))
        self.assertGreaterEqual(datetime.utcnow(),
                                datetime.strptime(deserialized_blob['at'], "%Y-%m-%dT%H:%M:%S.%fZ"))
        del deserialized_blob['at']
        self.assertDictEqual({'plugin': 'intermix-airflow-plugin', 'plugin_ver': '0.4', 'app': 'airflow',
                              'module': '__main__', 'classname': 'TestPatchedExecute', 'file': 'tests.py',
                              'function': 'test_prepends_blob_in_hook', 'linenumber': '47',
                              'app_ver': str(AIRFLOW_VERSION)}, deserialized_blob)

    @patch.object(psycopg2, 'connect')
    def test_prepends_blob_in_operator(self, psycopg2_connect):
        """ Test patching the PostgresOperator
        """
        def capture(sql, *args, **kwargs):
            return sql
        execute = psycopg2_connect.return_value.cursor.return_value.execute
        execute.side_effect = capture
        PO = PostgresOperator(sql='select * from users;', task_id='some_task')
        PO.execute(None)
        # Original query is intact
        self.assertEqual('select * from users;', PO.sql[-20:])

        prepend = PO.sql[:-21]
        self.assertEqual('/* INTERMIX_ID: ', prepend[:16])
        self.assertEqual('*/', prepend[-2:])

        base64_blob = prepend[16:-2]
        deserialized_blob = json.loads(base64.b64decode(base64_blob.encode()).decode())
        self.assertGreaterEqual(datetime.utcnow(),
                                datetime.strptime(deserialized_blob['at'], "%Y-%m-%dT%H:%M:%S.%fZ"))
        del deserialized_blob['at']
        self.assertDictEqual({'queue': 'default', 'task': 'some_task', 'plugin': 'intermix-airflow-plugin',
                              'module': '__main__', 'classname': 'TestPatchedExecute',
                              'file': 'tests.py', 'function': 'test_prepends_blob_in_operator', 'plugin_ver': '0.4',
                              'app': 'airflow', 'app_ver': str(AIRFLOW_VERSION), 'owner': 'Airflow', 'linenumber': '82',
                              'dag': 'adhoc_Airflow'}, deserialized_blob)


if __name__ == '__main__':
    unittest.main()
