import unittest
from unittest.mock import MagicMock
import mock as mock
from dbclient import HiveClient
from dbclient.test.TestUtils import TEST_CONFIG
from io import StringIO

class TestHiveClient(unittest.TestCase):

    def test_get_or_launch_cluster_default(self):
        checkpoint_service = MagicMock()
        hiveClient =  HiveClient(TEST_CONFIG, checkpoint_service)
        hiveClient.launch_cluster = MagicMock(return_value ="123")
        hiveClient.get_execution_context = MagicMock(return_value ="456")
        (cid, ec_id) = hiveClient.get_or_launch_cluster()
        self.assertEqual(cid, "123")
        self.assertEqual(ec_id, "456")

    def test_get_or_launch_cluster_cluster_name(self):
        checkpoint_service = MagicMock()
        hiveClient =  HiveClient(TEST_CONFIG, checkpoint_service)
        hiveClient.start_cluster_by_name = MagicMock(return_value ="123")
        hiveClient.get_execution_context = MagicMock(return_value ="456")
        (cid, ec_id) = hiveClient.get_or_launch_cluster("test")
        self.assertEqual(cid, "123")
        self.assertEqual(ec_id, "456")

    @mock.patch('dbclient.HiveClient.submit_command')
    def test_repair_legacy_tables(self, submit_command):
        def mock_submit_cmd(cid, ec_id, repair_cmd):
            if repair_cmd == cmd1:
                return {}
            if repair_cmd == cmd2:
                return {"resultType": "error"}

        checkpoint_service = MagicMock()
        hiveClient =  HiveClient(TEST_CONFIG, checkpoint_service)
        hiveClient.get_export_dir = MagicMock(return_value ="")
        hiveClient.get_or_launch_cluster = MagicMock(return_value=("123", "456"))
        cmd1 = """spark.sql("MSCK REPAIR TABLE default.test_legacy1")"""
        cmd2 = """spark.sql("MSCK REPAIR TABLE default.test_legacy2")"""
        submit_command.side_effect = mock_submit_cmd
        with mock.patch('sys.stdout', new = StringIO()) as fake_out:
            hiveClient.repair_legacy_tables(fix_table_log='test_repair_tables.log')
            output = fake_out.getvalue().split("\n")
            self.assertEqual(output[0], 'Table failed repair: default.test_legacy2')
            self.assertEqual(output[1], '1 table(s) failed to repair. See errors in failed_repair_tables.log')