import unittest
import unittest.mock as mock
from dbclient import *
from dbclient.test.TestUtils import TEST_CONFIG

class TestCheckpointDbClient(unittest.TestCase):
    def test_restore_checkpoint_objects(self):
        TEST_CONFIG['checkpoint_dir'] = 'checkpoint'
        # Not restore checkpoint objects if disabled
        checkpoint_dbclient = CheckpointDbClient(TEST_CONFIG)
        checkpoint_dbclient.restore_checkpoint_objects(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT)
        with open("checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertFalse(checkpoint_dbclient.is_completed_object(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT, key.rstrip()))

        # restore objects in dict when checkpoint is enabled
        TEST_CONFIG['use_checkpoint'] = True
        checkpoint_dbclient = CheckpointDbClient(TEST_CONFIG)
        checkpoint_dbclient.restore_checkpoint_objects(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT)
        checkpoint_dbclient.restore_checkpoint_objects(WM_EXPORT, WORKSPACE_NOTEBOOK_ACL_OBJECT)
        with open("checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertTrue(checkpoint_dbclient.is_completed_object(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT, key.rstrip()))
        with open("checkpoint/export_acl_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertTrue(checkpoint_dbclient.is_completed_object(WM_EXPORT, WORKSPACE_NOTEBOOK_ACL_OBJECT, key.rstrip()))

    def test_checkpoint_get(self):
        # make api request if checkpoint is disabled
        TEST_CONFIG['checkpoint_dir'] = 'checkpoint'
        checkpoint_dbclient = CheckpointDbClient(TEST_CONFIG)
        checkpoint_dbclient.restore_checkpoint_objects(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT)
        checkpoint_dbclient.get = mock.MagicMock(return_value ={})
        with open("checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                checkpoint_status, result = checkpoint_dbclient.checkpoint_get(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT, key, "test_endpoint")
                self.assertFalse(checkpoint_status)

        # skip api requests if checkpoint is enabled and object is checkpointed
        TEST_CONFIG['use_checkpoint'] = True
        checkpoint_dbclient = CheckpointDbClient(TEST_CONFIG)
        checkpoint_dbclient.restore_checkpoint_objects(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT)
        checkpoint_dbclient.get = mock.MagicMock(return_value = {"code": 200})
        with open("checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                checkpoint_status, result = checkpoint_dbclient.checkpoint_get(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT, key.rstrip(), "test_endpoint")
                self.assertTrue(checkpoint_status)
                self.assertEqual(result, {})

        checkpoint_status, result = checkpoint_dbclient.checkpoint_get(WM_EXPORT, WORKSPACE_NOTEBOOK_OBJECT, "non/checkpoint/path", "test_endpoint")
        self.assertFalse(checkpoint_status)
        self.assertEqual(result['code'], 200)