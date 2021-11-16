import unittest
from dbclient.test.TestUtils import TEST_CONFIG
from checkpoint_service import CheckpointService
import wmconstants

class TestCheckpointService(unittest.TestCase):
    def test_get_checkpoint_object_set(self):
        # Not restore checkpoint objects if disabled
        checkpoint_service = CheckpointService(TEST_CONFIG)
        checkpoint_set = checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        with open("checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertFalse(checkpoint_set.contains(key.rstrip()))

        # restore objects in dict when checkpoint is enabled
        TEST_CONFIG['use_checkpoint'] = True
        checkpoint_service = CheckpointService(TEST_CONFIG)
        checkpoint_set = checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        with open("checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertTrue(checkpoint_set.contains(key.rstrip('\n')))