import unittest
from dbclient.test.TestUtils import TEST_CONFIG
from checkpoint_service import CheckpointService
import wmconstants
import json
import os
import concurrent.futures

class TestCheckpointService(unittest.TestCase):
    def test_get_checkpoint_object_set(self):
        # Not restore checkpoint objects if disabled
        TEST_CONFIG['export_dir'] = 'test/'
        TEST_CONFIG['use_checkpoint'] = False
        checkpoint_service = CheckpointService(TEST_CONFIG)
        checkpoint_set = checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        with open("test/checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertFalse(checkpoint_set.contains(key.rstrip()))

        # restore objects in set when checkpoint is enabled
        TEST_CONFIG['use_checkpoint'] = True
        checkpoint_service = CheckpointService(TEST_CONFIG)
        checkpoint_set = checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        with open("test/checkpoint/export_notebooks.log", 'r') as read_fp:
            for key in read_fp:
                self.assertTrue(checkpoint_set.contains(key.rstrip('\n')))

    def test_checkpoint_key_map_get(self):
        TEST_CONFIG['export_dir'] = 'test/'
        TEST_CONFIG['use_checkpoint'] = True

        # restore objects in dict when checkpoint is enabled
        checkpoint_service = CheckpointService(TEST_CONFIG)
        checkpoint_key_map = checkpoint_service.get_checkpoint_key_map(
            wmconstants.WM_IMPORT, wmconstants.MLFLOW_RUN_OBJECT)
        with open("test/checkpoint/import_mlflow_runs.log", 'r') as read_fp:
            for single_key_value_map_str in read_fp:
                single_key_value_map = json.loads(single_key_value_map_str)
                assert(checkpoint_key_map.get(single_key_value_map["key"]) == single_key_value_map["value"])

    def test_checkpoint_key_map_check_contains_otherwise_makr_in_use(self):
        TEST_CONFIG['export_dir'] = 'test/'
        TEST_CONFIG['use_checkpoint'] = True
        # Create an empty temp file
        with open("test/checkpoint/export_mlflow_runs.log", 'w') as fp:
            pass

        checkpoint_service = CheckpointService(TEST_CONFIG)
        checkpoint_key_map = checkpoint_service.get_checkpoint_key_map(
            wmconstants.WM_EXPORT, wmconstants.MLFLOW_RUN_OBJECT)

        # This dictionary is used to keep track of how many times the key value was updated
        key_counter_map = {
            "key_1": 0,
            "key_2": 0,
            "key_3": 0,
            "key_4": 0,
            "key_5": 0,
        }
        # intentionally give duplicate keys, in order to test the thread safety of the key_map
        keys = [
            "key_1", "key_2", "key_3", "key_4", "key_5",
            "key_1", "key_2", "key_3", "key_4", "key_5",
            "key_2", "key_3", "key_4", "key_5",
            "key_4", "key_5", "key_3"
        ]

        def _increment_counter_if_available(key):
            # if the key is empty (not in use or set value), mark and increment the counter
            # Test that this is indeed thread_safe
            if not checkpoint_key_map.check_contains_otherwise_mark_in_use(key):
                key_counter_map[key] += 1
                checkpoint_key_map.write(key, "done operation")

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(_increment_counter_if_available(key)) for key in keys]
            concurrent.futures.wait(futures)

        for key in key_counter_map:
            # Each key must be updated only once
            assert(key_counter_map[key] == 1)

        os.remove("test/checkpoint/export_mlflow_runs.log")
