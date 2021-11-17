import json
import os
import unittest
from unittest.mock import MagicMock
from dbclient import ClustersClient
from dbclient.test.TestUtils import TEST_CONFIG


class TestClustersClient(unittest.TestCase):

    def test_log_cluster_ids_and_original_creators(self):
        TEST_OUTPUT_CREATOR_USER_IDS_FILE = "test_output_original_creator_user_ids.log"
        TEST_OUTPUT_CLUSTER_IDS_FILE = "test_output_cluster_ids_to_change_creator.log"
        EXPECTED_CREATOR_USER_IDS_FILE = "expected_original_creator_user_ids.log"
        EXPECTED_CLUSTER_IDS_FILE = "expected_cluster_ids_to_change_creator.log"

        TEST_DIR = "dbclient/test/"

        clustersClient = ClustersClient(TEST_CONFIG)
        clustersClient.get_export_dir = MagicMock(return_value =TEST_DIR)
        clustersClient.get_cluster_list = MagicMock(return_value=[
            {
                'cluster_name': "cluster_1",
                'cluster_id': 'new_cluster_id_1',
                'creator_user_name': 'new_cluster_creator_user_name_1'
            },
            {
                'cluster_name': "cluster_2",
                'cluster_id': 'new_cluster_id_2',
                'creator_user_name': 'new_cluster_creator_user_name_2'
            },
            {
                'cluster_name': "cluster_3",
                'cluster_id': 'new_cluster_id_3',
                'creator_user_name': 'new_cluster_creator_user_name_3'
            },
        ])
        clustersClient._log_cluster_ids_and_original_creators(
            "test_clusters.log",
            "test_user_name_to_user_id.log",
            TEST_OUTPUT_CREATOR_USER_IDS_FILE,
            TEST_OUTPUT_CLUSTER_IDS_FILE)

        with open(TEST_DIR + TEST_OUTPUT_CREATOR_USER_IDS_FILE, 'r') as fp:
            output_user_ids = json.loads(fp.read())

        with open(TEST_DIR + TEST_OUTPUT_CLUSTER_IDS_FILE, 'r') as fp:
            output_cluster_ids = json.loads(fp.read())

        with open(TEST_DIR + EXPECTED_CREATOR_USER_IDS_FILE, 'r') as fp:
            expected_user_ids = json.loads(fp.read())

        with open(TEST_DIR + EXPECTED_CLUSTER_IDS_FILE, 'r') as fp:
            expected_cluster_ids = json.loads(fp.read())

        os.remove(TEST_DIR + TEST_OUTPUT_CREATOR_USER_IDS_FILE)
        os.remove(TEST_DIR + TEST_OUTPUT_CLUSTER_IDS_FILE)

        self.assertEqual(expected_user_ids, output_user_ids)
        self.assertEqual(expected_cluster_ids, output_cluster_ids)

if __name__ == '__main__':
    unittest.main()
