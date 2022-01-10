import json
import os
import unittest
from unittest.mock import MagicMock
from dbclient import ClustersClient
from dbclient.test.TestUtils import TEST_CONFIG


class TestClustersClient(unittest.TestCase):
    TEST_OUTPUT_CREATOR_USER_IDS_FILE = "test_output_original_creator_user_ids.log"
    TEST_OUTPUT_CLUSTER_IDS_FILE = "test_output_cluster_ids_to_change_creator.log"
    TEST_DIR = "dbclient/test/cluster_client_test_files/"

    def _strip_new_lines_from_list(self, lst):
        return list(map(lambda x: x.strip(), lst))


    def test_log_cluster_ids_and_original_creators(self):
        EXPECTED_CREATOR_USER_IDS_FILE = "expected_original_creator_user_ids.log"
        EXPECTED_CLUSTER_IDS_FILE = "expected_cluster_ids_to_change_creator.log"


        clustersClient = ClustersClient(TEST_CONFIG)
        clustersClient.get_export_dir = MagicMock(return_value=self.TEST_DIR)
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
            self.TEST_OUTPUT_CREATOR_USER_IDS_FILE,
            self.TEST_OUTPUT_CLUSTER_IDS_FILE)

        with open(self.TEST_DIR + self.TEST_OUTPUT_CREATOR_USER_IDS_FILE, 'r') as fp:
            output_user_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + self.TEST_OUTPUT_CLUSTER_IDS_FILE, 'r') as fp:
            output_cluster_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + EXPECTED_CREATOR_USER_IDS_FILE, 'r') as fp:
            expected_user_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + EXPECTED_CLUSTER_IDS_FILE, 'r') as fp:
            expected_cluster_ids = self._strip_new_lines_from_list(fp.readlines())

        # os.remove(self.TEST_DIR + self.TEST_OUTPUT_CREATOR_USER_IDS_FILE)
        # os.remove(self.TEST_DIR + self.TEST_OUTPUT_CLUSTER_IDS_FILE)

        self.assertEqual(expected_user_ids, output_user_ids)
        self.assertEqual(expected_cluster_ids, output_cluster_ids)

    def test_log_cluster_ids_and_original_creators_with_same_cluster_names(self):
        EXPECTED_CREATOR_USER_IDS_FILE = "expected_original_creator_user_ids2.log"
        EXPECTED_CLUSTER_IDS_FILE = "expected_cluster_ids_to_change_creator2.log"

        clustersClient = ClustersClient(TEST_CONFIG)
        clustersClient.get_export_dir = MagicMock(return_value=self.TEST_DIR)

        # If there are multiple clusters with the same name, then only the cluster gets picked up.
        # e.g. In this case, only the new_cluster_id_3 has its creator_user_name changed
        clustersClient.get_cluster_list = MagicMock(return_value=[
            {
                'cluster_name': "same_cluster_name",
                'cluster_id': 'new_cluster_id_1',
                'creator_user_name': 'new_cluster_creator_user_name_1'
            },
            {
                'cluster_name': "same_cluster_name",
                'cluster_id': 'new_cluster_id_2',
                'creator_user_name': 'new_cluster_creator_user_name_2'
            },
            {
                'cluster_name': "same_cluster_name",
                'cluster_id': 'new_cluster_id_3',
                'creator_user_name': 'new_cluster_creator_user_name_3'
            }
        ])

        clustersClient._log_cluster_ids_and_original_creators(
            "test_clusters2.log",
            "test_user_name_to_user_id2.log",
            self.TEST_OUTPUT_CREATOR_USER_IDS_FILE,
            self.TEST_OUTPUT_CLUSTER_IDS_FILE)

        with open(self.TEST_DIR + self.TEST_OUTPUT_CREATOR_USER_IDS_FILE, 'r') as fp:
            output_user_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + self.TEST_OUTPUT_CLUSTER_IDS_FILE, 'r') as fp:
            output_cluster_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + EXPECTED_CREATOR_USER_IDS_FILE, 'r') as fp:
            expected_user_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + EXPECTED_CLUSTER_IDS_FILE, 'r') as fp:
            expected_cluster_ids = self._strip_new_lines_from_list(fp.readlines())

        os.remove(self.TEST_DIR + self.TEST_OUTPUT_CREATOR_USER_IDS_FILE)
        os.remove(self.TEST_DIR + self.TEST_OUTPUT_CLUSTER_IDS_FILE)

        self.assertEqual(expected_user_ids, output_user_ids)
        self.assertEqual(expected_cluster_ids, output_cluster_ids)

    def test_log_cluster_ids_and_original_creators_with_unchanged_creators(self):
        EXPECTED_CREATOR_USER_IDS_FILE = "expected_original_creator_user_ids3.log"
        EXPECTED_CLUSTER_IDS_FILE = "expected_cluster_ids_to_change_creator3.log"


        clustersClient = ClustersClient(TEST_CONFIG)
        clustersClient.get_export_dir = MagicMock(return_value=self.TEST_DIR)
        clustersClient.get_cluster_list = MagicMock(return_value=[
            {
                'cluster_name': "cluster_1",
                'cluster_id': 'new_cluster_id_1',
                'creator_user_name': 'original_cluster_creator_user_name_1'
            },
            {
                'cluster_name': "cluster_2",
                'cluster_id': 'new_cluster_id_2',
                'creator_user_name': 'original_cluster_creator_user_name_2'
            },
            {
                'cluster_name': "cluster_3",
                'cluster_id': 'new_cluster_id_3',
                'creator_user_name': 'original_cluster_creator_user_name_3'
            },
        ])
        clustersClient._log_cluster_ids_and_original_creators(
            "test_clusters.log",
            "test_user_name_to_user_id.log",
            self.TEST_OUTPUT_CREATOR_USER_IDS_FILE,
            self.TEST_OUTPUT_CLUSTER_IDS_FILE)

        with open(self.TEST_DIR + self.TEST_OUTPUT_CREATOR_USER_IDS_FILE, 'r') as fp:
            output_user_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + self.TEST_OUTPUT_CLUSTER_IDS_FILE, 'r') as fp:
            output_cluster_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + EXPECTED_CREATOR_USER_IDS_FILE, 'r') as fp:
            expected_user_ids = self._strip_new_lines_from_list(fp.readlines())

        with open(self.TEST_DIR + EXPECTED_CLUSTER_IDS_FILE, 'r') as fp:
            expected_cluster_ids = self._strip_new_lines_from_list(fp.readlines())

        os.remove(self.TEST_DIR + self.TEST_OUTPUT_CREATOR_USER_IDS_FILE)
        os.remove(self.TEST_DIR + self.TEST_OUTPUT_CLUSTER_IDS_FILE)

        self.assertEqual(expected_user_ids, output_user_ids)
        self.assertEqual(expected_cluster_ids, output_cluster_ids)


if __name__ == '__main__':
    unittest.main()
