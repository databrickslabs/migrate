import unittest
from unittest.mock import MagicMock
from dbclient import ScimClient
from dbclient.test.TestUtils import TEST_CONFIG

class TestScimClient(unittest.TestCase):

    def test_get_old_user_emails(self):
        checkpoint_service = MagicMock()
        scimClient =  ScimClient(TEST_CONFIG, checkpoint_service)
        scimClient.get_export_dir = MagicMock(return_value ="")
        user_list = scimClient.get_users_from_log("test_users.log")
        self.assertEqual(len(user_list), 2)

    def test_get_old_user_emails(self):
        checkpoint_service = MagicMock()
        scimClient =  ScimClient(TEST_CONFIG, checkpoint_service)
        scimClient.get_export_dir = MagicMock(return_value ="")
        old_user_map = scimClient.get_old_user_emails("test_users.log")
        self.assertEqual(old_user_map['29'], 'sourav.khandelwal@databricks.com')
        self.assertEqual(old_user_map['20'], 'test@databricks.com')


if __name__ == '__main__':
    unittest.main()