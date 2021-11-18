import unittest
import unittest.mock as mock
from dbclient import dbclient

def test_client_config():
    return {
        'profile': 'test_profile',
        'url': 'https://test.url',
        'token': 'test_token',
        'is_aws': True,
        'verbose': False,
        'verify_ssl': False,
        'skip_failed': True,
        'debug': False,
        'file_format': 'DBC',
        'overwrite_notebooks': False,
        'export_dir': 'test_dir'
    }

class DBClientTest(unittest.TestCase):
    @mock.patch('time.sleep')
    @mock.patch('dbclient.parser.get_login_credentials')
    @mock.patch('requests.get')
    def test_renew_token(self, mock_get, mock_get_login_credentials, mock_sleep):
        config = test_client_config()
        client = dbclient(config)

        # Failure response due to expired token.
        expired_response = mock.MagicMock()
        expired_response.status_code = 403
        expired_response.text = "<title>Error 403 Invalid access token.</title>"

        # Successful response.
        successful_response = mock.MagicMock()
        successful_response.status_code = 200
        successful_response.text = "CONTENT"
        successful_response.json.return_value = {'user': 'foo'}

        mock_get.side_effect = [expired_response, successful_response]

        # Get the the old token first.
        old_token = {
            'host': 'http://test.url',
            'token': 'test_token'
        }

        # Get the the new token eventually.
        new_token = {
            'host': 'http://new.url',
            'token': 'new_token'
        }
        mock_get_login_credentials.side_effect = [old_token, old_token, new_token]

        result = client.get("/endpoint")
        assert(result == {'user': 'foo', 'http_status_code': 200})

    @mock.patch('time.sleep')
    @mock.patch('dbclient.parser.get_login_credentials')
    @mock.patch('requests.get')
    def test_renew_token_timeout(self, mock_get, mock_get_login_credentials, mock_sleep):
        config = test_client_config()
        client = dbclient(config)

        # Failure response due to expired token.
        expired_response = mock.MagicMock()
        expired_response.status_code = 403
        expired_response.text = "<title>Error 403 Invalid access token.</title>"

        mock_get.side_effect = [expired_response]

        # Always get the the old token.
        old_token = {
            'host': 'http://test.url',
            'token': 'test_token'
        }
        mock_get_login_credentials.return_value = old_token

        with self.assertRaises(Exception):
            client.get("/endpoint")


if __name__ == '__main__':
    unittest.main()
