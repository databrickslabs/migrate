import tempfile
import unittest
import unittest.mock as mock
from tasks import *

test_export_dir = tempfile.TemporaryDirectory()


def test_client_config():
    return {
        'url': 'https://test.url',
        'token': 'test_token',
        'is_aws': True,
        'verbose': False,
        'verify_ssl': False,
        'skip_failed': True,
        'debug': False,
        'file_format': 'DBC',
        'overwrite_notebooks': False,
        'export_dir': test_export_dir.name + '/'
    }


class RequestMockContainer:
    def __init__(self, mock_get):
        self._mock_get = mock_get
        self._get_requests = []
        self._get_responses = []

    def mock_get_call(self, request, response):
        self._get_requests.append(request)
        self._get_responses.append(response)
        return self

    def __enter__(self):
        self._mock_get.side_effect = self._get_responses

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
        # self._mock_get.assert_has_calls(self._get_requests, any_order=False)


def test_path(file_name):
    return f'./tasks/test_data/{file_name}'


def mock_request(endpoint, version='2.0'):
    """Set the expected parameters to calls e.g. requests.get()."""
    client_config = test_client_config()
    return mock.call(f'{client_config["url"]}/api/{version}{endpoint}',
                     headers={
                         'Authorization': f'Bearer {client_config["token"]}',
                         'User-Agent': 'databrickslabs-migrate/0.1.0'
                     },
                     verify=client_config['verify_ssl']),


def mock_response(value=None, file=None, status_code=200):
    """Set the mocked return values to calls e.g. requests.get().

    :param value - return_value of response.json. If none, file will be used.
    :param file - relative file name starting from ./test_data.
    :param status_code - status_code of response.
    """
    response = mock.MagicMock()
    response.status_code = status_code
    if value:
        response.json.return_value = value
    else:
        with open(test_path(file), 'r') as f:
            response.json.return_value = json.load(f)
    return response


def print_file(file):
    with open(file, 'r') as f:
        print(f.read())


class BaseTaskTest(unittest.TestCase):
    def assert_json_files_equal(self, actual, expected):
        """Assert two json files equal.

        :param actual - relative file name starting from client_config['export_dir']
        :param expected - relative file name starting from ./test_data
        """
        def read_json_file(file):
            with open(file, 'r') as f:
                lines = sorted(f.readlines())
            return [json.loads(line) for line in lines]

        actual_json = read_json_file(f'{test_export_dir.name}/{actual}')
        expected_json = read_json_file(test_path(expected))
        self.assertEqual(actual_json, expected_json)


class ExportUserTaskTest(BaseTaskTest):
    @mock.patch('requests.get')
    def test_run(self, mock_get):
        with RequestMockContainer(mock_get).mock_get_call(
                mock_request('/preview/scim/v2/Users'), mock_response(file='users.json')
        ).mock_get_call(
            mock_request('/preview/scim/v2/Groups'), mock_response(file='groups.json')
        ).mock_get_call(
            mock_request('/preview/scim/v2/Users/001'),
            mock_response(value={'userName': 'alice@databricks.com'})
        ).mock_get_call(
            mock_request('/preview/scim/v2/Users/002'),
            mock_response(value={'userName': 'bob@databricks.com'})
        ).mock_get_call(
            mock_request('/instance-profiles/list'),
            mock_response(file='instance_profiles.json')
        ):
            UserExportTask(test_client_config()).run()

        self.assert_json_files_equal('users.log', 'expected_users.log')
        self.assert_json_files_equal('instance_profiles.log', 'expected_instance_profiles.log')
        self.assert_json_files_equal('groups/admins_group', 'expected_admins_group.log')
        self.assert_json_files_equal('groups/testing_group', 'expected_testing_group.log')


if __name__ == '__main__':
    unittest.main()
