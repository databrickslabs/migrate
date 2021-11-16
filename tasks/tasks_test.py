import tempfile
import unittest
import unittest.mock as mock
from tasks import *

test_export_dir = tempfile.TemporaryDirectory()


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
        'export_dir': test_export_dir.name + '/'
    }


class MockFunctionCallContainer:
    class _MockFunction:
        def __init__(self, mock_function):
            self._mock_function = mock_function
            self._mock_requests = []
            self._mock_responses = []

        def enter(self):
            self._mock_function.side_effect = self._mock_responses

        def exit(self):
            self._mock_function.assert_has_calls(self._mock_requests, any_order=False)

        def call(self, request, response):
            self._mock_requests.append(request)
            self._mock_responses.append(response)

    def __init__(self, mock_get=None, mock_post=None, mock_patch=None):
        self._mock_functions = []
        if mock_get is not None:
            self._mock_get = self._MockFunction(mock_get)
            self._mock_functions.append(self._mock_get)
        if mock_post is not None:
            self._mock_post = self._MockFunction(mock_post)
            self._mock_functions.append(self._mock_post)
        if mock_patch is not None:
            self._mock_patch = self._MockFunction(mock_patch)
            self._mock_functions.append(self._mock_patch)

    def mock_get_call(self, request, response):
        self._mock_get.call(request, response)
        return self

    def mock_post_call(self, request, response):
        self._mock_post.call(request, response)
        return self

    def mock_patch_call(self, request, response):
        self._mock_patch.call(request, response)
        return self

    def __enter__(self):
        for mock_function in self._mock_functions:
            mock_function.enter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_tb:
            for mock_function in self._mock_functions:
                mock_function.exit()


def test_path(file_name):
    return f'./tasks/test_data/{file_name}'


def log_path(file_name):
    return f'{test_export_dir.name}/{file_name}'


def mock_request(endpoint, version='2.0', json_params=None):
    """Set the expected parameters to calls e.g. requests.get()."""
    client_config = test_client_config()

    args = [f'{client_config["url"]}/api/{version}{endpoint}']
    kwargs = {
        'headers': {
            'Authorization': f'Bearer {client_config["token"]}',
            'User-Agent': 'databrickslabs-migrate/0.1.0'
        },
        'json': json_params,
        'verify': client_config['verify_ssl']
    }

    mock_call = mock.call(*args, **{k: v for k, v in kwargs.items() if v is not None})
    return mock_call


def mock_response(value=None, file=None, status_code=200):
    """Set the mocked return values to calls e.g. requests.get().

    :param value - return_value of response.json.
    :param file - relative file name starting from ./test_data.
    :param status_code - status_code of response.
    """
    response = mock.MagicMock()
    response.status_code = status_code
    if value:
        response.json.return_value = value
    elif file:
        with open(test_path(file), 'r') as f:
            response.json.return_value = json.load(f)
    return response


def print_file(file):
    with open(file, 'r') as f:
        print(f.read())


def read_json_file(file):
    with open(file, 'r') as f:
        lines = sorted(f.readlines())
    return [json.loads(line) for line in lines]


class BaseTaskTest(unittest.TestCase):
    def assert_json_files_equal(self, actual, expected):
        """Assert two json files equal.

        :param actual - relative file name starting from client_config['export_dir']
        :param expected - relative file name starting from ./test_data
        """
        actual_json = read_json_file(log_path(actual))
        expected_json = read_json_file(test_path(expected))
        self.assertEqual(actual_json, expected_json)


class ExportUserTaskTest(BaseTaskTest):
    @mock.patch('requests.get')
    def test_run(self, mock_get):
        with MockFunctionCallContainer(
                mock_get=mock_get
        ).mock_get_call(
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


class ImportUserTaskTest(BaseTaskTest):
    @mock.patch('requests.patch')
    @mock.patch('requests.post')
    @mock.patch('requests.get')
    def test_run(self, mock_get, mock_post, mock_patch):
        # Import instance profiles.
        container = MockFunctionCallContainer(
            mock_get=mock_get, mock_post=mock_post, mock_patch=mock_patch
        ).mock_get_call(
            mock_request('/instance-profiles/list'),
            mock_response(file='partial_instance_profiles.json')
        ).mock_post_call(
            mock_request('/instance-profiles/add',
                         json_params={'instance_profile_arn': 'arn:aws:iam::role_a'}),
            mock_response()
        ).mock_post_call(
            mock_request('/instance-profiles/add',
                         json_params={'instance_profile_arn': 'arn:aws:iam::role_c'}),
            mock_response()
        )

        # Import users.
        for user in read_json_file(log_path('users.log')):
            create_keys = ('emails', 'entitlements', 'displayName', 'name', 'userName')
            create_user = {k: user[k] for k in create_keys if k in user}
            container.mock_post_call(
                mock_request('/preview/scim/v2/Users', json_params=create_user),
                mock_response()
            )

        # Check imported users.
        container.mock_get_call(
            mock_request('/preview/scim/v2/Users'), mock_response(file='users.json')
        )

        # Import groups.
        for group in ['admins_group', 'testing_group']:
            create_group = {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                "displayName": group
            }
            container.mock_post_call(
                mock_request('/preview/scim/v2/Groups', json_params=create_group),
                mock_response()
            )

        # Check imported groups.
        container.mock_get_call(
            mock_request('/preview/scim/v2/Groups'), mock_response(file='groups.json')
        )

        # Add members.
        for group_id, member_id in [('999', '001'), ('998', '002')]:
            patch_members = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{
                    "op": "add",
                    "value": {"members": [{
                        "value": f"{member_id}"
                    }]}
                }]
            }
            container.mock_patch_call(
                mock_request(f'/preview/scim/v2/Groups/{group_id}', json_params=patch_members),
                mock_response()
            )

        # Add group roles.
        container.mock_get_call(
            mock_request('/preview/scim/v2/Groups'), mock_response(file='groups.json')
        )

        for group_id, roles in [
            ('999', [{"value": "arn:aws:iam::role_a"}, {"value": "arn:aws:iam::role_b"}]),
            ('998', [{"value": "arn:aws:iam::role_c"}])
        ]:
            patch_roles = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                           "Operations": [{"op": "add",
                                           "path": "roles",
                                           "value": roles}]}
            patch_entitlements = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                  "Operations": [{"op": "add",
                                                  "path": "entitlements",
                                                  "value": [{"value": "allow-cluster-create"}]}]}
            container.mock_patch_call(
                mock_request(f'/preview/scim/v2/Groups/{group_id}', json_params=patch_roles),
                mock_response()
            ).mock_patch_call(
                mock_request(f'/preview/scim/v2/Groups/{group_id}', json_params=patch_entitlements),
                mock_response()
            )

        # Add user roles.
        # Only add missing roles.
        for user_id, current_roles, expected_roles in [
            ('001', [{"value": "arn:aws:iam::role_a"}], [{"value": "arn:aws:iam::role_b"}]),
            ('002', [], [{"value": "arn:aws:iam::role_c"}])
        ]:
            patch_roles = {
                'schemas': ['urn:ietf:params:scim:api:messages:2.0:PatchOp'],
                'Operations': [
                    {
                        'op': 'add',
                        'path': 'roles',
                        'value': expected_roles
                    }
                ]
            }
            container.mock_get_call(
                mock_request(f'/preview/scim/v2/Users/{user_id}'),
                mock_response(value={'roles': current_roles, 'id': f'{user_id}'})
            ).mock_patch_call(
                mock_request(f'/preview/scim/v2/Users/{user_id}', json_params=patch_roles),
                mock_response()
            )

        # Add user entitlements.
        container.mock_get_call(
            mock_request('/preview/scim/v2/Groups'), mock_response(file='groups.json')
        )

        for group_id in ['999', '998']:
            patch_entitlements = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                  "Operations": [{"op": "add",
                                                  "path": "entitlements",
                                                  "value": [{"value": "allow-cluster-create"}]}]}
            container.mock_patch_call(
                mock_request(f'/preview/scim/v2/Groups/{group_id}', json_params=patch_entitlements),
                mock_response()
            )

        with container:
            UserImportTask(test_client_config()).run()


if __name__ == '__main__':
    unittest.main()
