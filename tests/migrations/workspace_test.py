import mock
import pytest

from databricks_migrate.migrations import WorkspaceMigrations


def generate_mocked_workspace_client(work_dir="testdir",
                                     is_aws=False,
                                     skip_failed=False,
                                     verify_ssl=False):
    api_client_mock = mock.MagicMock()
    api_client_v1_2_mock = mock.MagicMock()
    wc = WorkspaceMigrations(api_client_mock, api_client_v1_2_mock, work_dir, is_aws, skip_failed, verify_ssl)
    return api_client_mock, api_client_v1_2_mock, wc


def assert_perform_query_ntimes(mock, n: int):
    assert mock.perform_query.call_count == n, f"Expected perform query function call to be executed {n} times."


def test_workspace():
    list_resp = {
                  "objects": [
                    {
                      "path": "/Users/user@example.com/project",
                      "object_type": "DIRECTORY",
                      "object_id": 123
                    },
                    {
                      "path": "/Users/user@example.com/PythonExampleNotebook",
                      "language": "PYTHON",
                      "object_type": "NOTEBOOK",
                      "object_id": 456
                    }
                  ]
                }
    api_client_mock, api_client_v1_2_mock, wc = generate_mocked_workspace_client()
    api_client_mock.perform_query.return_value = list_resp
    assert wc.get_current_users() == 2
    assert_perform_query_ntimes(api_client_mock, 1)
    assert_perform_query_ntimes(api_client_v1_2_mock, 0)
