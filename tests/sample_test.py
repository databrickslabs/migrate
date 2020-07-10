import pytest

from databricks_migrate.db_export import export_user_home
from tests.utils import HTTPFixture, assert_api_calls, assert_content


def test_simple():
    assert 1 == 1


@pytest.mark.usefixtures("mock_dir")
def test_export_user_home(mock_dir):
    fake_objects = {
        "objects": [
            {
                "path": "/Users/user@example.com/PythonExampleNotebook",
                "language": "PYTHON",
                "object_type": "NOTEBOOK",
                "object_id": 456
            }
        ]
    }
    fake_download = {
        "content": "Ly8gRGF0YWJyaWNrcyBub3RlYm9vayBzb3VyY2UKMSsx",
    }
    fixture1 = HTTPFixture(uri="/api/2.0/workspace/list",
                           method=HTTPFixture.GET,
                           query_string="path=%2FUsers%2Ftestuser%40databricks.com",
                           response=fake_objects)
    fixture2 = HTTPFixture(uri="/api/2.0/workspace/export",
                           method=HTTPFixture.GET,
                           query_string="path=%2FUsers%2Fuser%40example.com%2FPythonExampleNotebook&format=DBC",
                           response=fake_download)

    print('using temporary directory', mock_dir)


    # check that the request is served
    configs = {'token': "test", 'export_dir': str(mock_dir) + "/", 'is_aws': True,
               'skip_failed': True,
               'verify_ssl': False}

    assert_api_calls([fixture1, fixture2], export_user_home, username="testuser@databricks.com", client_config=configs)

    expected_log = """/Users/user@example.com/PythonExampleNotebook\n"""
    assert_content(mock_dir + "/user_exports/testuser@databricks.com/user_workspace.log", expected_log)
