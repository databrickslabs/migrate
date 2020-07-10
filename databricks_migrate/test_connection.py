import sys

import click
import requests
from databricks_cli.configure.config import debug_option, profile_option
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_migrate import CONTEXT_SETTINGS, log, API_VERSION_2_0, API_VERSION_1_2
from databricks_migrate.migrations import BaseMigrationClient
from databricks_migrate.utils import provide_api_client


@click.command(context_settings=CONTEXT_SETTINGS, help="Test cli connection")
@click.option('--azure', is_flag=True, help='Run on Azure. (Default is AWS)')
@click.option('--no-ssl-verification', is_flag=True, help='Set Verify=False when making http requests.')
@click.option('--skip-failed', is_flag=True, help='Skip retries for any failed exports.')
@debug_option
@eat_exceptions
@profile_option
@provide_api_client(api_version=API_VERSION_2_0)
@provide_api_client(api_version=API_VERSION_1_2)
def test_connection_cli(azure: bool,
                        no_ssl_verification: bool,
                        skip_failed: bool,
                        api_client: ApiClient,
                        api_client_v1_2: ApiClient):
    export_dir = 'azure_logs/' if azure else 'logs/'
    log.info(f"Testing connection at {api_client.url} with headers: {api_client.default_headers}")
    db_client = BaseMigrationClient(api_client, api_client_v1_2, export_dir, not azure, skip_failed, no_ssl_verification)
    try:
        is_successful = db_client.test_connection()
    except requests.exceptions.RequestException as e:
        log.error(f"Request exception thrown: {str(e)}")
        sys.exit(1)
    if is_successful != 0:
        log.info("Connection successful!")
    else:
        log.info("Unsuccessful connection. Verify credentials.")