import sys

import click
import requests

from databricks_migrate import CONTEXT_SETTINGS, log
from databricks_migrate.dbclient import get_login_credentials, DBClient, build_client_config


@click.command(context_settings=CONTEXT_SETTINGS, help="Test cli connection")
@click.option('--profile', type=str, help='Profile to parse the credentials')
@click.option('--azure', is_flag=True, help='Run on Azure. (Default is AWS)')
@click.option('--no-ssl-verification', is_flag=True, help='Set Verify=False when making http requests.')
@click.option('--skip-failed', is_flag=True, help='Skip retries for any failed exports.')
def test_connection_cli(profile, azure, no_ssl_verification, skip_failed):
    # parse the path location of the Databricks CLI configuration
    login_creds = get_login_credentials(profile=profile)
    # parse the credentials
    url = login_creds['host']
    token = login_creds['token']
    export_dir = 'logs/'

    log.info("Test connection at {0} with profile {1}\n".format(url, profile))
    client_config = build_client_config(url, token, azure, no_ssl_verification, skip_failed)
    db_client = DBClient(client_config)
    try:
        is_successful = db_client.test_connection()
    except requests.exceptions.RequestException as e:
        log.error(f"Request exception thrown: {str(e)}")
        sys.exit(1)
    if is_successful == 0:
        log.info("Connection successful!")
    else:
        log.info("Unsuccessful connection. Verify credentials.")