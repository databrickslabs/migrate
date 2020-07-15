from os import makedirs

import click
from databricks_cli.configure.config import debug_option, profile_option
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_migrate import CONTEXT_SETTINGS, log, API_VERSION_1_2, API_VERSION_2_0
from databricks_migrate.migrations import WorkspaceMigrations, \
    ScimMigrations, ClusterMigrations, JobsMigrations, HiveMigrations
from databricks_migrate.utils import provide_api_client, log_action


@click.command(context_settings=CONTEXT_SETTINGS, help="Import user workspace artifacts into Databricks")
@log_action("start importing databricks objects", debug_params=True)
@click.option('--users', is_flag=True, help='Import all the users and groups from the logfile.')
@click.option('--workspace', is_flag=True, help='Import all notebooks from export dir into the workspace.')
@click.option('--archive-missing', is_flag=True,
              help='Import all missing users into the top level /Archive/ directory.')
@click.option('--libs', is_flag=True, help='Import all the libs from the logfile into the workspace.')
@click.option('--clusters', is_flag=True, help='Import all the cluster configs for the environment')
@click.option('--jobs', is_flag=True, help='Import all job configurations to the environment.')
@click.option('--metastore', is_flag=True, help='Import the metastore to the workspace.')
@click.option('--skip-failed', is_flag=True, help='Skip retries for any failed exports.')
@click.option('--azure', is_flag=True, help='Run on Azure. (Default is AWS)')
@click.option('--no-ssl-verification', is_flag=True, help='Set Verify=False when making http requests.')
@debug_option
@eat_exceptions
@profile_option
@provide_api_client(api_version=API_VERSION_1_2)
@provide_api_client(api_version=API_VERSION_2_0)
def import_cli(users: bool,
               workspace: bool,
               archive_missing: bool,
               libs: bool,
               clusters: bool,
               jobs: bool,
               metastore: bool,
               skip_failed: bool,
               azure: bool,
               no_ssl_verification: bool,
               api_client: ApiClient,
               api_client_v1_2: ApiClient):
    # Client settings
    is_aws = not azure
    export_dir = 'logs/' if is_aws else 'azure_logs/'

    ws_c = WorkspaceMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    scim_c = ScimMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    clusters_c = ClusterMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    jobs_c = JobsMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    hive_c = HiveMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    makedirs(export_dir, exist_ok=True)

    if workspace:
        import_workspace(archive_missing, ws_c)

    if libs:
        import_libs()

    if users:
        # If workspace is aws import instance profiles
        if is_aws:
            import_instance_profiles(clusters_c)
        import_users(scim_c)

    if clusters:
        # If workspace is aws import instance profiles
        if is_aws:
            import_instance_profiles(clusters_c)
        import_instance_pools(clusters_c)
        import_clusters(clusters_c)

    if jobs:
        import_jobs(jobs_c)

    if metastore:
        import_metastore(hive_c)


@log_action("import metastore", debug_params=True)
def import_metastore(hive_c):
    hive_c.import_hive_metastore()


@log_action("import jobs", debug_params=True)
def import_jobs(jobs_c):
    jobs_c.import_job_configs()


@log_action("import instance pools", debug_params=True)
def import_instance_pools(cl_c):
    cl_c.import_instance_pools()


@log_action("import clusters", debug_params=True)
def import_clusters(cl_c):
    cl_c.import_cluster_configs()


@log_action("import instance profiles", debug_params=True)
def import_instance_profiles(cl_c):
    cl_c.import_instance_profiles()


@log_action("import users", debug_params=True)
def import_users(scim_c):
    scim_c.import_all_users_and_groups()


@log_action("import libraries", debug_params=True)
def import_libs():
    log.info("Importing libraries not yet implemented")


@log_action("import all the workspace objects", debug_params=True)
def import_workspace(archive_missing, ws_c):
    if archive_missing:
        ws_c.import_all_workspace_items(archive_missing=True)
    else:
        ws_c.import_all_workspace_items(archive_missing=False)
