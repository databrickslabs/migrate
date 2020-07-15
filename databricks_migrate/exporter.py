from os import makedirs

import click
from databricks_cli.configure.config import profile_option, debug_option
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_migrate import CONTEXT_SETTINGS, log, API_VERSION_1_2, API_VERSION_2_0
from databricks_migrate.migrations import WorkspaceMigrations, LibraryMigrations, ScimMigrations, \
    ClusterMigrations, JobsMigrations, HiveMigrations, \
    DbfsMigrations
from databricks_migrate.utils import provide_api_client, log_action


@click.command(context_settings=CONTEXT_SETTINGS, help='Export user workspace artifacts from Databricks')
@log_action("start exporting databricks objects", debug_params=True)
@click.option('--users', is_flag=True, help='Download all the users and groups in the workspace')
@click.option('--workspace', is_flag=True, help='Log all the notebook paths in the workspace. (metadata only)')
@click.option('--download', is_flag=True, help='Download all notebooks for the environment')
@click.option('--libs', is_flag=True, help='Log all the libs for the environment')
@click.option('--clusters', is_flag=True, help='Log all the clusters for the environment')
@click.option('--jobs', is_flag=True, help='Log all the job configs for the environment')
@click.option('--metastore', is_flag=True, help='Log all the metastore table definitions')
@click.option('--database', type=str, help='Database name to export for the metastore. Single database name supported')
@click.option('--iam', type=str, help='IAM Instance Profile to export metastore entires')
@click.option('--mounts', is_flag=True, help='Log all mount points.')
@click.option('--export-home', type=str, help='User workspace name to export, typically the users email address')
@click.option('--skip-failed', is_flag=True, help='Skip retries for any failed exports.')
@click.option('--azure', is_flag=True, help='Run on Azure. (Default is AWS)')
@click.option('--no-ssl-verification', is_flag=True, help='Set Verify=False when making http requests.')
@debug_option
@eat_exceptions
@profile_option
@provide_api_client(api_version=API_VERSION_1_2)
@provide_api_client(api_version=API_VERSION_2_0)
def export_cli(users: bool,
               workspace: bool,
               download: bool,
               libs: bool,
               clusters: bool,
               jobs: bool,
               metastore: bool,
               database: str,
               iam: str,
               mounts,
               export_home: str,
               skip_failed: bool,
               azure: bool,
               no_ssl_verification: bool,
               api_client: ApiClient,
               api_client_v1_2: ApiClient):
    is_aws = not azure
    export_dir = 'logs/' if is_aws else 'azure_logs/'

    ws_c = WorkspaceMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    scim_c = ScimMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    lib_c = LibraryMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    clusters_c = ClusterMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    jobs_c = JobsMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    hive_c = HiveMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    dbfs_c = DbfsMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    makedirs(export_dir, exist_ok=True)

    if export_home is not None:
        export_user_home(export_home, ws_c)

    if workspace:
        export_workspace(ws_c)

    if download:
        execute_download(ws_c)

    if libs:
        # We do not support apis required to export in azure
        if azure:
            log.info("Databricks does not support library exports on Azure today")
        else:
            export_libs(lib_c)

    if users:
        export_users(scim_c)
        export_groups(scim_c)
        # if the workspace is aws export instance profiles
        if is_aws:
            export_instance_profiles(clusters_c)

    if clusters:
        export_clusters(clusters_c)
        export_instance_pools(clusters_c)

    if jobs:
        export_jobs(jobs_c)

    if metastore:
        export_metastore(database, iam, hive_c)

    if mounts:
        export_mounts(dbfs_c)


@log_action("export mounts", debug_params=True)
def export_mounts(dbfs_c):
    dbfs_c.export_dbfs_mounts()


@log_action("export metastore configs", debug_params=True)
def export_metastore(database, iam, hive_c):
    if database is not None:
        # export only a single database with a given iam role
        database_name = database
        hive_c.export_database(database_name, iam)
    else:
        # export all of the metastore
        hive_c.export_hive_metastore()


@log_action("export job configs", debug_params=True)
def export_jobs(jobs_c):
    jobs_c.log_job_configs()


@log_action("export metastore configs", debug_params=True)
def export_clusters(cl_c):
    cl_c.log_cluster_configs()


@log_action("export instance pool configs", debug_params=True)
def export_instance_pools(cl_c):
    cl_c.log_instance_pools()


@log_action("export users", debug_params=True)
def export_users(scim_c):
    scim_c.log_all_users()


@log_action("export groups", debug_params=True)
def export_groups(scim_c):
    scim_c.log_all_groups()


@log_action("export instance profiles", debug_params=True)
def export_instance_profiles(cl_c):
    cl_c.log_instance_profiles()


@log_action("export library configs", debug_params=True)
def export_libs(lib_c):
    lib_c.log_library_details()


@log_action("download workspace objects", debug_params=True)
def execute_download(ws_c):
    ws_c.download_notebooks()


@log_action("export workspace object configs", debug_params=True)
def export_workspace(ws_c):
    ws_c.init_workspace_logfiles()
    ws_c.log_all_workspace_items()


@log_action("export user home objects", debug_params=True)
def export_user_home(username, ws_c):
    ws_c.export_user_home(username, 'user_exports')
