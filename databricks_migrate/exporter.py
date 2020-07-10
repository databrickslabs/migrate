from datetime import timedelta, datetime
from os import makedirs
from timeit import default_timer as timer

import click
from databricks_cli.configure.config import profile_option, debug_option
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_migrate import CONTEXT_SETTINGS, log, API_VERSION_1_2, API_VERSION_2_0
from databricks_migrate.migrations import WorkspaceMigrations, LibraryMigrations, ScimMigrations, \
    ClusterMigrations, JobsMigrations, HiveMigrations, \
    DbfsMigrations
from databricks_migrate.utils import provide_api_client


@click.command(context_settings=CONTEXT_SETTINGS, help='Export user workspace artifacts from Databricks')
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
    now = str(datetime.now())

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
        export_workspace(now, ws_c)
    if download:
        execute_download(now, ws_c)
    if libs:
        export_libs(now, azure, lib_c)
    if users:
        export_users_and_groups(now, azure, scim_c, clusters_c)
    if clusters:
        export_clusters(now, clusters_c)
    if jobs:
        export_jobs(now, jobs_c)
    if metastore:
        export_metastore(now, database, iam, hive_c)
    if mounts:
        export_mounts(now, dbfs_c)


def export_mounts(now, dbfs_c):
    log.info("Export the mount configs at {0}".format(now))
    start = timer()

    # log job configs
    dbfs_c.export_dbfs_mounts()
    end = timer()
    log.info("Complete Mounts Export Time: " + str(timedelta(seconds=end - start)))


def export_metastore(now, database, iam, hive_c):
    log.info("Export the metastore configs at {0}".format(now))
    start = timer()

    if database is not None:
        # export only a single database with a given iam role
        database_name = database
        hive_c.export_database(database_name, iam)
    else:
        # export all of the metastore
        hive_c.export_hive_metastore()
    end = timer()
    log.info("Complete Metastore Export Time: " + str(timedelta(seconds=end - start)))


def export_jobs(now, jobs_c):
    log.info("Export the jobs configs at {0}".format(now))
    start = timer()

    # log job configs
    jobs_c.log_job_configs()
    end = timer()
    log.info("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))


def export_clusters(now, cl_c):
    log.info("Export the cluster configs at {0}".format(now))
    start = timer()
    # log the cluster json
    cl_c.log_cluster_configs()
    end = timer()
    log.info("Complete Cluster Export Time: " + str(timedelta(seconds=end - start)))
    # log the instance pools
    log.info("Start instance pool logging ...")
    start = timer()
    cl_c.log_instance_pools()
    end = timer()
    log.info("Complete Instance Pools Export Time: " + str(timedelta(seconds=end - start)))


def export_users_and_groups(now, azure, scim_c, cl_c):
    log.info("Export all users and groups at {0}".format(now))
    # ws_c = ScimClient(client_config)
    start = timer()
    # log all users
    scim_c.log_all_users()
    end = timer()
    log.info("Complete Users Export Time: " + str(timedelta(seconds=end - start)))
    start = timer()
    # log all groups
    scim_c.log_all_groups()
    end = timer()
    log.info("Complete Group Export Time: " + str(timedelta(seconds=end - start)))
    # log the instance profiles
    if not azure:
        # cl_c = ClustersClient(client_config)
        log.info("Start instance profile logging ...")
        start = timer()
        cl_c.log_instance_profiles()
        end = timer()
        log.info("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))


def export_libs(now, azure: bool, lib_c):
    if azure:
        log.info("Databricks does not support library exports on Azure today")
    else:
        log.info("Starting complete library log at {0}".format(now))
        start = timer()
        lib_c.log_library_details()
        end = timer()
        log.info("Complete Library Download Time: " + str(timedelta(seconds=end - start)))


def execute_download(now, ws_c):
    log.info("Starting complete workspace download at {0}".format(now))
    start = timer()
    # log notebooks and libraries
    ws_c.download_notebooks()
    end = timer()
    log.info("Complete Workspace Download Time: " + str(timedelta(seconds=end - start)))


def export_workspace(now, ws_c):
    log.info("Export the complete workspace at {0}".format(now))
    start = timer()
    # log notebooks and libraries
    ws_c.init_workspace_logfiles()
    ws_c.log_all_workspace_items()
    end = timer()
    log.info("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))


def export_user_home(username, ws_c):
    username = username
    log.info("Exporting home directory: {0}".format(username))
    start = timer()
    # log notebooks and libraries
    ws_c.export_user_home(username, 'user_exports')
    end = timer()
    log.info("Complete User Export Time: " + str(timedelta(seconds=end - start)))


