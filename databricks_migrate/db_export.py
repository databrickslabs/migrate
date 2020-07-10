from datetime import timedelta, datetime
from os import makedirs
from timeit import default_timer as timer

import click

from databricks_migrate import CONTEXT_SETTINGS, log
from databricks_migrate.dbclient import build_client_config, WorkspaceClient, LibraryClient, ScimClient, \
    ClustersClient, JobsClient, HiveClient, \
    DbfsClient
from databricks_migrate.utils import get_login_args


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
@click.option('--profile', type=str, help='Profile to parse the credentials')
@click.option('--no-ssl-verification', is_flag=True, help='Set Verify=False when making http requests.')
def export_cli(**kwargs):
    # This was to avoid creating a long parameter list
    profile = kwargs["profile"]
    azure = kwargs["azure"]
    skip_failed = kwargs["skip_failed"]
    no_ssl_verification = kwargs["no_ssl_verification"]
    export_home = kwargs["export_home"]
    workspace = kwargs["workspace"]
    download = kwargs["download"]
    libs = kwargs["libs"]
    users = kwargs["users"]
    clusters = kwargs["clusters"]
    jobs = kwargs["jobs"]
    metastore = kwargs["metastore"]
    iam = kwargs["iam"]
    database = kwargs["database"]
    mounts = kwargs["mounts"]

    log.debug(f"cli params: {kwargs}")

    login_args = get_login_args(profile=profile, azure=azure)
    url = login_args['host']
    token = login_args['token']
    client_config = build_client_config(url, token, azure, no_ssl_verification, skip_failed)

    makedirs(client_config['export_dir'], exist_ok=True)
    log.debug(f"url: {url} token: {token}")
    now = str(datetime.now())

    if export_home is not None:
        export_user_home(username=export_home, client_config=client_config)

    if workspace:
        export_workspace(now=now, client_config=client_config)

    if download:
        execute_download(now=now, client_config=client_config)

    if libs:
        export_libs(now=now, client_config=client_config)

    if users:
        export_users_and_groups(now=now, client_config=client_config)

    if clusters:
        export_clusters(now=now, client_config=client_config)

    if jobs:
        export_jobs(now=now, client_config=client_config)

    if metastore:
        export_metastore(now=now, database=database, iam=iam, client_config=client_config)

    if mounts:
        export_mounts(now=now, client_config=client_config)


def export_mounts(now, client_config):
    log.info("Export the mount configs at {0}".format(now))
    start = timer()
    dbfs_c = DbfsClient(client_config)
    # log job configs
    dbfs_c.export_dbfs_mounts()
    end = timer()
    log.info("Complete Mounts Export Time: " + str(timedelta(seconds=end - start)))


def export_metastore(now, database, iam, client_config):
    log.info("Export the metastore configs at {0}".format(now))
    start = timer()
    hive_c = HiveClient(client_config)
    if database is not None:
        # export only a single database with a given iam role
        database_name = database
        hive_c.export_database(database_name, iam)
    else:
        # export all of the metastore
        hive_c.export_hive_metastore()
    end = timer()
    log.info("Complete Metastore Export Time: " + str(timedelta(seconds=end - start)))


def export_jobs(now, client_config):
    log.info("Export the jobs configs at {0}".format(now))
    start = timer()
    jobs_c = JobsClient(client_config)
    # log job configs
    jobs_c.log_job_configs()
    end = timer()
    log.info("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))


def export_clusters(now, client_config):
    log.info("Export the cluster configs at {0}".format(now))
    cl_c = ClustersClient(client_config)
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


def export_users_and_groups(now, client_config):
    log.info("Export all users and groups at {0}".format(now))
    ws_c = ScimClient(client_config)
    start = timer()
    # log all users
    ws_c.log_all_users()
    end = timer()
    log.info("Complete Users Export Time: " + str(timedelta(seconds=end - start)))
    start = timer()
    # log all groups
    ws_c.log_all_groups()
    end = timer()
    log.info("Complete Group Export Time: " + str(timedelta(seconds=end - start)))
    # log the instance profiles
    if client_config['is_aws']:
        cl_c = ClustersClient(client_config)
        log.info("Start instance profile logging ...")
        start = timer()
        cl_c.log_instance_profiles()
        end = timer()
        log.info("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))


def export_libs(now, client_config):
    if not client_config['is_aws']:
        log.info("Databricks does not support library exports on Azure today")
    else:
        log.info("Starting complete library log at {0}".format(now))
        lib_c = LibraryClient(client_config)
        start = timer()
        lib_c.log_library_details()
        end = timer()
        log.info("Complete Library Download Time: " + str(timedelta(seconds=end - start)))


def execute_download(now, client_config):
    log.info("Starting complete workspace download at {0}".format(now))
    ws_c = WorkspaceClient(client_config)
    start = timer()
    # log notebooks and libraries
    ws_c.download_notebooks()
    end = timer()
    log.info("Complete Workspace Download Time: " + str(timedelta(seconds=end - start)))


def export_workspace(now, client_config):
    log.info("Export the complete workspace at {0}".format(now))
    ws_c = WorkspaceClient(client_config)
    start = timer()
    # log notebooks and libraries
    ws_c.init_workspace_logfiles()
    ws_c.log_all_workspace_items()
    end = timer()
    log.info("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))


def export_user_home(username, client_config):
    username = username
    log.info("Exporting home directory: {0}".format(username))
    ws_c = WorkspaceClient(client_config)
    start = timer()
    # log notebooks and libraries
    ws_c.export_user_home(username, 'user_exports')
    end = timer()
    log.info("Complete User Export Time: " + str(timedelta(seconds=end - start)))
