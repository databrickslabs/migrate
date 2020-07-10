from datetime import timedelta, datetime
from os import makedirs
from timeit import default_timer as timer

import click
from databricks_cli.configure.config import debug_option, profile_option
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_migrate import CONTEXT_SETTINGS, log, API_VERSION_1_2, API_VERSION_2_0
from databricks_migrate.migrations import WorkspaceMigrations, \
    ScimMigrations, ClusterMigrations, JobsMigrations, HiveMigrations
from databricks_migrate.utils import provide_api_client


@click.command(context_settings=CONTEXT_SETTINGS, help="Import user workspace artifacts into Databricks")
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
    now = str(datetime.now())

    ws_c = WorkspaceMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    scim_c = ScimMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    clusters_c = ClusterMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    jobs_c = JobsMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    hive_c = HiveMigrations(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, no_ssl_verification)
    makedirs(export_dir, exist_ok=True)

    if workspace:
        import_workspace(now=now, archive_missing=archive_missing, ws_c=ws_c)

    if libs:
        import_libs()

    if users:
        import_users(now=now, is_aws=is_aws, scim_c=scim_c, cl_c=clusters_c)

    if clusters:
        import_clusters(now=now, is_aws=is_aws, cl_c=clusters_c)

    if jobs:
        import_jobs(now=now, jobs_c=jobs_c)

    if metastore:
        import_metastore(now=now, hive_c=hive_c)


def import_metastore(now, hive_c):
    log.info("Importing the metastore configs at {0}".format(now))
    start = timer()
    # log job configs
    hive_c.import_hive_metastore()
    end = timer()
    log.info("Complete Metastore Import Time: " + str(timedelta(seconds=end - start)))


def import_jobs(now, jobs_c):
    log.info("Importing the jobs configs at {0}".format(now))
    start = timer()
    jobs_c.import_job_configs()
    end = timer()
    log.info("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))


def import_clusters(now, is_aws, cl_c):
    log.info("Import the cluster configs at {0}".format(now))
    if is_aws:
        log.info("Start import of instance profiles ...")
        start = timer()
        cl_c.import_instance_profiles()
        end = timer()
        log.info("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
    log.info("Start import of instance pool configurations ...")
    start = timer()
    cl_c.import_instance_pools()
    end = timer()
    log.info("Complete Instance Pools Creation Time: " + str(timedelta(seconds=end - start)))
    log.info("Start import of cluster configurations ...")
    start = timer()
    cl_c.import_cluster_configs()
    end = timer()
    log.info("Complete Cluster Import Time: " + str(timedelta(seconds=end - start)))


def import_users(now, is_aws, scim_c, cl_c):
    log.info("Import all users and groups at {0}".format(now))
    if is_aws:
        log.info("Start import of instance profiles first to ensure they exist...")
        start = timer()
        cl_c.import_instance_profiles()
        end = timer()
        log.info("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
    start = timer()
    scim_c.import_all_users_and_groups()
    end = timer()
    log.info("Complete Users and Groups Import Time: " + str(timedelta(seconds=end - start)))


def import_libs():
    log.info("Importing libraries not yet implemented")
    # ########### TO DO #######################
    # lib_c = LibraryClient(client_config)
    # start = timer()
    # end = timer()
    # log.info("Complete Library Import Time: " + str(timedelta(seconds=end - start)))


def import_workspace(now, archive_missing, ws_c):
    log.info("Import the complete workspace at {0}".format(now))
    start = timer()
    # log notebooks and libraries
    if archive_missing:
        ws_c.import_all_workspace_items(archive_missing=True)
    else:
        ws_c.import_all_workspace_items(archive_missing=False)
    end = timer()
    log.info("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))
