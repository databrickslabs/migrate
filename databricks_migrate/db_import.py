from datetime import timedelta, datetime
from os import makedirs
from timeit import default_timer as timer

import click

from databricks_migrate import CONTEXT_SETTINGS, log
from databricks_migrate.dbclient import build_client_config, WorkspaceClient, \
    ScimClient, ClustersClient, JobsClient, HiveClient
from databricks_migrate.utils import get_login_args


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
@click.option('--profile', type=str, help='Profile to parse the credentials')
@click.option('--no-ssl-verification', is_flag=True, help='Set Verify=False when making http requests.')
def import_cli(**kwargs):
    # Client settings
    profile = kwargs["profile"]
    azure = kwargs["azure"]
    skip_failed = kwargs["skip_failed"]
    archive_missing = kwargs["archive_missing"]
    no_ssl_verification = kwargs["no_ssl_verification"]
    # Import flags
    users = kwargs["users"]
    workspace = kwargs["workspace"]
    libs = kwargs["libs"]
    clusters = kwargs["clusters"]
    jobs = kwargs["jobs"]
    metastore = kwargs["metastore"]

    log.debug(f"cli params: {kwargs}")

    login_args = get_login_args(profile=profile, azure=azure)
    url = login_args['host']
    token = login_args['token']

    client_config = build_client_config(url, token, azure, no_ssl_verification, skip_failed)
    makedirs(client_config['export_dir'], exist_ok=True)
    log.debug(f"url: {url} token: {token}")
    now = str(datetime.now())

    if workspace:
        import_workspace(now=now, url=url, client_config=client_config, archive_missing=archive_missing)

    if libs:
        import_libs(client_config)

    if users:
        import_users(now=now, client_config=client_config)

    if users:
        import_users(now=now, client_config=client_config)

    if clusters:
        import_clusters(now=now, client_config=client_config)

    if jobs:
        import_jobs(now=now, client_config=client_config)

    if metastore:
        import_metastore(now=now, client_config=client_config)


def import_metastore(now, client_config):
    log.info("Importing the metastore configs at {0}".format(now))
    start = timer()
    hive_c = HiveClient(client_config)
    # log job configs
    hive_c.import_hive_metastore()
    end = timer()
    log.info("Complete Metastore Import Time: " + str(timedelta(seconds=end - start)))


def import_jobs(now, client_config):
    log.info("Importing the jobs configs at {0}".format(now))
    start = timer()
    jobs_c = JobsClient(client_config)
    jobs_c.import_job_configs()
    end = timer()
    log.info("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))


def import_clusters(now, client_config):
    log.info("Import the cluster configs at {0}".format(now))
    cl_c = ClustersClient(client_config)
    if client_config['is_aws']:
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


def import_users(now, client_config):
    log.info("Import all users and groups at {0}".format(now))
    scim_c = ScimClient(client_config)
    if client_config['is_aws']:
        log.info("Start import of instance profiles first to ensure they exist...")
        cl_c = ClustersClient(client_config)
        start = timer()
        cl_c.import_instance_profiles()
        end = timer()
        log.info("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
    start = timer()
    scim_c.import_all_users_and_groups()
    end = timer()
    log.info("Complete Users and Groups Import Time: " + str(timedelta(seconds=end - start)))


def import_libs(client_config):
    log.info("Importing libraries not yet implemented")
    # ########### TO DO #######################
    # lib_c = LibraryClient(client_config)
    # start = timer()
    # end = timer()
    # log.info("Complete Library Import Time: " + str(timedelta(seconds=end - start)))


def import_workspace(now, url, client_config, archive_missing):
    log.info("Import the complete workspace at {0}".format(now))
    log.info("Import on {0}".format(url))
    ws_c = WorkspaceClient(client_config)
    start = timer()
    # log notebooks and libraries
    if archive_missing:
        ws_c.import_all_workspace_items(archive_missing=True)
    else:
        ws_c.import_all_workspace_items(archive_missing=False)
    end = timer()
    log.info("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))
