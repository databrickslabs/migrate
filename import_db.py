from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta, datetime
from os import makedirs
from checkpoint_service import CheckpointService
import logging_utils
import os

# python 3.6
def main():
    # define a parser to identify what component to import / export
    my_parser = get_import_parser()
    # parse the args
    args = my_parser.parse_args()

    # parse the path location of the Databricks CLI configuration
    login_args = get_login_credentials(profile=args.profile)
    if is_azure_creds(login_args) and (not args.azure):
        raise ValueError('Login credentials do not match args. Please provide --azure flag for azure environments.')

    if is_gcp_creds(login_args) and (not args.gcp):
        raise ValueError('Login credentials do not match args. Please provide --gcp flag for gcp environments.')

    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    url = login_args['host']
    token = login_args.get('token', login_args.get('password'))
    client_config = build_client_config(args.profile, url, token, args)
    session = args.session if args.session else ""
    client_config['session'] = session
    client_config['export_dir'] = os.path.join(client_config['export_dir'], session) + '/'

    makedirs(client_config['export_dir'], exist_ok=True)
    logging_utils.set_default_logging(client_config['export_dir'])

    checkpoint_service = CheckpointService(client_config)
    if client_config['debug']:
        print(url)
    now = str(datetime.now())

    if args.users:
        print("Import all users and groups at {0}".format(now))
        scim_c = ScimClient(client_config, checkpoint_service)
        if client_config['is_aws']:
            print("Start import of instance profiles first to ensure they exist...")
            cl_c = ClustersClient(client_config, checkpoint_service)
            start = timer()
            cl_c.import_instance_profiles()
            end = timer()
            print("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        scim_c.import_all_users_and_groups()
        end = timer()
        print("Complete Users and Groups Import Time: " + str(timedelta(seconds=end - start)))

    if args.workspace:
        print("Import the complete workspace at {0}".format(now))
        print("Import on {0}".format(url))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError('Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        # log notebooks and libraries
        ws_c.import_all_workspace_items(archive_missing=args.archive_missing, num_parallel=args.num_parallel)
        end = timer()
        print("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))

    if args.workspace_top_level:
        print("Import the top level workspace items at {0}".format(now))
        print("Import on {0}".format(url))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError('Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        # log notebooks and libraries
        ws_c.import_current_workspace_items()
        end = timer()
        print("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))

    if args.workspace_acls:
        print("Import workspace ACLs at {0}".format(now))
        print("Import on {0}".format(url))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and libraries
        # Workspace Acl Import cannot handle parallel APIs due to the heavy loads.
        ws_c.import_workspace_acls(num_parallel=1)
        end = timer()
        print("Complete Workspace acl Import Time: " + str(timedelta(seconds=end - start)))

    if args.clusters:
        print("Import all cluster configs at {0}".format(now))
        cl_c = ClustersClient(client_config, checkpoint_service)
        if client_config['is_aws']:
            print("Start import of instance profiles ...")
            start = timer()
            cl_c.import_instance_profiles()
            end = timer()
            print("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
        print("Start import of cluster policies ...")
        start = timer()
        cl_c.import_cluster_policies()
        end = timer()
        print("Complete Cluster Policies Creation Time: " + str(timedelta(seconds=end - start)))
        print("Start import of instance pool configurations ...")
        start = timer()
        cl_c.import_instance_pools()
        end = timer()
        print("Complete Instance Pools Creation Time: " + str(timedelta(seconds=end - start)))
        print("Start import of cluster configurations ...")
        start = timer()
        cl_c.import_cluster_configs()
        end = timer()
        print("Complete Cluster Import Time: " + str(timedelta(seconds=end - start)))

    if args.jobs:
        print("Importing the jobs configs at {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config, checkpoint_service)
        jobs_c.import_job_configs()
        end = timer()
        print("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))

    if args.metastore or args.metastore_unicode:
        print("Importing the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(client_config, checkpoint_service)
        # log job configs
        hive_c.import_hive_metastore(cluster_name=args.cluster_name, has_unicode=args.metastore_unicode,
                                    should_repair_table=args.repair_metastore_tables)
        end = timer()
        print("Complete Metastore Import Time: " + str(timedelta(seconds=end - start)))

    if args.repair_metastore_tables:
        print("Repairing metastore table")
        start = timer()
        hive_c = HiveClient(client_config, checkpoint_service)
        hive_c.repair_legacy_tables(cluster_name=args.cluster_name)
        end = timer()
        print("Complete Metastore Repair Time: " + str(timedelta(seconds=end - start)))

    if args.table_acls:
        print("Importing table acls configs at {0}".format(now))
        start = timer()
        table_acls_c = TableACLsClient(client_config, checkpoint_service)
        # log table ACLS configs
        notebook_exit_value = table_acls_c.import_table_acls()
        end = timer()
        print(f'Complete Table ACLs with exit value: {json.dumps(notebook_exit_value)}, Import Time: {timedelta(seconds=end - start)}')

    if args.secrets:
        print("Import secret scopes configs at {0}".format(now))
        start = timer()
        sc = SecretsClient(client_config, checkpoint_service)
        sc.import_all_secrets()
        end = timer()
        print("Complete Secrets Import Time: " + str(timedelta(seconds=end - start)))

    if args.pause_all_jobs:
        print("Pause all current jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config, checkpoint_service)
        # log job configs
        jobs_c.pause_all_jobs()
        end = timer()
        print("Paused all jobs time: " + str(timedelta(seconds=end - start)))

    if args.unpause_all_jobs:
        print("Unpause all current jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config, checkpoint_service)
        # log job configs
        jobs_c.pause_all_jobs(False)
        end = timer()
        print("Unpaused all jobs time: " + str(timedelta(seconds=end - start)))
    
    if args.import_pause_status:
        print("Importing pause status for migrated jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config, checkpoint_service)
        # log job configs
        jobs_c.import_pause_status()
        end = timer()
        print("Import pause jobs time: " + str(timedelta(seconds=end - start)))


    if args.delete_all_jobs:
        print("Delete all current jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config, checkpoint_service)
        url = jobs_c.get_url()
        response = prompt_for_input(f'\nPlease confirm that you would like to delete jobs from {url} [yes/no]:')
        if response:
            print("Deleting all job configs ... ")
            jobs_c.delete_all_jobs()
        end = timer()
        print("Delete all jobs time: " + str(timedelta(seconds=end - start)))

    if args.single_user:
        user_email = args.single_user
        print(f"Import user {user_email} at {now}")
        scim_c = ScimClient(client_config, checkpoint_service)
        start = timer()
        # log all users
        scim_c.import_single_user(user_email)
        end = timer()
        print("Complete single user import: " + str(timedelta(seconds=end - start)))

    if args.import_home:
        username = args.import_home
        print("Importing home directory: {0}".format(username))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and libraries
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError('Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        ws_c.import_user_home(username, 'user_exports')
        end = timer()
        print("Complete Single User Import Time: " + str(timedelta(seconds=end - start)))

    if args.import_groups:
        print("Importing Groups from logs")
        start = timer()
        scim_c = ScimClient(client_config, checkpoint_service)
        scim_c.import_all_users_and_groups()
        user_names = scim_c.get_users_from_log()
        print('Export users notebooks:', user_names)
        ws_c = WorkspaceClient(client_config, WorkspaceClient)
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError('Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        for username in user_names:
            ws_c.import_user_home(username, 'user_exports')
        jobs_c = JobsClient(client_config, checkpoint_service)
        # this will only import the groups jobs since we're filtering the jobs during the export process
        print('Importing the groups members jobs:')
        jobs_c.import_job_configs()
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

    if args.libs:
        start = timer()
        print("Not supported today")
        end = timer()
        # print("Complete Library Import Time: " + str(timedelta(seconds=end - start)))

    if args.mlflow_experiments:
        print("Importing MLflow experiments.")
        mlflow_c = MLFlowClient(client_config, checkpoint_service)
        mlflow_c.import_mlflow_experiments(num_parallel=args.num_parallel)
        failed_task_log = logging_utils.get_error_log_file(wmconstants.WM_IMPORT, wmconstants.MLFLOW_EXPERIMENT_OBJECT, client_config['export_dir'])
        logging_utils.raise_if_failed_task_file_exists(failed_task_log, "MLflow Runs Import.")

    if args.mlflow_experiments_permissions:
        print("Importing MLflow experiment permissions.")
        mlflow_c = MLFlowClient(client_config, checkpoint_service)
        mlflow_c.import_mlflow_experiments_acls(num_parallel=args.num_parallel)
        failed_task_log = logging_utils.get_error_log_file(wmconstants.WM_IMPORT, wmconstants.MLFLOW_EXPERIMENT_PERMISSION_OBJECT, client_config['export_dir'])
        logging_utils.raise_if_failed_task_file_exists(failed_task_log, "MLflow Experiments Permissions Import.")

    if args.mlflow_runs:
        print("Importing MLflow runs.")
        mlflow_c = MLFlowClient(client_config, checkpoint_service)
        assert args.src_profile is not None, "Import MLflow runs requires --src-profile flag."
        src_login_args = get_login_credentials(profile=args.src_profile)
        src_client_config = build_client_config(args.src_profile, src_login_args['host'], src_login_args.get('token', login_args.get('password')), args)
        mlflow_c.import_mlflow_runs(src_client_config, num_parallel=args.num_parallel)
        failed_task_log = logging_utils.get_error_log_file(wmconstants.WM_IMPORT, wmconstants.MLFLOW_RUN_OBJECT, client_config['export_dir'])
        logging_utils.raise_if_failed_task_file_exists(failed_task_log, "MLflow Runs Import.")


    if args.get_repair_log:
        print("Finding partitioned tables to repair at {0}".format(now))
        start = timer()
        hive_c = HiveClient(client_config, checkpoint_service)
        # log job configs
        hive_c.repair_legacy_tables()
        end = timer()
        print("Complete Report Time: " + str(timedelta(seconds=end - start)))


if __name__ == '__main__':
    print("Note: running import_db.py directly is not recommended. Please use migration_pipeline.py")
    main()
