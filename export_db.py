from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta, datetime
import os
import shutil
from checkpoint_service import CheckpointService
import logging_utils

# python 3.6
def main():
    # define a parser to identify what component to import / export
    my_parser = get_export_parser()
    # parse the args
    args = my_parser.parse_args()

    if os.name == 'nt' and (not args.bypass_windows_check):
        raise ValueError('This tool currently does not support running on Windows OS')

    # parse the path location of the Databricks CLI configuration
    login_args = get_login_credentials(profile=args.profile)
    if is_azure_creds(login_args) and (not args.azure) :
        raise ValueError('Login credentials do not match args. Please provide --azure flag for azure envs.')

    if is_gcp_creds(login_args) and (not args.gcp) :
        raise ValueError('Login credentials do not match args. Please provide --gcp flag for gcp envs.')

    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # parse the credentials
    url = login_args['host']
    token = login_args.get('token', login_args.get('password'))
    client_config = build_client_config(args.profile, url, token, args)
    session = args.session if args.session else ""
    client_config['session'] = session
    client_config['export_dir'] = os.path.join(client_config['export_dir'], session) + '/'

    os.makedirs(client_config['export_dir'], exist_ok=True)
    logging_utils.set_default_logging(client_config['export_dir'])

    checkpoint_service = CheckpointService(client_config)

    if client_config['debug']:
        print(url)
    now = str(datetime.now())

    if args.users:
        print("Export all users and groups at {0}".format(now))
        scim_c = ScimClient(client_config, checkpoint_service)
        start = timer()
        # log all users
        scim_c.log_all_users()
        end = timer()
        print("Complete Users Export Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        # log all groups
        scim_c.log_all_groups()
        end = timer()
        print("Complete Group Export Time: " + str(timedelta(seconds=end - start)))
        # log the instance profiles
        if scim_c.is_aws():
            cl_c = ClustersClient(client_config, checkpoint_service)
            print("Start instance profile logging ...")
            start = timer()
            cl_c.log_instance_profiles()
            end = timer()
            print("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))

    if args.workspace:
        print("Export the complete workspace at {0}".format(now))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and libraries
        ws_c.init_workspace_logfiles()
        num_notebooks = ws_c.log_all_workspace_items_entry(exclude_prefixes=args.exclude_work_item_prefixes)
        print("Total number of notebooks logged: ", num_notebooks)
        end = timer()
        print("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))

    if args.workspace_acls:
        print("Export the ACLs for workspace objects at {0}".format(now))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and directory acls
        ws_c.log_all_workspace_acls(num_parallel=args.num_parallel)
        end = timer()
        print("Complete Workspace Permission Export Time: " + str(timedelta(seconds=end - start)))

    if args.download:
        print("Starting complete workspace download at {0}".format(now))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and libraries
        num_notebooks = ws_c.download_notebooks(num_parallel=args.num_parallel)
        print(f"Total number of notebooks downloaded: {num_notebooks}")
        end = timer()
        print("Complete Workspace Download Time: " + str(timedelta(seconds=end - start)))

    if args.libs:
        if not client_config['is_aws']:
            print("Databricks does not support library exports on Azure or GCP today")
        else:
            print("Starting complete library log at {0}".format(now))
            lib_c = LibraryClient(client_config)
            start = timer()
            lib_c.log_library_details()
            lib_c.log_cluster_libs()
            end = timer()
            print("Complete Library Download Time: " + str(timedelta(seconds=end - start)))

    if args.clusters:
        print("Export the cluster configs at {0}".format(now))
        cl_c = ClustersClient(client_config, checkpoint_service)
        start = timer()
        # log the cluster json
        cl_c.log_cluster_configs()
        cl_c.log_cluster_policies()
        end = timer()
        print("Complete Cluster Export Time: " + str(timedelta(seconds=end - start)))
        # log the instance pools
        print("Start instance pool logging ...")
        start = timer()
        cl_c.log_instance_pools()
        end = timer()
        print("Complete Instance Pools Export Time: " + str(timedelta(seconds=end - start)))

    if args.jobs:
        print("Export the jobs configs at {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config, checkpoint_service)
        # log job configs
        jobs_c.log_job_configs()
        end = timer()
        print("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))

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

    if args.metastore or args.metastore_unicode:
        print("Export the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(client_config, checkpoint_service)
        if args.database is not None:
            # export only a single database with a given iam role
            database_name = args.database
            hive_c.export_database(database_name, args.cluster_name, args.iam, has_unicode=args.metastore_unicode)
        else:
            # export all of the metastore
            hive_c.export_hive_metastore(cluster_name=args.cluster_name, has_unicode=args.metastore_unicode)
        end = timer()
        print("Complete Metastore Export Time: " + str(timedelta(seconds=end - start)))

    if args.table_acls:
        print("Export the table ACLs configs at {0}".format(now))
        start = timer()
        table_acls_c = TableACLsClient(client_config, checkpoint_service)
        if args.database is not None:
            # export table ACLs only for a single database
            notebook_exit_value = table_acls_c.export_table_acls(db_name=args.database)
        else:
            # export table ACLs only for all databases
            notebook_exit_value= table_acls_c.export_table_acls(db_name='')
        end = timer()
        if notebook_exit_value['num_errors'] == 0:
            print("Complete Table ACL Export Time: " + str(timedelta(seconds=end - start)))
        elif notebook_exit_value['num_errors'] == -1:
            print("Internal Notebook error, while executing  ACL Export , Time: " + str(timedelta(seconds=end - start)))
        else:
            print("Errors while exporting ACLs, some object's ACLs will be skipped  "
                  + "(those objects ACL's will be ignored,they are documented with prefix 'ERROR_!!!'), "
                  + f'notebook output: {json.dumps(notebook_exit_value)}, Table ACL Export Time: '
                  + str(timedelta(seconds=end - start)))

    if args.secrets:
        if not args.cluster_name:
            print("Please provide an existing cluster name w/ --cluster-name option\n")
            return
        print("Export the secret scopes configs at {0}".format(now))
        start = timer()
        sc = SecretsClient(client_config, checkpoint_service)
        # log job configs
        sc.log_all_secrets(args.cluster_name)
        sc.log_all_secrets_acls()
        end = timer()
        print("Complete Secrets Export Time: " + str(timedelta(seconds=end - start)))

    if args.mounts:
        print("Export the mount configs at {0}".format(now))
        start = timer()
        dbfs_c = DbfsClient(client_config, checkpoint_service)
        # log job configs
        dbfs_c.export_dbfs_mounts()
        end = timer()
        print("Complete Mounts Export Time: " + str(timedelta(seconds=end - start)))

    if args.update_account_id and args.old_account_id:
        print("Updating old account id to new account at {0}".format(now))
        start = timer()
        client = dbclient(client_config)
        client.update_account_id(args.update_account_id, args.old_account_id)
        end = timer()
        print("Complete account id update time: " + str(timedelta(seconds=end - start)))

    if args.replace_old_email and args.update_new_email:
        print("Updating old email to new email address at {0}".format(now))
        start = timer()
        client = dbclient(client_config)
        client.update_email_addresses(args.replace_old_email, args.update_new_email)
        end = timer()
        print("Complete email update time: " + str(timedelta(seconds=end - start)))

    if args.replace_email:
        print("Updating old email(s) to new email(s)) at {0}".format(now))
        start = timer()
        client = dbclient(client_config)
        #parse list list of e-mail mapping pairs. Format is:  old1@email.com:new1@e-mail.com,old2email.com:new2@email.com
        emailpairs = args.replace_email.split(',')
        print(str(len(emailpairs)) +' emails found to replace')
        for emailpair in emailpairs:
            if len(emailpair.split(':')) < 2:
                print('Syntax error in e-mail '+emailpair+'. Old e-mail address and new e-mail address new to be separated by a :')
            else:
                old_email=emailpair.split(':')[0]
                new_email=emailpair.split(':')[1]
                print('Replacing old e-mail: '+old_email+' with new e-mail '+new_email)
                client.update_email_addresses(old_email, new_email)
        end = timer()
        print("Complete email update time: " + str(timedelta(seconds=end - start)))

    if args.single_user:
        user_email = args.single_user
        print(f"Export user {user_email} at {now}")
        scim_c = ScimClient(client_config, checkpoint_service)
        start = timer()
        # log all users
        scim_c.log_single_user(user_email)
        end = timer()
        print("Complete single user export: " + str(timedelta(seconds=end - start)))

    if args.workspace_top_level_only:
        print("Export top level workspace objects at {0}".format(now))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and directory acls
        ws_c.export_top_level_folders()
        end = timer()
        print("Complete Workspace Top Level Notebooks Export Time: " + str(timedelta(seconds=end - start)))

    if args.export_home:
        username = args.export_home
        print("Exporting home directory: {0}".format(username))
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        start = timer()
        # log notebooks and libraries
        ws_c.export_user_home(username, 'user_exports', num_parallel=args.num_parallel)
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

    if args.export_groups:
        group_name_list = convert_args_to_list(args.export_groups)
        print("Exporting Groups: {0}".format(group_name_list))
        start = timer()
        scim_c = ScimClient(client_config, checkpoint_service)
        # log notebooks and libraries
        user_names = scim_c.log_groups_from_list(group_name_list)
        print('Export users notebooks:', user_names)
        ws_c = WorkspaceClient(client_config, checkpoint_service)
        for username in user_names:
            is_user_home_empty = ws_c.is_user_home_empty(username)
            if not is_user_home_empty:
                ws_c.export_user_home(username, 'user_exports', num_parallel=args.num_parallel)
        print('Exporting users jobs:')
        jobs_c = JobsClient(client_config, checkpoint_service)
        jobs_c.log_job_configs(users_list=user_names)
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

    if args.mlflow_experiments:
        print("Exporting MLflow experiments.")
        mlflow_c = MLFlowClient(client_config, checkpoint_service)
        mlflow_c.export_mlflow_experiments()
        failed_task_log = logging_utils.get_error_log_file(wmconstants.WM_EXPORT, wmconstants.MLFLOW_EXPERIMENT_OBJECT, client_config['export_dir'])
        logging_utils.raise_if_failed_task_file_exists(failed_task_log, "MLflow Experiments Export.")

    if args.mlflow_experiments_permissions:
        print("Importing MLflow experiment permissions.")
        mlflow_c = MLFlowClient(client_config, checkpoint_service)
        mlflow_c.export_mlflow_experiments_acls()
        failed_task_log = logging_utils.get_error_log_file(wmconstants.WM_EXPORT, wmconstants.MLFLOW_EXPERIMENT_PERMISSION_OBJECT, client_config['export_dir'])
        logging_utils.raise_if_failed_task_file_exists(failed_task_log, "MLflow Experiments Permissions Export.")

    if args.mlflow_runs:
        print("Exporting MLflow runs.")
        mlflow_c = MLFlowClient(client_config, checkpoint_service)
        mlflow_c.export_mlflow_runs(args.start_date, num_parallel=args.num_parallel)
        failed_task_log = logging_utils.get_error_log_file(wmconstants.WM_EXPORT, wmconstants.MLFLOW_RUN_OBJECT, client_config['export_dir'])
        logging_utils.raise_if_failed_task_file_exists(failed_task_log, "MLflow Runs Export.")


    if args.reset_exports:
        print('Request to clean up old export directory')
        start = timer()
        client = dbclient(client_config)
        export_dir = client.get_export_dir()
        response = prompt_for_input(f'\nPlease confirm that you would like to delete all the logs from {export_dir}'
                                    f' [yes/no]:')
        if response:
            print('Deleting old export directory and logs ...')
            try:
                shutil.rmtree(export_dir)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))
        end = timer()
        print("Completed cleanup: " + str(timedelta(seconds=end - start)))


if __name__ == '__main__':
    print("Note: running export_db.py directly is not recommended. Please use migration_pipeline.py")
    main()
