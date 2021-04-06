from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta, datetime
import os
import shutil


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
    if is_azure_creds(login_args) and (not args.azure):
        raise ValueError('Login credentials do not match args. Please provide --azure flag for azure envs.')

    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # parse the credentials
    url = login_args['host']
    token = login_args['token']
    client_config = build_client_config(url, token, args)

    os.makedirs(client_config['export_dir'], exist_ok=True)

    if client_config['debug']:
        print(url, token)
    now = str(datetime.now())

    if args.users:
        print("Export all users and groups at {0}".format(now))
        scim_c = ScimClient(client_config)
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
            cl_c = ClustersClient(client_config)
            print("Start instance profile logging ...")
            start = timer()
            cl_c.log_instance_profiles()
            end = timer()
            print("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))

    if args.workspace:
        print("Export the complete workspace at {0}".format(now))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.init_workspace_logfiles()
        num_notebooks = ws_c.log_all_workspace_items()
        print("Total number of notebooks logged: ", num_notebooks)
        end = timer()
        print("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))

    if args.workspace_acls:
        print("Export the ACLs for workspace objects at {0}".format(now))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and directory acls
        ws_c.log_all_workspace_acls()
        end = timer()
        print("Complete Workspace Permission Export Time: " + str(timedelta(seconds=end - start)))

    if args.download:
        print("Starting complete workspace download at {0}".format(now))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        num_notebooks = ws_c.download_notebooks()
        print(f"Total number of notebooks downloaded: {num_notebooks}")
        end = timer()
        print("Complete Workspace Download Time: " + str(timedelta(seconds=end - start)))

    if args.libs:
        if not client_config['is_aws']:
            print("Databricks does not support library exports on Azure today")
        else:
            print("Starting complete library log at {0}".format(now))
            lib_c = LibraryClient(client_config)
            start = timer()
            lib_c.log_library_details()
            end = timer()
            print("Complete Library Download Time: " + str(timedelta(seconds=end - start)))

    if args.clusters:
        print("Export the cluster configs at {0}".format(now))
        cl_c = ClustersClient(client_config)
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
        jobs_c = JobsClient(client_config)
        # log job configs
        jobs_c.log_job_configs()
        end = timer()
        print("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))

    if args.pause_all_jobs:
        print("Pause all current jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config)
        # log job configs
        jobs_c.pause_all_jobs()
        end = timer()
        print("Paused all jobs time: " + str(timedelta(seconds=end - start)))

    if args.unpause_all_jobs:
        print("Unpause all current jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config)
        # log job configs
        jobs_c.pause_all_jobs(False)
        end = timer()
        print("Unpaused all jobs time: " + str(timedelta(seconds=end - start)))

    if args.metastore or args.metastore_unicode:
        print("Export the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(client_config)
        if args.database is not None:
            # export only a single database with a given iam role
            database_name = args.database
            hive_c.export_database(database_name, args.cluster_name, args.iam, has_unicode=args.metastore_unicode)
        else:
            # export all of the metastore
            hive_c.export_hive_metastore(cluster_name=args.cluster_name, has_unicode=args.metastore_unicode)
        end = timer()
        print("Complete Metastore Export Time: " + str(timedelta(seconds=end - start)))

    if args.secrets:
        if not args.cluster_name:
            print("Please provide an existing cluster name w/ --cluster-name option\n")
            return
        print("Export the secret scopes configs at {0}".format(now))
        start = timer()
        sc = SecretsClient(client_config)
        # log job configs
        sc.log_all_secrets(args.cluster_name)
        sc.log_all_secrets_acls()
        end = timer()
        print("Complete Secrets Export Time: " + str(timedelta(seconds=end - start)))

    if args.mounts:
        print("Export the mount configs at {0}".format(now))
        start = timer()
        dbfs_c = DbfsClient(client_config)
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
        scim_c = ScimClient(client_config)
        start = timer()
        # log all users
        scim_c.log_single_user(user_email)
        end = timer()
        print("Complete single user export: " + str(timedelta(seconds=end - start)))

    if args.workspace_top_level_only:
        print("Export top level workspace objects at {0}".format(now))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and directory acls
        ws_c.export_top_level_folders()
        end = timer()
        print("Complete Workspace Top Level Notebooks Export Time: " + str(timedelta(seconds=end - start)))

    if args.export_home:
        username = args.export_home
        print("Exporting home directory: {0}".format(username))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.export_user_home(username, 'user_exports')
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

    if args.export_groups:
        group_name_list = convert_args_to_list(args.export_groups)
        print("Exporting Groups: {0}".format(group_name_list))
        start = timer()
        scim_c = ScimClient(client_config)
        # log notebooks and libraries
        user_names = scim_c.log_groups_from_list(group_name_list)
        print('Export users notebooks:', user_names)
        ws_c = WorkspaceClient(client_config)
        for username in user_names:
            is_user_home_empty = ws_c.is_user_home_empty(username)
            if not is_user_home_empty:
                ws_c.export_user_home(username, 'user_exports')
        print('Exporting users jobs:')
        jobs_c = JobsClient(client_config)
        jobs_c.log_job_configs(users_list=user_names)
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

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
    main()
