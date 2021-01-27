from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta, datetime
from os import makedirs


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

    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    url = login_args['host']
    token = login_args['token']
    client_config = build_client_config(url, token, args)

    makedirs(client_config['export_dir'], exist_ok=True)

    if client_config['debug']:
        print(url, token)
    now = str(datetime.now())

    if args.users:
        print("Import all users and groups at {0}".format(now))
        scim_c = ScimClient(client_config)
        if client_config['is_aws']:
            print("Start import of instance profiles first to ensure they exist...")
            cl_c = ClustersClient(client_config)
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
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.import_all_workspace_items(archive_missing=args.archive_missing)
        end = timer()
        print("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))

    if args.workspace_acls:
        print("Import workspace ACLs at {0}".format(now))
        print("Import on {0}".format(url))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.import_workspace_acls()
        end = timer()
        print("Complete Workspace acl Import Time: " + str(timedelta(seconds=end - start)))

    if args.clusters:
        print("Import all cluster configs at {0}".format(now))
        cl_c = ClustersClient(client_config)
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
        jobs_c = JobsClient(client_config)
        jobs_c.import_job_configs()
        end = timer()
        print("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))

    if args.metastore or args.metastore_unicode:
        print("Importing the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(client_config)
        # log job configs
        hive_c.import_hive_metastore(cluster_name=args.cluster_name, has_unicode=args.metastore_unicode)
        end = timer()
        print("Complete Metastore Import Time: " + str(timedelta(seconds=end - start)))

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

    if args.delete_all_jobs:
        print("Delete all current jobs {0}".format(now))
        start = timer()
        jobs_c = JobsClient(client_config)
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
        scim_c = ScimClient(client_config)
        start = timer()
        # log all users
        scim_c.import_single_user(user_email)
        end = timer()
        print("Complete single user import: " + str(timedelta(seconds=end - start)))

    if args.import_home:
        username = args.import_home
        print("Importing home directory: {0}".format(username))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.import_user_home(username, 'user_exports')
        end = timer()
        print("Complete Single User Import Time: " + str(timedelta(seconds=end - start)))

    if args.import_groups:
        print("Importing Groups from logs")
        start = timer()
        scim_c = ScimClient(client_config)
        scim_c.import_all_users_and_groups()
        user_names = scim_c.get_users_from_log()
        print('Export users notebooks:', user_names)
        ws_c = WorkspaceClient(client_config)
        for username in user_names:
            ws_c.import_user_home(username, 'user_exports')
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

    if args.libs:
        start = timer()
        print("Not supported today")
        end = timer()
        # print("Complete Library Import Time: " + str(timedelta(seconds=end - start)))


if __name__ == '__main__':
    main()
