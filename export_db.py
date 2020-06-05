from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta, datetime
from os import makedirs

# python 3.6
def main():
    # define a parser to identify what component to import / export
    my_parser = get_export_parser()
    # parse the args
    args = my_parser.parse_args()

    # parse the path location of the Databricks CLI configuration
    login_args = get_login_credentials(profile=args.profile)
    if is_azure_creds(login_args) and (not args.azure):
        raise ValueError('Login credentials do not match args. Please provide --azure flag for azure envs.')

    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # parse the credentials
    url = login_args['host']
    token = login_args['token']
    client_config = build_client_config(url, token, args)

    makedirs(client_config['export_dir'], exist_ok=True)

    if client_config['debug']:
        print(url, token)
    now = str(datetime.now())

    if args.export_home:
        username = args.export_home
        print("Exporting home directory: {0}".format(username))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.export_user_home(username, 'user_exports')
        end = timer()
        print("Complete User Export Time: " + str(timedelta(seconds=end - start)))

    if args.workspace:
        print("Export the complete workspace at {0}".format(now))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.init_workspace_logfiles()
        ws_c.log_all_workspace_items()
        end = timer()
        print("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))

    if args.download:
        print("Starting complete workspace download at {0}".format(now))
        ws_c = WorkspaceClient(client_config)
        start = timer()
        # log notebooks and libraries
        ws_c.download_notebooks()
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

    if args.users:
        print("Export all users and groups at {0}".format(now))
        ws_c = ScimClient(client_config)
        start = timer()
        # log all users
        ws_c.log_all_users()
        end = timer()
        print("Complete Users Export Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        # log all groups
        ws_c.log_all_groups()
        end = timer()
        print("Complete Group Export Time: " + str(timedelta(seconds=end - start)))
        # log the instance profiles
        if client_config['is_aws']:
            cl_c = ClustersClient(client_config)
            print("Start instance profile logging ...")
            start = timer()
            cl_c.log_instance_profiles()
            end = timer()
            print("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))

    if args.clusters:
        print("Export the cluster configs at {0}".format(now))
        cl_c = ClustersClient(client_config)
        start = timer()
        # log the cluster json
        cl_c.log_cluster_configs()
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

    if args.metastore:
        print("Export the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(client_config)
        if args.database is not None:
            # export only a single database with a given iam role
            database_name = args.database
            hive_c.export_database(database_name, args.iam)
        else:
            # export all of the metastore
            hive_c.export_hive_metastore()
        end = timer()
        print("Complete Metastore Export Time: " + str(timedelta(seconds=end - start)))

    if args.mounts:
        print("Export the mount configs at {0}".format(now))
        start = timer()
        dbfs_c = DbfsClient(client_config)
        # log job configs
        dbfs_c.export_dbfs_mounts()
        end = timer()
        print("Complete Mounts Export Time: " + str(timedelta(seconds=end - start)))

if __name__ == '__main__':
    main()
