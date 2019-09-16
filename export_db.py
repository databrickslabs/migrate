from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta
from os import makedirs, path
from datetime import datetime


# python 3.6

def main():
    # define a parser to identify what component to import / export
    parser = get_migration_parser()
    # parse the args
    args = parser.parse_args()

    # parse the path location of the Databricks CLI configuration
    login_creds = get_login_credentials(profile=args.profile)

    # cant use netrc credentials because requests module tries to load the creds into http basic auth headers
    # aws demo by default
    is_aws = (not args.azure)
    # parse the credentials
    url = login_creds['host']
    token = login_creds['token']
    if is_aws:
        export_dir = 'logs/'
    else:
        export_dir = 'azure_logs/'

    makedirs('logs', exist_ok=True)
    makedirs('artifacts', exist_ok=True)

    now = str(datetime.now())
    if args.workspace:
        print("Export the complete workspace at {0}".format(now))
        ws_c = WorkspaceClient(token, url, export_dir)
        start = timer()
        # log notebooks and libraries
        ws_c.log_all_workspace_items()
        end = timer()
        print("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))

    if args.download:
        print("Starting complete workspace download at {0}".format(now))
        ws_c = WorkspaceClient(token, url, export_dir)
        start = timer()
        # log notebooks and libraries
        ws_c.download_notebooks()
        end = timer()
        print("Complete Workspace Download Time: " + str(timedelta(seconds=end - start)))

    if args.libs:
        print("Starting complete library log at {0}".format(now))
        lib_c = LibraryClient(token, url, export_dir)
        start = timer()
        lib_c.log_library_details()
        end = timer()
        print("Complete Library Download Time: " + str(timedelta(seconds=end - start)))

    if args.users:
        print("Export all users and groups at {0}".format(now))
        ws_c = ScimClient(token, url, export_dir)
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
    if args.clusters:
        print("Export the cluster configs at {0}".format(now))
        cl_c = ClustersClient(token, url, export_dir)
        start = timer()
        # log the cluster json
        cl_c.log_cluster_configs()
        end = timer()
        print("Complete Cluster Export Time: " + str(timedelta(seconds=end - start)))
        # log the instance profiles
        if is_aws:
            start = timer()
            cl_c.log_instance_profiles()
            end = timer()
            print("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))
        # log the instance pools
        start = timer()
        cl_c.log_instance_pools()
        end = timer()
        print("Complete Instance Pools Export Time: " + str(timedelta(seconds=end - start)))

    if args.jobs:
        print("Export the jobs configs at {0}".format(now))
        start = timer()
        jobs_c = JobsClient(token, url, export_dir)
        # log job configs
        jobs_c.log_job_configs()
        end = timer()
        print("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))

    if args.metastore:
        print("Export the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(token, url, export_dir)
        # log job configs
        hive_c.export_hive_metastore(is_aws)
        end = timer()
        print("Complete Metastore Export Time: " + str(timedelta(seconds=end - start)))


if __name__ == '__main__':
    main()
