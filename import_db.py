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
    # aws demo by default
    is_aws = (not args.azure)
    is_verbose = (not args.silent)
    # parse the credentials
    url = login_args['host']
    token = login_args['token']
    if is_aws:
        export_dir = 'logs/'
    else:
        export_dir = 'azure_logs/'

    makedirs(export_dir, exist_ok=True)

    debug = False
    if debug:
        print(url, token)

    now = str(datetime.now())
    if args.workspace:
        print("Import the complete workspace at {0}".format(now))
        print("Import on {0}".format(url))
        ws_c = WorkspaceClient(token, url, export_dir, is_aws, is_verbose)
        start = timer()
        # log notebooks and libraries
        if args.archive_missing:
            ws_c.import_all_workspace_items(archive_missing=True)
        else:
            ws_c.import_all_workspace_items(archive_missing=False)
        end = timer()
        print("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))

    if args.libs:
        lib_c = LibraryClient(token, url, export_dir)
        start = timer()
        ########### TO DO #######################
        end = timer()
        #print("Complete Library Import Time: " + str(timedelta(seconds=end - start)))

    if args.users:
        print("Import all users and groups at {0}".format(now))
        scim_c = ScimClient(token, url, export_dir, is_aws, is_verbose)
        if is_aws:
            print("Start import of instance profiles first to ensure they exist...")
            cl_c = ClustersClient(token, url, export_dir, is_aws, is_verbose)
            start = timer()
            cl_c.import_instance_profiles()
            end = timer()
            print("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        scim_c.import_all_users_and_groups()
        end = timer()
        print("Complete Users and Groups Import Time: " + str(timedelta(seconds=end - start)))

    if args.clusters:
        print("Import the cluster configs at {0}".format(now))
        cl_c = ClustersClient(token, url, export_dir, is_aws, is_verbose)
        if is_aws:
            print("Start import of instance profiles ...")
            start = timer()
            cl_c.import_instance_profiles()
            end = timer()
            print("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
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
        jobs_c = JobsClient(token, url, export_dir, is_aws, is_verbose)
        jobs_c.import_job_configs()
        end = timer()
        print("Complete Jobs Export Time: " + str(timedelta(seconds=end - start)))

    if args.metastore:
        print("Importing the metastore configs at {0}".format(now))
        start = timer()
        hive_c = HiveClient(token, url, export_dir, is_aws, is_verbose)
        # log job configs
        hive_c.import_hive_metastore()
        end = timer()
        print("Complete Metastore Import Time: " + str(timedelta(seconds=end - start)))


if __name__ == '__main__':
    main()
