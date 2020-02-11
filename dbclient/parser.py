import argparse
from os import path

auth_key = ['host',
            'username',
            'token']


def get_login_credentials(creds_path='~/.databrickscfg', profile='DEFAULT'):
    if profile is None:
        profile = 'DEFAULT'
    if creds_path == '~/.databrickscfg':
        fname = path.expanduser(creds_path)
    elif creds_path[0] == '~':
        # relative path, still need to resolve using the above os api
        fname = path.expanduser(creds_path)
    else:
        fname = creds_path
    hit_profile = False
    cred_dict = {}
    with open(fname, 'r') as fp:
        for line in fp:
            # if line is not empty
            if line:
                # strip chars and clean up the string
                clean_l = line[:-1].rstrip().lstrip()
                if clean_l:
                    # non-empty lines processing below
                    if hit_profile:
                        if clean_l[0] == '[':
                            return cred_dict
                        else:
                            # can't split on = due to community edition code
                            split_index = clean_l.find('=')
                            x = clean_l[:split_index].rstrip()
                            cred_dict[x] = clean_l[split_index + 1:].lstrip()
                    if clean_l[0] == '[':
                        if clean_l[1:-1] == profile:
                            # next couple of lines are the credentials
                            hit_profile = True
                            continue
    if cred_dict:
        return cred_dict
    else:
        raise ValueError('Unable to find credentials to load.')


def get_export_parser():
    # export workspace items
    parser = argparse.ArgumentParser(description='Export user workspace artifacts from Databricks')

    # export all users and groups
    parser.add_argument('--users', action='store_true',
                        help='Download all the users and groups in the workspace')

    # log all user workspace paths
    parser.add_argument('--workspace', action='store_true',
                        help='Log all the notebook paths in the workspace. (metadata only)')

    # download all user workspace notebooks
    parser.add_argument('--download', action='store_true',
                        help='Download all notebooks for the environment')

    # add all lib configs
    parser.add_argument('--libs', action='store_true',
                        help='Log all the libs for the environment')

    # add all clusters configs
    parser.add_argument('--clusters', action='store_true',
                        help='Log all the clusters for the environment')

    # get all job configs
    parser.add_argument('--jobs', action='store_true',
                        help='Log all the job configs for the environment')
    # get all metastore
    parser.add_argument('--metastore', action='store_true',
                        help='Log all the metastore table definitions')

    # get azure logs
    parser.add_argument('--azure', action='store_true',
                        help='Run on Azure. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT', 
                        help='Profile to parse the credentials')

    parser.add_argument('--export-home', action='store',
                        help='User workspace name to export, typically the users email address')

    parser.add_argument('--silent', action='store_true',
                        help='Silent all logging of export operations.')
    return parser


def get_import_parser():
    # import workspace items parser
    parser = argparse.ArgumentParser(description='Import user workspace artifacts into Databricks')

    # import all users and groups
    parser.add_argument('--users', action='store_true',
                        help='Import all the users and groups from the logfile.')

    # import all notebooks
    parser.add_argument('--workspace', action='store_true',
                        help='Import all notebooks from export dir into the workspace.')

    # import all notebooks
    parser.add_argument('--archive-missing', action='store_true',
                        help='Import all missing users into the top level /Archive/ directory.')

    # import all lib configs
    parser.add_argument('--libs', action='store_true',
                        help='Import all the libs from the logfile into the workspace.')

    # import all clusters configs
    parser.add_argument('--clusters', action='store_true',
                        help='Import all the cluster configs for the environment')

    # import all job configs
    parser.add_argument('--jobs', action='store_true',
                        help='Import all job configurations to the environment.')

    # import all metastore
    parser.add_argument('--metastore', action='store_true',
                        help='Import the metastore to the workspace.')

    # get azure logs
    parser.add_argument('--azure', action='store_true',
                        help='Run on Azure. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')
    return parser
