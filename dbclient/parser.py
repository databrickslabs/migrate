import argparse
from os import path

auth_key = ['host',
            'username',
            'token']


def is_azure_creds(creds):
    if 'azuredatabricks.net' in creds['host']:
        return True
    return False


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
                clean_l = line.rstrip().lstrip()
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
                        help='log all the metastore table definitions')

    # get database to export for metastore
    parser.add_argument('--database', action='store',
                        help='Database name to export for the metastore. Single database name supported')

    # iam role used to export the metastore
    parser.add_argument('--iam', action='store',
                        help='IAM Instance Profile to export metastore entires')

    # skip failures
    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip retries for any failed exports.')

    # get mount points
    parser.add_argument('--mounts', action='store_true', default=False,
                        help='Log all mount points.')
    # get azure logs
    parser.add_argument('--azure', action='store_true', default= False,
                        help='Run on Azure. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')

    parser.add_argument('--export-home', action='store',
                        help='User workspace name to export, typically the users email address')

    parser.add_argument('--silent', action='store_true', default=False,
                        help='Silent all logging of export operations.')
    # Don't verify ssl
    parser.add_argument('--no-ssl-verification', action='store_true',
                        help='Set Verify=False when making http requests.')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

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

    # skip failures
    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip retries for any failed exports.')

    # get azure logs
    parser.add_argument('--azure', action='store_true',
                        help='Run on Azure. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')
    # Don't verify ssl
    parser.add_argument('--no-ssl-verification', action='store_true',
                        help='Set Verify=False when making http requests.')

    parser.add_argument('--silent', action='store_true',
                        help='Silent all logging of import operations.')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')
    return parser


def build_client_config(url, token, args):
    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # aws is the default
    config = {'url': url,
              'token': token,
              'is_aws': (not args.azure),
              'verbose': (not args.silent),
              'verify_ssl': (not args.no_ssl_verification),
              'skip_failed': args.skip_failed,
              'debug': args.debug
              }
    if config['is_aws']:
        config['export_dir'] = 'logs/'
    else:
        config['export_dir'] = 'azure_logs/'
    return config
