import argparse
import configparser
import re
from enum import Enum
from os import path

auth_key = ['host',
            'username',
            'token']


class NotebookFormat(Enum):
    dbc = 'DBC'
    source = 'SOURCE'
    html = 'HTML'
    # jupyter is only supported for python notebooks. consider adding this back if there's demand
    # jupyter = 'JUPYTER'

    def __str__(self):
        return self.value


def is_azure_creds(creds):
    if 'azuredatabricks.net' in creds['host']:
        return True
    return False


def convert_args_to_list(arg_str):
    arg_list = map(lambda x: x.lstrip().rstrip(), arg_str.split(','))
    return list(arg_list)


def get_login_credentials(creds_path='~/.databrickscfg', profile='DEFAULT'):
    config = configparser.ConfigParser()
    abs_creds_path = path.expanduser(creds_path)
    config.read(abs_creds_path)
    try:
        current_profile = dict(config[profile])
        return current_profile
    except KeyError:
        raise ValueError('Unable to find credentials to load for profile. Profile only supports tokens.')


def get_export_user_parser():
    # export workspace items
    parser = argparse.ArgumentParser(description='Export user(s) workspace artifacts from Databricks')

    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')

    parser.add_argument('--azure', action='store_true', default=False,
                        help='Run on Azure. (Default is AWS)')

    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip retries for any failed hive metastore exports.')

    parser.add_argument('--silent', action='store_true', default=False,
                        help='Silent all logging of export operations.')
    # Don't verify ssl
    parser.add_argument('--no-ssl-verification', action='store_true',
                        help='Set Verify=False when making http requests.')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    parser.add_argument('--set-export-dir', action='store',
                        help='Set the base directory to export artifacts')

    parser.add_argument('--users', action='store',
                        help='Download user(s) artifacts such as notebooks, cluster specs, jobs. '
                             'Provide a list of user ids / emails to export')

    return parser


def get_export_parser():
    # export workspace items
    parser = argparse.ArgumentParser(description='Export full workspace artifacts from Databricks')

    # export all users and groups
    parser.add_argument('--users', action='store_true',
                        help='Download all the users and groups in the workspace')

    # log all user workspace paths
    parser.add_argument('--workspace', action='store_true',
                        help='Log all the notebook paths in the workspace. (metadata only)')

    parser.add_argument('--notebook-format', type=NotebookFormat,
                        choices=list(NotebookFormat), default=NotebookFormat.dbc,
                        help='Choose the file format to download the notebooks (default: DBC)')

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

    # get all secret scopes
    parser.add_argument('--secrets', action='store_true',
                        help='log all the secret scopes')

    # get all metastore
    parser.add_argument('--metastore-unicode', action='store_true',
                        help='log all the metastore table definitions including unicode characters')

    # cluster name used to export the metastore
    parser.add_argument('--cluster-name', action='store',
                        help='Cluster name to export the metastore to a specific cluster. Cluster will be started.')

    # get database to export for metastore
    parser.add_argument('--database', action='store',
                        help='Database name to export for the metastore. Single database name supported')

    # iam role used to export the metastore
    parser.add_argument('--iam', action='store',
                        help='IAM Instance Profile to export metastore entires')

    # skip failures
    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip retries for any failed hive metastore exports.')

    # get mount points
    parser.add_argument('--mounts', action='store_true', default=False,
                        help='Log all mount points.')
    # get azure logs
    parser.add_argument('--azure', action='store_true', default=False,
                        help='Run on Azure. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')

    parser.add_argument('--single-user', action='store',
                        help='User\'s email to export their user identity and entitlements')

    parser.add_argument('--export-home', action='store',
                        help='User workspace name to export, typically the users email address')

    parser.add_argument('--export-groups', action='store',
                        help='Group names to export as a set. Includes group, users, and notebooks.')

    parser.add_argument('--workspace-acls', action='store_true',
                        help='Permissions for workspace objects to export')

    parser.add_argument('--workspace-top-level-only', action='store_true',
                        help='Download only top level notebook directories')

    parser.add_argument('--silent', action='store_true', default=False,
                        help='Silent all logging of export operations.')
    # Don't verify ssl
    parser.add_argument('--no-ssl-verification', action='store_true',
                        help='Set Verify=False when making http requests.')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    parser.add_argument('--reset-exports', action='store_true',
                        help='Clear export directory')

    parser.add_argument('--set-export-dir', action='store',
                        help='Set the base directory to export artifacts')

    parser.add_argument('--pause-all-jobs', action='store_true',
                        help='Pause all scheduled jobs')

    parser.add_argument('--unpause-all-jobs', action='store_true',
                        help='Unpause all scheduled jobs')

    parser.add_argument('--update-account-id', action='store',
                        help='Set the account id for instance profiles to a new account id')

    parser.add_argument('--old-account-id', action='store',
                        help='Old account ID to filter on')

    parser.add_argument('--replace-old-email', action='store',
                        help='Old email address to update from logs')

    parser.add_argument('--update-new-email', action='store',
                        help='New email address to replace the logs')

    parser.add_argument('--bypass-windows-check', action='store_true',
                        help='By-pass windows os checker')
    return parser


def get_import_parser():
    # import workspace items parser
    parser = argparse.ArgumentParser(description='Import full workspace artifacts into Databricks')

    # import all users and groups
    parser.add_argument('--users', action='store_true',
                        help='Import all the users and groups from the logfile.')

    # import all notebooks
    parser.add_argument('--workspace', action='store_true',
                        help='Import all notebooks from export dir into the workspace.')

    parser.add_argument('--workspace-top-level', action='store_true',
                        help='Import all top level notebooks from export dir into the workspace. Excluding Users dirs')

    parser.add_argument('--workspace-acls', action='store_true',
                        help='Permissions for workspace objects to import')

    parser.add_argument('--notebook-format', type=NotebookFormat,
                        choices=list(NotebookFormat), default=NotebookFormat.dbc,
                        help='Choose the file format of the notebook to import (default: DBC)')

    parser.add_argument('--import-home', action='store',
                        help='User workspace name to import, typically the users email address')

    parser.add_argument('--import-groups', action='store_true',
                        help='Groups to import into a new workspace. Includes group creation and user notebooks.')

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

    # import all metastore including defns with unicode
    parser.add_argument('--metastore-unicode', action='store_true',
                        help='Import all the metastore table definitions with unicode characters')

    parser.add_argument('--get-repair-log', action='store_true',
                        help='Report on current tables requiring repairs')

    # cluster name used to import the metastore
    parser.add_argument('--cluster-name', action='store',
                        help='Cluster name to import the metastore to a specific cluster. Cluster will be started.')
    # skip failures
    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip missing users that do not exist when importing user notebooks')

    # get azure logs
    parser.add_argument('--azure', action='store_true',
                        help='Run on Azure. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')

    parser.add_argument('--single-user', action='store',
                        help='User\'s email to export their user identity and entitlements')

    # Don't verify ssl
    parser.add_argument('--no-ssl-verification', action='store_true',
                        help='Set Verify=False when making http requests.')

    parser.add_argument('--silent', action='store_true',
                        help='Silent all logging of import operations.')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    parser.add_argument('--set-export-dir', action='store',
                        help='Set the base directory to import artifacts if the export dir was a customized')

    parser.add_argument('--pause-all-jobs', action='store_true',
                        help='Pause all scheduled jobs')

    parser.add_argument('--unpause-all-jobs', action='store_true',
                        help='Unpause all scheduled jobs')

    parser.add_argument('--delete-all-jobs', action='store_true',
                        help='Delete all jobs')
    return parser


def prompt_for_input(message):
    import sys
    # raw_input returns the empty string for "enter", therefore default is no
    yes = {'yes','y', 'ye'}
    no = {'no','n', ''}

    choice = input(message + '\n').lower()
    if choice in yes:
        return True
    elif choice in no:
        return False
    else:
        sys.stdout.write("Please respond with 'yes' or 'no'")


def url_validation(url):
    if '/?o=' in url:
        # if the workspace_id exists, lets remove it from the URL
        new_url = re.sub("\/\?o=.*", '', url)
        return new_url
    elif 'net/' == url[-4:]:
        return url[:-1]
    elif 'com/' == url[-4:]:
        return url[:-1]
    return url


def build_client_config(url, token, args):
    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # aws is the default
    config = {'url': url_validation(url),
              'token': token,
              'is_aws': (not args.azure),
              'verbose': (not args.silent),
              'verify_ssl': (not args.no_ssl_verification),
              'skip_failed': args.skip_failed,
              'debug': args.debug,
              'file_format': str(args.notebook_format)
              }
    if args.set_export_dir:
        if args.set_export_dir.rstrip()[-1] != '/':
            config['export_dir'] = args.set_export_dir + '/'
        else:
            config['export_dir'] = args.set_export_dir
    elif config['is_aws']:
        config['export_dir'] = 'logs/'
    else:
        config['export_dir'] = 'azure_logs/'
    return config
