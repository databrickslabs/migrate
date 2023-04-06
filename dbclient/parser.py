import argparse
from datetime import datetime, timedelta
import configparser
import wmconstants
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


class ValidateSkipTasks(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        valid_tasks = wmconstants.TASK_OBJECTS
        for task in values:
            if task not in valid_tasks:
                raise ValueError(f"invalid task {task}. Skipped tasks must come from {valid_tasks}.")
        setattr(args, self.dest, values)


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "not a valid date: {0!r}. It must be in YYYY-MM-DD".format(s)
        raise argparse.ArgumentTypeError(msg)


def is_azure_creds(creds):
    if 'azuredatabricks.net' in creds.get('host', ''):
        return True
    return False


def is_gcp_creds(creds):
    if 'gcp.databricks.com' in creds.get('host', ''):
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
        if not current_profile:
            raise ValueError(f"Unable to find a defined profile to run this tool. Profile \'{profile}\' not found.")
        return current_profile
    except KeyError:
        raise ValueError(
            'Unable to find credentials to load for profile. Profile only supports tokens.')


def get_export_preparation_parser():
    # update exported Azure files for compatibility with AWS
    parser = argparse.ArgumentParser(description='Update exported Azure files for compatibility with AWS')

    parser.add_argument('--set-export-dir', action='store', required=True,
                        help='Set the base directory to export artifacts')

    parser.add_argument('--session', action='store', default='', required=True,
                        help='If set, the script resumes from latest checkpoint of given session; '
                             'Otherwise, pipeline starts from beginning and creates a new session.')

    parser.add_argument('--replace-email', action='store',
                        help='Update old emails with new e-mails. Format old1@email:new1@email.com,old2@email.com:new2@email.com')

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

    # get all mlflow experiments
    parser.add_argument('--mlflow-experiments', action='store_true',
                        help='log all the mlflow experiments')

    # get all mlflow experiments permissions
    parser.add_argument('--mlflow-experiments-permissions', action='store_true',
                        help='log all the mlflow experiments permissions')

    # get all mlflow runs
    parser.add_argument('--mlflow-runs', action='store_true',
                        help='log all the mlflow runs')

    # get all metastore
    parser.add_argument('--metastore-unicode', action='store_true',
                        help='log all the metastore table definitions including unicode characters')

    parser.add_argument('--session', action='store', default='',
                        help='If set, the script resumes from latest checkpoint of given session; '
                             'Otherwise, pipeline starts from beginning and creates a new session.')

    # get all table ACLs (TODO need to make sure that unicode database object names are supported)
    parser.add_argument('--table-acls', action='store_true',
                        help='log all table ACL grant and deny statements')

    # cluster name used to export the metastore
    parser.add_argument('--cluster-name', action='store',
                        help='Cluster name to export the metastore to a specific cluster. Cluster will be started.')

    # get database to export for metastore and table ACLs
    parser.add_argument('--database', action='store',
                        help='Database name to export for the metastore and table ACLs. Single database name supported')

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

    parser.add_argument('--gcp', action='store_true', default=False,
                        help='Run on GCP. (Default is AWS)')
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

    parser.add_argument('--replace-email', action='store',
                        help='Update old emails with new e-mails. NOTE: Similar to replace-old-email but capable of using multiple e-mails. Format old1@email:new1@email.com,old2@email.com:new2@email.com')

    parser.add_argument('--bypass-windows-check', action='store_true',
                        help='By-pass windows os checker')

    parser.add_argument('--use-checkpoint', action='store_true',
                        help='use checkpointing to restart from previous state')

    parser.add_argument('--num-parallel', type=int, default=4, help='Number of parallel threads to use to '
                                                                          'export/import')

    parser.add_argument('--retry-total', type=int, default=3, help='Total number or retries when making calls to Databricks API')

    parser.add_argument('--retry-backoff', type=float, default=1.0, help='Backoff factor to apply between retry attempts when making calls to Databricks API')

    parser.add_argument('--start-date', action='store', default=None,
                        help='start-date format: YYYY-MM-DD. If not provided, defaults to past 30 days. Currently, only used for exporting ML runs objects.',
                        type=valid_date)

    parser.add_argument('--exclude-work-item-prefixes', nargs='+', type=str, default=[],
                        help='List of prefixes to skip export for log_all_workspace_items')
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

    # skip previous successful imports
    parser.add_argument('--restart-from-checkpoint', action='store_true',
                        help='Restart the workspace import and skip previously successful imports. '
                             'Only works with --workspace option')

    parser.add_argument('--workspace-top-level', action='store_true',
                        help='Import all top level notebooks from export dir into the workspace. Excluding Users dirs')

    parser.add_argument('--workspace-acls', action='store_true',
                        help='Permissions for workspace objects to import')

    parser.add_argument('--overwrite-notebooks', action='store_true', default=False,
                        help='Flag to overwrite notebooks to forcefully overwrite during notebook imports')

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

    parser.add_argument('--session', action='store', default='',
                        help='If set, the script resumes from latest checkpoint of given session; '
                             'Otherwise, pipeline starts from beginning and creates a new session.')

    # import all table acls
    parser.add_argument('--table-acls', action='store_true',
                        help='Import table acls to the workspace.')

    parser.add_argument('--get-repair-log', action='store_true',
                        help='Report on current tables requiring repairs')

    parser.add_argument('--repair-metastore-tables', action='store_true', default=False,
                        help='Repair legacy metastore tables')

    # cluster name used to import the metastore
    parser.add_argument('--cluster-name', action='store',
                        help='Cluster name to import the metastore to a specific cluster. Cluster will be started.')
    # skip failures
    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip missing users that do not exist when importing user notebooks')

    # import all secret scopes
    parser.add_argument('--secrets', action='store_true',
                        help='Import all secret scopes')

    # import all mlflow experiments
    parser.add_argument('--mlflow-experiments', action='store_true',
                        help='Import all the mlflow experiments')

    # import all mlflow experiments permissions
    parser.add_argument('--mlflow-experiments-permissions', action='store_true',
                        help='Import all the mlflow experiments permissions')

    # import all mlflow runs
    parser.add_argument('--mlflow-runs', action='store_true',
                        help='Import all the mlflow runs')

    # get azure logs
    parser.add_argument('--azure', action='store_true',
                        help='Run on Azure. (Default is AWS)')

    parser.add_argument('--gcp', action='store_true',
                        help='Run on GCP. (Default is AWS)')
    #
    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')

    # Source workspace's profile. Necessary for importing mlflow runs objects
    parser.add_argument('--src-profile', action='store', default=None,
                        help='Source Profile to parse the credentials')

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

    parser.add_argument('--import-pause-status', action='store_true',
                        help='Imports pause status for migrated jobs')

    parser.add_argument('--delete-all-jobs', action='store_true',
                        help='Delete all jobs')

    parser.add_argument('--use-checkpoint', action='store_true',
                        help='use checkpointing to restart from previous state')

    parser.add_argument('--num-parallel', type=int, default=4, help='Number of parallel threads to use to '
                                                                          'export/import')

    parser.add_argument('--retry-total', type=int, default=3, help='Total number or retries when making calls to Databricks API')

    parser.add_argument('--retry-backoff', type=float, default=1.0, help='Backoff factor to apply between retry attempts when making calls to Databricks API')

    return parser


def prompt_for_input(message):
    import sys
    # raw_input returns the empty string for "enter", therefore default is no
    yes = {'yes', 'y', 'ye'}
    no = {'no', 'n', ''}

    choice = input(message + '\n').lower()
    if choice in yes:
        return True
    elif choice in no:
        return False
    else:
        sys.stdout.write("Please respond with 'yes' or 'no'")


def build_client_config_without_profile(args):
    return build_client_config('', '', '', args)


def build_client_config(profile, url, token, args):
    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # aws is the default
    config = {'profile': profile,
              'url': url,
              'token': token,
              'is_aws': (not args.azure and not args.gcp),
              'is_azure': (args.azure),
              'is_gcp': (args.gcp),
              'verbose': (not args.silent),
              'verify_ssl': (not args.no_ssl_verification),
              'skip_failed': args.skip_failed,
              'debug': args.debug,
              'file_format': str(args.notebook_format)
              }
    # this option only exists during imports so we check for existence
    if 'overwrite_notebooks' in args:
        config['overwrite_notebooks'] = args.overwrite_notebooks
    else:
        config['overwrite_notebooks'] = False
    if args.set_export_dir:
        if args.set_export_dir.rstrip()[-1] != '/':
            config['export_dir'] = args.set_export_dir + '/'
        else:
            config['export_dir'] = args.set_export_dir
    elif config['is_aws']:
        config['export_dir'] = 'logs/'
    elif config['is_azure']:
        config['export_dir'] = 'azure_logs/'
    else:
        config['export_dir'] = 'gcp_logs/'

    config['use_checkpoint'] = args.use_checkpoint
    config['num_parallel'] = args.num_parallel
    config['retry_total'] = args.retry_total
    config['retry_backoff'] = args.retry_backoff
    return config


def get_pipeline_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Export user(s) workspace artifacts from Databricks')

    parser.add_argument('--profile', action='store', default='DEFAULT',
                        help='Profile to parse the credentials')

    parser.add_argument('--azure', action='store_true', default=False,
                        help='Run on Azure. (Default is AWS)')

    parser.add_argument('--gcp', action='store_true', default=False,
                        help='Run on GCP. (Default is AWS)')

    parser.add_argument('--silent', action='store_true', default=False,
                        help='Silent all logging of export operations.')

    parser.add_argument('--verbose', action='store_true', default=False,
                        help='Verbose logging')

    parser.add_argument('--no-ssl-verification', action='store_true',
                        help='Set Verify=False when making http requests.')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    parser.add_argument('--no-prompt', action='store_true', default=False,
                        help='Skip interactive prompt to confirm workspace import')

    parser.add_argument('--set-export-dir', action='store',
                        help='Set the base directory to export artifacts')

    parser.add_argument('--cluster-name', action='store', required=False,
                        help='Cluster name to export the metastore to a specific cluster. Cluster will be started.')

    # Workspace arguments
    parser.add_argument('--notebook-format', type=NotebookFormat,
                        choices=list(NotebookFormat), default=NotebookFormat.dbc,
                        help='Choose the file format to download the notebooks (default: DBC)')

    parser.add_argument('--overwrite-notebooks', action='store_true', default=False,
                        help='Flag to overwrite notebooks to forcefully overwrite during notebook imports')

    parser.add_argument('--archive-missing', action='store_true',
                        help='Import all missing users into the top level /Archive/ directory.')

    # Metastore arguments
    parser.add_argument('--repair-metastore-tables', action='store_true', default=False,
                        help='Repair legacy metastore tables')

    parser.add_argument('--metastore-unicode', action='store_true',
                        help='log all the metastore table definitions including unicode characters')

    parser.add_argument('--skip-failed', action='store_true', default=False,
                        help='Skip retries for any failed hive metastore exports.')

    parser.add_argument('--skip-missing-users', action='store_true', default=False,
                        help='Skip missing principles during import.')

    # Pipeline arguments
    parser.add_argument('--session', action='store', default='',
                        help='If set, pipeline resumes from latest checkpoint of given session; '
                             'Otherwise, pipeline starts from beginning and creates a new session.')

    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Dry run the pipeline i.e. will not execute tasks if true.')

    parser.add_argument('--export-pipeline', action='store_true',
                        help='Execute all export tasks.')

    parser.add_argument('--import-pipeline', action='store_true',
                        help='Execute all import tasks.')

    parser.add_argument('--validate-pipeline', action='store_true',
                        help='Validate exported data between source and destination.')

    parser.add_argument('--validate-source-session', action='store', default='',
                        help='Session used by exporting source workspace. Only used for ' +
                             '--validate-pipeline.')

    parser.add_argument('--validate-destination-session', action='store', default='',
                        help='Session used by exporting destination workspace. Only used for ' +
                             '--validate-pipeline.')

    parser.add_argument('--use-checkpoint', action='store_true',
                        help='use checkpointing to restart from previous state')

    parser.add_argument('--skip-tasks', nargs='+', type=str, action=ValidateSkipTasks, default=[],
                        help='List of tasks to skip from the pipeline.')

    parser.add_argument('--keep-tasks', nargs='+', type=str, action=ValidateSkipTasks, default=[],
                        help='List of tasks to run in the pipeline; overrides --skip-tasks.')

    parser.add_argument('--num-parallel', type=int, default=4, help='Number of parallel threads to use to '
                                                                          'export/import')

    parser.add_argument('--retry-total', type=int, default=3, help='Total number or retries when making calls to Databricks API')

    parser.add_argument('--retry-backoff', type=float, default=1.0, help='Backoff factor to apply between retry attempts when making calls to Databricks API')

    parser.add_argument('--start-date', action='store', default=None,
                        help='start-date format: YYYY-MM-DD. If not provided, defaults to past 30 days. Currently, only used for exporting ML runs objects.',
                        type=valid_date)

    parser.add_argument('--exclude-work-item-prefixes', nargs='+', type=str, default=[],
                        help='List of prefixes to skip export for log_all_workspace_items')

    parser.add_argument('--groups-to-keep', nargs='+', type=str, default=[],
                        help='List of groups (and therefore users/notebooks) to keep if specified')

    parser.add_argument('--timeout', type=float, default=300.0,
                        help='Timeout for the calls to Databricks\' REST API, in seconds, defaults to 300.0 --use float e.g. 100.0 to make it bigger')

    return parser
