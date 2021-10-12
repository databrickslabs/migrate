import os.path
import logging
from datetime import timedelta, datetime
from timeit import default_timer as timer
from dbclient import *
from pipeline import AbstractTask, Pipeline


class UserExportTask(AbstractTask):
    """Task that exports users and groups, which is equals to export_db.py --users."""

    def __init__(self, client_config):
        super().__init__(name="export_users_and_groups")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
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
            cl_c = ClustersClient(self.client_config)
            print("Start instance profile logging ...")
            start = timer()
            cl_c.log_instance_profiles()
            end = timer()
            print("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))


class UserImportTask(AbstractTask):
    """Task that imports users and groups, which is equals to import_db.py --users."""

    def __init__(self, client_config):
        super().__init__(name="import_users_and_groups")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        if self.client_config['is_aws']:
            print("Start import of instance profiles first to ensure they exist...")
            cl_c = ClustersClient(self.client_config)
            start = timer()
            cl_c.import_instance_profiles()
            end = timer()
            print("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        scim_c.import_all_users_and_groups()
        end = timer()
        print("Complete Users and Groups Import Time: " + str(timedelta(seconds=end - start)))


def generate_session() -> str:
    return datetime.now().strftime('%Y%m%d%H%M%S')


def build_pipeline() -> Pipeline:
    """Build the pipeline based on the command line arguments."""
    args = get_pipeline_parser().parse_args()

    login_args = get_login_credentials(profile=args.profile)
    if is_azure_creds(login_args) and (not args.azure):
        raise ValueError(
            'Login credentials do not match args. Please provide --azure flag for azure envs.')

    # Cant use netrc credentials because requests module tries to load the credentials into http
    # basic auth headers parse the credentials
    url = login_args['host']
    token = login_args['token']
    client_config = build_client_config(url, token, args)
    session = args.session if args.session else generate_session()
    client_config['export_dir'] = os.path.join(client_config['export_dir'], session) + '/'

    os.makedirs(client_config['export_dir'], exist_ok=True)

    if client_config['debug']:
        print(url, token)
    pipeline = Pipeline(client_config['export_dir'], args.dry_run)
    export_user = pipeline.add_task(UserExportTask(client_config))
    pipeline.add_task(UserImportTask(client_config), [export_user])
    return pipeline


def main():
    logging.basicConfig(format="%(asctime)s;%(levelname)s;%(message)s", datefmt='%Y-%m-%d,%H:%M:%S',
                        level=logging.INFO)

    if os.name == 'nt' and (not args.bypass_windows_check):
        raise ValueError('This tool currently does not support running on Windows OS')

    pipeline = build_pipeline()
    pipeline.run()


if __name__ == '__main__':
    main()
