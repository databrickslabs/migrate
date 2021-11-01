import os.path
import logging
from datetime import datetime
from pipeline import Pipeline
from dbclient import parser
from tasks import *


def generate_session() -> str:
    return datetime.now().strftime('%Y%m%d%H%M%S')


def build_pipeline() -> Pipeline:
    """Build the pipeline based on the command line arguments."""
    args = parser.get_pipeline_parser().parse_args()

    login_args = parser.get_login_credentials(profile=args.profile)
    if parser.is_azure_creds(login_args) and (not args.azure):
        raise ValueError(
            'Login credentials do not match args. Please provide --azure flag for azure envs.')

    # Cant use netrc credentials because requests module tries to load the credentials into http
    # basic auth headers parse the credentials
    url = login_args['host']
    token = login_args['token']
    client_config = parser.build_client_config(url, token, args)

    # Resume session if specified, and create a new one otherwise. Different session will work in
    # different export_dir in order to be isolated.
    session = args.session if args.session else generate_session()
    client_config['export_dir'] = os.path.join(client_config['export_dir'], session) + '/'

    if client_config['debug']:
        logging.info(url, token)

    if not args.dry_run:
        os.makedirs(client_config['export_dir'], exist_ok=True)

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
