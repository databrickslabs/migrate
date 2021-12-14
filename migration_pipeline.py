import os.path
from datetime import datetime
from pipeline import Pipeline
from tasks import *
import wmconstants
from checkpoint_service import CheckpointService


def generate_session(args) -> str:
    if args.validate_pipeline:
        prefix = 'V'
    elif args.export_pipeline:
        prefix = 'E'
    else:
        prefix = 'I'

    return prefix + datetime.now().strftime('%Y%m%d%H%M%S')


def build_pipeline(args) -> Pipeline:
    """Build the pipeline based on the command line arguments."""
    # Resume session if specified, and create a new one otherwise. Different session will work in
    # different export_dir in order to be isolated.
    session = args.session if args.session else generate_session(args)
    print(f"Using the session id: {session}")

    if args.validate_pipeline:
        client_config = parser.build_client_config_without_profile(args)
    else:
        login_args = parser.get_login_credentials(profile=args.profile)
        if parser.is_azure_creds(login_args) and (not args.azure):
            raise ValueError(
                'Login credentials do not match args. Please provide --azure flag for azure envs.')

        # Cant use netrc credentials because requests module tries to load the credentials into http
        # basic auth headers parse the credentials
        url = login_args['host']
        token = login_args['token']
        client_config = parser.build_client_config(args.profile, url, token, args)

    client_config['session'] = session

    # Need to keep the export_dir as base_dir to find exported data from source and destination.
    client_config['base_dir'] = client_config['export_dir']
    client_config['export_dir'] = os.path.join(client_config['base_dir'], session) + '/'

    if client_config['debug']:
        logging.info(url, token)

    if not args.dry_run:
        os.makedirs(client_config['export_dir'], exist_ok=True)

    checkpoint_service = CheckpointService(client_config)
    if args.export_pipeline:
        return build_export_pipeline(client_config, checkpoint_service, args)

    if args.import_pipeline:
        return build_import_pipeline(client_config, checkpoint_service, args)

    if args.validate_pipeline:
        return build_validate_pipeline(client_config, checkpoint_service, args)


def build_export_pipeline(client_config, checkpoint_service, args) -> Pipeline:
    """
    All export jobs
    export_instance_profiles -> export_users -> export_groups -> export_secrets -> export_clusters -> export_instance_pools -> export_jobs
                                                              -> log_workspace_items -> export_workspace_acls
                                                              -> export_notebooks
                                                              -> export_metastore -> export_metastore_table_acls
    """
    skip_tasks = args.skip_tasks

    completed_pipeline_steps = checkpoint_service.get_checkpoint_key_set(
        wmconstants.WM_EXPORT, wmconstants.MIGRATION_PIPELINE_OBJECT_TYPE)
    pipeline = Pipeline(client_config['export_dir'], completed_pipeline_steps, args.dry_run)
    export_instance_profiles = pipeline.add_task(InstanceProfileExportTask(client_config, wmconstants.INSTANCE_PROFILES in skip_tasks))
    export_users = pipeline.add_task(UserExportTask(client_config, wmconstants.USERS in skip_tasks), [export_instance_profiles])
    export_groups = pipeline.add_task(GroupExportTask(client_config, wmconstants.GROUPS in skip_tasks), [export_users])
    workspace_item_log_export = pipeline.add_task(WorkspaceItemLogExportTask(client_config, checkpoint_service, wmconstants.WORKSPACE_ITEM_LOG in skip_tasks), [export_groups])
    export_workspace_acls = pipeline.add_task(WorkspaceACLExportTask(client_config, checkpoint_service, wmconstants.WORKSPACE_ACLS in skip_tasks), [workspace_item_log_export])
    export_notebooks = pipeline.add_task(NotebookExportTask(client_config, checkpoint_service, wmconstants.NOTEBOOKS in skip_tasks), [workspace_item_log_export])
    export_secrets = pipeline.add_task(SecretExportTask(client_config, args, wmconstants.SECRETS in skip_tasks), [export_groups])
    export_clusters = pipeline.add_task(ClustersExportTask(client_config, args, wmconstants.CLUSTERS in skip_tasks), [export_secrets])
    export_instance_pools = pipeline.add_task(InstancePoolsExportTask(client_config, args, wmconstants.INSTANCE_POOLS in skip_tasks), [export_clusters])
    export_jobs = pipeline.add_task(JobsExportTask(client_config, args, wmconstants.JOBS in skip_tasks), [export_instance_pools])
    export_metastore = pipeline.add_task(MetastoreExportTask(client_config, checkpoint_service, args, wmconstants.METASTORE in skip_tasks), [export_groups])
    export_metastore_table_acls = pipeline.add_task(MetastoreTableACLExportTask(client_config, args, wmconstants.METASTORE_TABLE_ACLS in skip_tasks), [export_metastore])
    # FinishExport task is never skipped
    finish_export = pipeline.add_task(FinishExportTask(client_config),
                                      [export_workspace_acls, export_notebooks, export_jobs,
                                       export_metastore_table_acls])

    return pipeline


def build_import_pipeline(client_config, checkpoint_service, args) -> Pipeline:
    """
    All import jobs
    import_instance_profiles -> import_users -> import_groups -> import_secrets -> import_clusters -> import_instance_pools -> import_jobs
                                                              -> log_workspace_items -> import_notebooks -> import_workspace_acls
                                                              -> import_metastore -> import_metastore_table_acls
    """
    skip_tasks = args.skip_tasks

    completed_pipeline_steps = checkpoint_service.get_checkpoint_key_set(
        wmconstants.WM_IMPORT, wmconstants.MIGRATION_PIPELINE_OBJECT_TYPE)
    pipeline = Pipeline(client_config['export_dir'], completed_pipeline_steps, args.dry_run)
    import_instance_profiles = pipeline.add_task(InstanceProfileImportTask(client_config, wmconstants.INSTANCE_PROFILES in skip_tasks))
    import_users = pipeline.add_task(UserImportTask(client_config, wmconstants.USERS in skip_tasks), [import_instance_profiles])
    import_groups = pipeline.add_task(GroupImportTask(client_config, wmconstants.GROUPS in skip_tasks), [import_users])
    import_notebooks = pipeline.add_task(NotebookImportTask(client_config, checkpoint_service, args, wmconstants.NOTEBOOKS in skip_tasks), [import_groups])
    import_workspace_acls = pipeline.add_task(WorkspaceACLImportTask(client_config, checkpoint_service, wmconstants.WORKSPACE_ACLS in skip_tasks), [import_notebooks])
    import_secrets = pipeline.add_task(SecretImportTask(client_config, wmconstants.SECRETS in skip_tasks), [import_groups])
    import_clusters = pipeline.add_task(ClustersImportTask(client_config, args, wmconstants.CLUSTERS in skip_tasks), [import_secrets])
    import_instance_pools = pipeline.add_task(InstancePoolsImportTask(client_config, args, wmconstants.INSTANCE_POOLS in skip_tasks), [import_clusters])
    import_jobs = pipeline.add_task(JobsImportTask(client_config, args, wmconstants.JOBS in skip_tasks), [import_instance_pools])
    import_metastore = pipeline.add_task(MetastoreImportTask(client_config, checkpoint_service, args, wmconstants.METASTORE in skip_tasks), [import_groups])
    import_metastore_table_acls = pipeline.add_task(MetastoreTableACLImportTask(client_config, args, wmconstants.METASTORE_TABLE_ACLS in skip_tasks), [import_metastore])
    return pipeline


def build_validate_pipeline(client_config, checkpoint_service, args):
    completed_pipeline_steps = checkpoint_service.get_checkpoint_key_set(
        wmconstants.WM_VALIDATE, wmconstants.MIGRATION_PIPELINE_OBJECT_TYPE)

    base_dir = client_config['base_dir']
    source_dir = os.path.join(base_dir, args.validate_source_session) + '/'
    destination_dir = os.path.join(base_dir, args.validate_destination_session) + '/'

    init_diff_logger(client_config['export_dir'])
    pipeline = Pipeline(client_config['export_dir'], completed_pipeline_steps, args.dry_run)

    def add_diff_task(name, file_path, config, parents=None):
        source_file = os.path.join(source_dir, file_path)
        destination_file = os.path.join(destination_dir, file_path)
        return pipeline.add_task(DiffTask(name, source_file, destination_file, config), parents)

    def add_dir_diff_task(name, dir_path, config, parents=None):
        source = os.path.join(source_dir, dir_path)
        destination = os.path.join(destination_dir, dir_path)
        return pipeline.add_task(DirDiffTask(name, source, destination, config), parents)

    # InstanceProfileExportTask
    add_diff_task(
        "validate-instance_profile", "instance_profiles.log",
        DiffConfig(primary_key='instance_profile_arn'),
    )
    # UserExportTask
    add_diff_task(
        "validate-users", "users.log",
        DiffConfig(
            primary_key='userName',
            ignored_keys={'id'},
            children={
                "emails": DiffConfig(
                    primary_key="value",
                ),
                "roles": DiffConfig(
                    primary_key="value",
                ),
                "groups": DiffConfig(
                    primary_key="display",
                    ignored_keys={'value', '$ref'}
                ),
                "entitlements": DiffConfig(
                    primary_key="value",
                ),
            }),
    )
    # GroupExportTask
    add_dir_diff_task("validate-groups", "groups", DiffConfig(
        primary_key='displayName',
        ignored_keys={'id'},
        children={
            "members": DiffConfig(
                primary_key="display",
                ignored_keys={"value", "$ref"}
            ),
            "roles": DiffConfig(
                primary_key="value",
            ),
            "groups": DiffConfig(
                primary_key="display",
                ignored_keys={'value', '$ref'}
            ),
            "entitlements": DiffConfig(
                primary_key="value",
            ),
        }))
    # WorkspaceItemLogExportTask
    workspace_item_config = DiffConfig(primary_key='path', ignored_keys={'object_id'})
    add_diff_task("validate-user_dirs", "user_dirs.log", workspace_item_config)
    add_diff_task("validate-user_workspace", "user_workspace.log", workspace_item_config)
    add_diff_task("validate-libraries", "libraries.log", workspace_item_config)

    # WorkspaceACLExportTask
    acl_config = DiffConfig(
        primary_key='path',
        ignored_keys={'object_id'},
        children={
            "access_control_list": DiffConfig(
                primary_key=["user_name", "group_name"],
                children={
                    "all_permissions": DiffConfig(
                        primary_key="__HASH__",
                        ignored_keys={'inherited_from_object'}
                    )
                }
            )
        }
    )
    add_diff_task("validate-acl_notebooks", "acl_notebooks.log", acl_config)
    add_diff_task("validate-acl_directories", "acl_directories.log", acl_config)

    return pipeline


def main():
    logging.basicConfig(format="%(asctime)s;%(levelname)s;%(message)s", datefmt='%Y-%m-%d,%H:%M:%S',
                        level=logging.INFO)

    args = parser.get_pipeline_parser().parse_args()
    if os.name == 'nt' and (not args.bypass_windows_check):
        raise ValueError('This tool currently does not support running on Windows OS')

    pipeline = build_pipeline(args)
    pipeline.run()


if __name__ == '__main__':
    main()
