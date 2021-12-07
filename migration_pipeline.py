import os.path
from datetime import datetime
from pipeline import Pipeline
from tasks import *
import wmconstants
from checkpoint_service import CheckpointService

def generate_session() -> str:
    return datetime.now().strftime('%Y%m%d%H%M%S')


def build_pipeline(args) -> Pipeline:
    """Build the pipeline based on the command line arguments."""

    login_args = parser.get_login_credentials(profile=args.profile)
    if parser.is_azure_creds(login_args) and (not args.azure):
        raise ValueError(
            'Login credentials do not match args. Please provide --azure flag for azure envs.')

    # Cant use netrc credentials because requests module tries to load the credentials into http
    # basic auth headers parse the credentials
    url = login_args['host']
    token = login_args['token']
    client_config = parser.build_client_config(args.profile, url, token, args)

    # Resume session if specified, and create a new one otherwise. Different session will work in
    # different export_dir in order to be isolated.
    session = args.session if args.session else generate_session()

    print(f"Using the session id: {session}")

    client_config['session'] = session
    client_config['export_dir'] = os.path.join(client_config['export_dir'], session) + '/'

    if client_config['debug']:
        logging.info(url, token)

    if not args.dry_run:
        os.makedirs(client_config['export_dir'], exist_ok=True)

    checkpoint_service = CheckpointService(client_config)
    if args.export_pipeline:
        return build_export_pipeline(client_config, checkpoint_service, args)

    if args.import_pipeline:
        return build_import_pipeline(client_config, checkpoint_service, args)

    # Verification job
    # TODO: Add verification job at the end


def _add_task_or_skip(pipeline, skip_tasks, task, dependent_tasks):
    """
    :param skip_tasks: tasks to skip
    :param task: task to add to the pipeline
    :param dependent_tasks: a list of tasks

    If the task is in the skip_tasks, then just return the first item of dependent_tasks
    If not, we add the task to the pipeline with the dependent_tasks.
    """
    # filter out None tasks. If the list is empty, then treat the tasks as None
    valid_dependent_tasks = list(filter(lambda x: x is not None, dependent_tasks))
    dependent_tasks_or_none = valid_dependent_tasks if valid_dependent_tasks else None
    if task.task_object_name in skip_tasks:
        return dependent_tasks_or_none[0] if dependent_tasks_or_none else None
    else:
        return pipeline.add_task(task, dependent_tasks_or_none)


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
    export_instance_profiles = _add_task_or_skip(pipeline, skip_tasks, InstanceProfileExportTask(client_config), [])
    export_users = _add_task_or_skip(pipeline, skip_tasks, UserExportTask(client_config), [export_instance_profiles])
    export_groups = _add_task_or_skip(pipeline, skip_tasks, GroupExportTask(client_config), [export_users])
    export_workspace_item_log = _add_task_or_skip(pipeline, skip_tasks, WorkspaceItemLogExportTask(client_config, checkpoint_service), [export_groups])
    export_workspace_acls = _add_task_or_skip(pipeline, skip_tasks, WorkspaceACLExportTask(client_config, checkpoint_service), [export_workspace_item_log])
    export_notebooks = _add_task_or_skip(pipeline, skip_tasks, NotebookExportTask(client_config, checkpoint_service), [export_workspace_item_log])
    export_secrets = _add_task_or_skip(pipeline, skip_tasks, SecretExportTask(client_config, args), [export_groups])
    export_clusters = _add_task_or_skip(pipeline, skip_tasks, ClustersExportTask(client_config, args), [export_secrets])
    export_instance_pools = _add_task_or_skip(pipeline, skip_tasks, InstancePoolsExportTask(client_config, args), [export_clusters])
    export_jobs = _add_task_or_skip(pipeline, skip_tasks, JobsExportTask(client_config, args), [export_instance_pools])
    export_metastore = _add_task_or_skip(pipeline, skip_tasks, MetastoreExportTask(client_config, checkpoint_service, args), [export_groups])
    export_metastore_table_acls = _add_task_or_skip(pipeline, skip_tasks, MetastoreTableACLExportTask(client_config, args), [export_metastore])
    # Cannot skip finish_export task
    finish_export = _add_task_or_skip(pipeline, skip_tasks, FinishExportTask(client_config),
                                      [export_workspace_acls, export_notebooks, export_jobs, export_metastore_table_acls])

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
    import_instance_profiles = _add_task_or_skip(pipeline, skip_tasks, InstanceProfileImportTask(client_config), [])
    import_users = _add_task_or_skip(pipeline, skip_tasks, UserImportTask(client_config), [import_instance_profiles])
    import_groups = _add_task_or_skip(pipeline, skip_tasks, GroupImportTask(client_config), [import_users])
    import_notebooks = _add_task_or_skip(pipeline, skip_tasks, NotebookImportTask(client_config, checkpoint_service, args), [import_groups])
    import_workspace_acls = _add_task_or_skip(pipeline, skip_tasks, WorkspaceACLImportTask(client_config, checkpoint_service), [import_notebooks])
    import_secrets = _add_task_or_skip(pipeline, skip_tasks, SecretImportTask(client_config), [import_groups])
    import_clusters = _add_task_or_skip(pipeline, skip_tasks, ClustersImportTask(client_config, args), [import_secrets])
    import_instance_pools = _add_task_or_skip(pipeline, skip_tasks, InstancePoolsImportTask(client_config, args), [import_clusters])
    import_jobs = _add_task_or_skip(pipeline, skip_tasks, JobsImportTask(client_config, args), [import_instance_pools])
    import_metastore = _add_task_or_skip(pipeline, skip_tasks, MetastoreImportTask(client_config, checkpoint_service, args), [import_groups])
    import_metastore_table_acls = _add_task_or_skip(pipeline, skip_tasks, MetastoreTableACLImportTask(client_config, args), [import_metastore])

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
