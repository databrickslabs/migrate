import logging

import json

import validate
from pipeline import AbstractTask
from dbclient import *
from validate import *
from timeit import default_timer as timer
from datetime import timedelta
import wmconstants


class InstanceProfileExportTask(AbstractTask):
    """Task that exports instance profiles."""
    def __init__(self, client_config, skip=False):
        super().__init__("export_instance_profiles", wmconstants.WM_EXPORT, wmconstants.INSTANCE_PROFILE_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        if scim_c.is_aws():
            cl_c = ClustersClient(self.client_config)
            cl_c.log_instance_profiles()


class UserExportTask(AbstractTask):
    """Task that exports users."""

    def __init__(self, client_config, skip=False):
        super().__init__("export_users", wmconstants.WM_EXPORT, wmconstants.USER_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.log_all_users()


class GroupExportTask(AbstractTask):
    """Task that exports groups."""

    def __init__(self, client_config, skip=False):
        super().__init__("export_groups", wmconstants.WM_EXPORT, wmconstants.GROUP_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.log_all_groups()


class InstanceProfileImportTask(AbstractTask):
    """Task that imports instance profiles."""
    def __init__(self, client_config, skip=False):
        super().__init__("import_instance_profiles", wmconstants.WM_IMPORT, wmconstants.INSTANCE_PROFILE_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        if self.client_config['is_aws']:
            cl_c = ClustersClient(self.client_config)
            cl_c.import_instance_profiles()


class UserImportTask(AbstractTask):
    """Task that imports users."""

    def __init__(self, client_config, skip=False):
        super().__init__("import_users", wmconstants.WM_IMPORT, wmconstants.USER_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.import_all_users()


class GroupImportTask(AbstractTask):
    """Task that imports groups."""

    def __init__(self, client_config, skip=False):
        super().__init__("import_groups", wmconstants.WM_IMPORT, wmconstants.GROUP_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.import_all_groups()

class WorkspaceItemLogExportTask(AbstractTask):
    """Task that log all workspace items to download them at a later time.

    The behavior is equivalent to `$ python export_db.py --workspace`, which lives in main function of
    export_db.py.
    """
 
    def __init__(self, client_config, checkpoint_service, skip=False):
        super().__init__("export_workspace_items_log", wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_PATH_OBJECT, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service

    def run(self):
        ws_c = WorkspaceClient(self.client_config, self.checkpoint_service)
        # log notebooks and libraries
        ws_c.init_workspace_logfiles()
        num_notebooks = ws_c.log_all_workspace_items()
        print("Total number of notebooks logged: ", num_notebooks)


class WorkspaceACLExportTask(AbstractTask):
    """Task that exports ACLs of all notebooks and directories.

    The behavior is equivalent to `$ python export_db.py --workspace-acls`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, checkpoint_service, skip=False):
        super().__init__("export_workspace_acls", wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service

    def run(self):
        ws_c = WorkspaceClient(self.client_config, self.checkpoint_service)
        # log notebooks and directory acls
        ws_c.log_all_workspace_acls()


class NotebookExportTask(AbstractTask):
    """Task that download all notebooks.

    The behavior is equivalent to `$ python export_db.py --download`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, checkpoint_service, skip=False):
        super().__init__("export_notebooks", wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service

    def run(self):
        ws_c = WorkspaceClient(self.client_config, self.checkpoint_service)
        num_notebooks = ws_c.download_notebooks()
        print(f"Total number of notebooks downloaded: {num_notebooks}")


class WorkspaceACLImportTask(AbstractTask):
    """Task that import ACLs of all notebooks and directories.

    The behavior is equivalent to `$ python import_db.py --workspace-acls`, which lives in main function of
    import_db.py.
    """
    def __init__(self, client_config, checkpoint_service, skip=False):
        super().__init__("import_workspace_acls", wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service

    def run(self):
        ws_c = WorkspaceClient(self.client_config, self.checkpoint_service)
        ws_c.import_workspace_acls()


class NotebookImportTask(AbstractTask):
    """Task that imports all notebooks.

    The behavior is equivalent to `$ python import_db.py --workspace`, which lives in main function of
    import_db.py.
    """
    def __init__(self, client_config, checkpoint_service, args, skip=False):
        super().__init__("import_notebooks", wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service
        self.args = args

    def run(self):
        ws_c = WorkspaceClient(self.client_config, self.checkpoint_service)
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError(
                    'Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        ws_c.import_all_workspace_items(archive_missing=self.args.archive_missing)


class ClustersExportTask(AbstractTask):
    """Task that exports all clusters."""
    def __init__(self, client_config, args, skip=False):
        super().__init__("export_clusters", wmconstants.WM_EXPORT, wmconstants.CLUSTER_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        # log the cluster json
        cl_c.log_cluster_configs()
        cl_c.log_cluster_policies()


class InstancePoolsExportTask(AbstractTask):
    """Task that exports all instance pools."""
    def __init__(self, client_config, args, skip=False):
        super().__init__("export_instance_pools", wmconstants.WM_EXPORT, wmconstants.INSTANCE_POOL_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        cl_c.log_instance_pools()


class ClustersImportTask(AbstractTask):
    """Task that imports all clusters."""
    def __init__(self, client_config, args, skip=False):
        super().__init__("import_clusters", wmconstants.WM_IMPORT, wmconstants.CLUSTER_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        cl_c.import_cluster_policies()
        cl_c.import_cluster_configs()


class InstancePoolsImportTask(AbstractTask):
    """Task that imports all instance pools."""
    def __init__(self, client_config, args, skip=False):
        super().__init__("import_instance_pools", wmconstants.WM_IMPORT, wmconstants.INSTANCE_POOL_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        cl_c.import_instance_pools()


class JobsExportTask(AbstractTask):
    """Task that exports all jobs.

    The behavior is equivalent to `$ python export_db.py --jobs`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, args, skip=False):
        super().__init__("export_jobs", wmconstants.WM_EXPORT, wmconstants.JOB_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        jobs_c = JobsClient(self.client_config)
        jobs_c.log_job_configs()


class JobsImportTask(AbstractTask):
    """Task that imports all jobs.

    The behavior is equivalent to `$ python import_db.py --jobs`, which lives in main function of
    import_db.py.
    """
    def __init__(self, client_config, args, skip=False):
        super().__init__("import_jobs", wmconstants.WM_IMPORT, wmconstants.JOB_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        jobs_c = JobsClient(self.client_config)
        jobs_c.import_job_configs()


class MetastoreExportTask(AbstractTask):
    """Task that exports all metastore tables.

    The behavior is equivalent to `$ python export_db.py --metastore`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, checkpoint_service, args, skip=False):
        super().__init__("export_metastore", wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service
        self.args = args

    def run(self):
        hive_c = HiveClient(self.client_config, self.checkpoint_service)
        hive_c.export_hive_metastore(cluster_name=self.args.cluster_name,
                                     has_unicode=self.args.metastore_unicode)


class MetastoreImportTask(AbstractTask):
    """Task that imports all metastore tables.

    The behavior is equivalent to `$ python import_db.py --metastore`, which lives in main function of
    import_db.py.
    """
    def __init__(self, client_config, checkpoint_service, args, skip=False):
        super().__init__("import_metastore", wmconstants.WM_IMPORT, wmconstants.METASTORE_TABLES, skip)
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service
        self.args = args

    def run(self):
        hive_c = HiveClient(self.client_config, self.checkpoint_service)
        # log job configs
        hive_c.import_hive_metastore(cluster_name=self.args.cluster_name,
                                     has_unicode=self.args.metastore_unicode,
                                     should_repair_table=self.args.repair_metastore_tables)


class MetastoreTableACLExportTask(AbstractTask):
    """Task that exports all metastore table ACLs.

    The behavior is equivalent to `$ python export_db.py --table-acls`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, args, skip=False):
        super().__init__("export_metastore_table_acls", wmconstants.WM_EXPORT, wmconstants.METASTORE_TABLES_ACL, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        table_acls_c = TableACLsClient(self.client_config)
        notebook_exit_value = table_acls_c.export_table_acls(db_name='')
        if notebook_exit_value['num_errors'] == 0:
            print("Table ACL export completed successfully without errors")
        elif notebook_exit_value['num_errors'] == -1:
            print("Internal Notebook error, while executing  ACL Export")
        else:
            print("Errors while exporting ACLs, some object's ACLs will be skipped  "
                  + "(those objects ACL's will be ignored,they are documented with prefix 'ERROR_!!!'), "
                  + f'notebook output: {json.dumps(notebook_exit_value)}')


class MetastoreTableACLImportTask(AbstractTask):
    """Task that imports all metastore table ACLs.

    The behavior is equivalent to `$ python import_db.py --table-acls`, which lives in main function of
    import_db.py.
    """
    def __init__(self, client_config, args, skip=False):
        super().__init__("import_metastore_table_acls", wmconstants.WM_IMPORT, wmconstants.METASTORE_TABLES_ACL, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        table_acls_c = TableACLsClient(self.client_config)
        table_acls_c.import_table_acls()


class SecretExportTask(AbstractTask):
    """Task that exports secrets and scopes.

    The behavior is equivalent to `$ python export_db.py --secrets --cluster-name $clusterName
    """
    def __init__(self, client_config, args, skip=False):
        super().__init__("export_secrets", wmconstants.WM_EXPORT, wmconstants.SECRET_OBJECT, skip)
        self.client_config = client_config
        self.args = args

    def run(self):
        secrets_c = SecretsClient(self.client_config)
        secrets_c.log_all_secrets(cluster_name=self.args.cluster_name)
        secrets_c.log_all_secrets_acls()


class SecretImportTask(AbstractTask):
    """Task that imports secrets and scopes.

    The behavior is equivalent to `$ python import_db.py --secrets`
    """
    def __init__(self, client_config, skip=False):
        super().__init__("import_secrets", wmconstants.WM_IMPORT, wmconstants.SECRET_OBJECT, skip)
        self.client_config = client_config

    def run(self):
        secrets_c = SecretsClient(self.client_config)
        secrets_c.import_all_secrets()


class FinishExportTask(AbstractTask):
    """
    Final tasks to finish export. This task will print out necessary information to be used for import pipeline.
    Todo: Add some validation logic to ensure export finished successfully.
    """
    def __init__(self, client_config, skip=False):
        super().__init__("finish_export", "", "", skip)
        self.client_config = client_config

    def run(self):
        print(f"Export finished successfully. Session Id: {self.client_config['session']}")


def read_json_file(file):
    with open(file, 'r') as f:
        lines = sorted(f.readlines())
    return [json.loads(line) for line in lines]


class DiffTask(AbstractTask):
    def __init__(self, name, source, destination, config=None):
        super().__init__(name)
        self.source = source
        self.destination = destination
        self.config = config

    def run(self):
        source_data = validate.prepare_diff_input(read_json_file(self.source), self.config)
        destination_data = validate.prepare_diff_input(read_json_file(self.destination),
                                                       self.config)
        diff = diff_json(source_data, destination_data)
        print_diff(diff)
