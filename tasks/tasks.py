import logging

from pipeline import AbstractTask
from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta

class InstanceProfileExportTask(AbstractTask):
    """Task that exports instance profiles."""
    def __init__(self, client_config):
        super().__init__(name="export_instance_profiles")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        if scim_c.is_aws():
            cl_c = ClustersClient(self.client_config)
            cl_c.log_instance_profiles()

class UserExportTask(AbstractTask):
    """Task that exports users."""

    def __init__(self, client_config):
        super().__init__(name="export_users")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.log_all_users()

class GroupExportTask(AbstractTask):
    """Task that exports groups."""
    def __init__(self, client_config):
        super().__init__(name="export_groups")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.log_all_groups()

class InstanceProfileImportTask(AbstractTask):
    """Task that imports instance profiles."""
    def __init__(self, client_config):
        super().__init__(name="import_instance_profiles")
        self.client_config = client_config

    def run(self):
        if self.client_config['is_aws']:
            cl_c = ClustersClient(self.client_config)
            cl_c.import_instance_profiles()

class UserImportTask(AbstractTask):
    """Task that imports users."""

    def __init__(self, client_config):
        super().__init__(name="import_users")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.import_all_users()

class GroupImportTask(AbstractTask):
    """Task that imports groups."""

    def __init__(self, client_config):
        super().__init__(name="import_groups")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        scim_c.import_all_groups()

class WorkspaceItemLogTask(AbstractTask):
    """Task that log all workspace items to download them at a later time.

    The behavior is equivalent to `$ python export_db.py --workspace`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, checkpoint_service):
        super().__init__(name="export_workspace_items_log")
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
    def __init__(self, client_config, checkpoint_service):
        super().__init__(name="export_workspace_acls")
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
    def __init__(self, client_config, checkpoint_service):
        super().__init__(name="export_notebooks")
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
    def __init__(self, client_config, checkpoint_service):
        super().__init__(name="import_workspace_acls")
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
    def __init__(self, client_config, checkpoint_service, args):
        super().__init__(name="import_notebooks")
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service
        self.args = args

    def run(self):
        ws_c = WorkspaceClient(self.client_config, self.checkpoint_service)
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError('Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        ws_c.import_all_workspace_items(archive_missing=self.args.archive_missing)

class ClustersExportTask(AbstractTask):
    """Task that exports all clusters."""
    def __init__(self, client_config, args):
        super().__init__(name="export_clusters")
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        # log the cluster json
        cl_c.log_cluster_configs()
        cl_c.log_cluster_policies()


class InstancePoolsExportTask(AbstractTask):
    """Task that exports all instance pools."""
    def __init__(self, client_config, args):
        super().__init__(name="export_instance_pools")
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        cl_c.log_instance_pools()

class ClustersImportTask(AbstractTask):
    """Task that imports all clusters."""
    def __init__(self, client_config, args):
        super().__init__(name="import_clusters")
        self.client_config = client_config
        self.args = args

    def run(self):
        cl_c = ClustersClient(self.client_config)
        cl_c.import_cluster_policies()
        cl_c.import_cluster_configs()

class InstancePoolsImportTask(AbstractTask):
    """Task that imports all instance pools."""
    def __init__(self, client_config, args):
        super().__init__(name="import_instance_pools")
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
    def __init__(self, client_config, args):
        super().__init__(name="export_jobs")
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
    def __init__(self, client_config, args):
        super().__init__(name="import_jobs")
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
    def __init__(self, client_config, checkpoint_service, args):
        super().__init__(name="export_metastore")
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service
        self.args = args

    def run(self):
        hive_c = HiveClient(self.client_config, self.checkpoint_service)
        hive_c.export_hive_metastore(cluster_name=self.args.cluster_name, has_unicode=self.args.metastore_unicode)

class MetastoreImportTask(AbstractTask):
    """Task that imports all metastore tables.

    The behavior is equivalent to `$ python import_db.py --metastore`, which lives in main function of
    import_db.py.
    """
    def __init__(self, client_config, checkpoint_service, args):
        super().__init__(name="import_metastore")
        self.client_config = client_config
        self.checkpoint_service = checkpoint_service
        self.args = args

    def run(self):
        hive_c = HiveClient(self.client_config, self.checkpoint_service)
        # log job configs
        hive_c.import_hive_metastore(cluster_name=self.args.cluster_name, has_unicode=self.args.metastore_unicode,
                                     should_repair_table=self.args.repair_metastore_tables)

class MetastoreTableACLExportTask(AbstractTask):
    """Task that exports all metastore table ACLs.

    The behavior is equivalent to `$ python export_db.py --table-acls`, which lives in main function of
    export_db.py.
    """
    def __init__(self, client_config, args):
        super().__init__(name="export_metastore_table_acls")
        self.client_config = client_config
        self.args = args

    def run(self):
        table_acls_c = TableACLsClient(self.client_config)
        notebook_exit_value= table_acls_c.export_table_acls(db_name='')
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
    def __init__(self, client_config, args):
        super().__init__(name="import_metastore_table_acls")
        self.client_config = client_config
        self.args = args

    def run(self):
        table_acls_c = TableACLsClient(self.client_config)
        table_acls_c.import_table_acls()

class SecretExportTask(AbstractTask):
    """Task that exports secrets and scopes.

    The behavior is equivalent to `$ python export_db.py --secrets --cluster-name $clusterName
    """
    def __init__(self, client_config, args):
        super().__init__(name="export_secrets")
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
    def __init__(self, client_config):
        super().__init__(name="import_secrets")
        self.client_config = client_config

    def run(self):
        secrets_c = SecretsClient(self.client_config)
        secrets_c.import_all_secrets()
