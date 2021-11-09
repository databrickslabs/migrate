import logging

from pipeline import AbstractTask
from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta


class UserExportTask(AbstractTask):
    """Task that exports users and groups.

    The behavior is equivalent to `$ python export_db.py --users`, which lives in main function of
    export_db.py.
    """

    def __init__(self, client_config):
        super().__init__(name="export_users_and_groups")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        start = timer()
        # log all users
        scim_c.log_all_users()
        end = timer()
        logging.info("Complete Users Export Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        # log all groups
        scim_c.log_all_groups()
        end = timer()
        logging.info("Complete Group Export Time: " + str(timedelta(seconds=end - start)))
        # log the instance profiles
        if scim_c.is_aws():
            cl_c = ClustersClient(self.client_config)
            logging.info("Start instance profile logging ...")
            start = timer()
            cl_c.log_instance_profiles()
            end = timer()
            logging.info("Complete Instance Profile Export Time: " + str(timedelta(seconds=end - start)))


class UserImportTask(AbstractTask):
    """Task that imports users and groups.py --users.

    The behavior is equivalent to `$ python import_db.py --users`, which lives in main function of
    import_db.py.
    """

    def __init__(self, client_config):
        super().__init__(name="import_users_and_groups")
        self.client_config = client_config

    def run(self):
        scim_c = ScimClient(self.client_config)
        if self.client_config['is_aws']:
            logging.info("Start import of instance profiles first to ensure they exist...")
            cl_c = ClustersClient(self.client_config)
            start = timer()
            cl_c.import_instance_profiles()
            end = timer()
            logging.info("Complete Instance Profile Import Time: " + str(timedelta(seconds=end - start)))
        start = timer()
        scim_c.import_all_users_and_groups()
        end = timer()
        logging.info("Complete Users and Groups Import Time: " + str(timedelta(seconds=end - start)))

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
        start = timer()
        # log notebooks and libraries
        ws_c.init_workspace_logfiles()
        num_notebooks = ws_c.log_all_workspace_items()
        print("Total number of notebooks logged: ", num_notebooks)
        end = timer()
        print("Complete Workspace Export Time: " + str(timedelta(seconds=end - start)))

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
        start = timer()
        # log notebooks and directory acls
        ws_c.log_all_workspace_acls()
        end = timer()
        print("Complete Workspace Permission Export Time: " + str(timedelta(seconds=end - start)))

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
        start = timer()
        num_notebooks = ws_c.download_notebooks()
        print(f"Total number of notebooks downloaded: {num_notebooks}")
        end = timer()
        print("Complete Workspace Download Time: " + str(timedelta(seconds=end - start)))

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
        start = timer()
        ws_c.import_workspace_acls()
        end = timer()
        print("Complete Workspace acl Import Time: " + str(timedelta(seconds=end - start)))

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
        start = timer()
        if ws_c.is_overwrite_notebooks():
            # if OVERWRITE is configured, check that the SOURCE format option is used. Otherwise fail
            if not ws_c.is_source_file_format():
                raise ValueError('Overwrite notebooks only supports the SOURCE format. See Rest API docs for details')
        ws_c.import_all_workspace_items(archive_missing=self.args.archive_missing)
        end = timer()
        print("Complete Workspace Import Time: " + str(timedelta(seconds=end - start)))