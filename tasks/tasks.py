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
