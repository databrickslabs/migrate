import json

from databricks_cli.sdk import ApiClient, ManagedLibraryService, ClusterService

from databricks_migrate import log
from databricks_migrate.migrations import BaseMigrationClient


class LibraryMigrations(BaseMigrationClient):

    def __init__(self, api_client: ApiClient, api_client_v1_2: ApiClient, export_dir, is_aws, skip_failed, verify_ssl):

        super().__init__(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, verify_ssl)
        self.library_service = ManagedLibraryService(api_client)
        self.cluster_service = ClusterService(api_client)

    def get_cluster_list(self, alive=True):
        """ Returns an array of json objects for the running clusters. Grab the cluster_name or cluster_id """
        cl = self.cluster_service.list_clusters()
        if alive:
            running = filter(lambda x: x['state'] == "RUNNING", cl['clusters'])
            return list(running)
        else:
            return cl['clusters']

    def log_library_details(self, log_file='lib_details.log'):
        libs_log = self._export_dir + log_file
        all_libs = self.api_client_v1_2.perform_query("GET", "/libraries/list")
        log.debug(all_libs)
        with open(libs_log, "w") as fp:
            for x in all_libs:
                log.debug(f"Processing library: {x}")
                lib_details = self.api_client_v1_2.perform_query("GET",'/libraries/status?libraryId={0}'.format(x['id']))
                log.debug(f"Library Details: {lib_details}")
                fp.write(json.dumps(lib_details) + '\n')
                fp.flush()

    def log_cluster_libs(self, cl_log_file='attached_cluster_libs.log'):
        cl_lib_log = self._export_dir + cl_log_file
        cl = self.get_cluster_list(False)
        with open(cl_lib_log, "w") as fp:
            for x in cl:
                cid = x['cluster_id']
                libs = self.library_service.cluster_status(cluster_id=cid)
                # libs = self.get("/libraries/cluster-status?cluster_id={0}".format(cid))
                fp.write(json.dumps(libs))
                fp.write("\n")
