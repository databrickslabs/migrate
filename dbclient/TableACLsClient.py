from .ClustersClient import *
import base64
import shutil
import wmconstants
import logging_utils
import logging

# noinspection SpellCheckingInspection


class TableACLsClient(ClustersClient):
    """
    Imports and Exports table ACLS to and from a JSON format.

    The actual import and export logic is implemented in two notebooks:
    ../data/notebooks/Export_Table_ACLs.py
    ../data/notebooks/Import_Table_ACLs.py

    Those notebooks need to be executed on a cluster with Table ACLS activated,
    otherwise the commands used for importing and exporting table ACLS :
       SHOW GRANT
    and
        GRANT
    are not available

    Those notebooks can be used standalone as well.

    This class inherits from the HiveClient to use some of the funtionaly
    inside HiveClient - it would be cleaner to refactor HiveClient
    and to pull some of the shared funtionality out.
    """

    REAL_PATH = os.path.dirname(os.path.realpath(__file__))
    EXPORT_TABLE_ACLS_LOCAL_PATH = REAL_PATH + "/../data/notebooks/Export_Table_ACLs.py"
    IMPORT_TABLE_ACLS_LOCAL_PATH = REAL_PATH + "/../data/notebooks/Import_Table_ACLs.py"

    CLUSTER_LAUNCH_POLLING_INTERVAL_SECONDS = 5
    NOTEBOOK_RUN_POLLING_INTERVAL_SECONDS = 2
    BUFFER_SIZE_BYTES = 1024 * 1024  # 1MB limit for dbfs blocks in API
    DB_ADMIN_SUFFIX = "+dbadmin@databricks.com"

    def import_file_to_workspace(self, source_local_path, workspace_path):
        workspace_mkdirs_params = {
            "path": workspace_path[:workspace_path.rindex('/')]
        }
        self.post("/workspace/mkdirs", workspace_mkdirs_params)

        with open(source_local_path, 'rb') as local_file:
            contents = local_file.read()
            workspace_import_params = {
                "content": base64.encodebytes(contents).decode('utf-8'),
                "path": workspace_path,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE"
            }
            self.post("/workspace/import", workspace_import_params)

    def copy_file_to_dbfs(self, source_local_path, target_dbfs_path):
        dbfs_create_params = {
            "path": target_dbfs_path,
            "overwrite": True
        }
        res = self.post("/dbfs/create", dbfs_create_params)
        dbfs_file_handle = res["handle"]
        with open(source_local_path, 'rb') as local_file:

            while True:
                contents = local_file.read(self.BUFFER_SIZE_BYTES)
                if len(contents) == 0:
                    break
                dbfs_add_block_params = {
                    "data": base64.encodebytes(contents).decode('utf-8'),
                    "handle": dbfs_file_handle
                }
                self.post("/dbfs/add-block", dbfs_add_block_params)
        self.post("/dbfs/close", {"handle": dbfs_file_handle})

    def copy_files_to_dbfs_path(self, source_local_path, target_dbfs_path):
        dbfs_mkdirs_params = {
            "path": target_dbfs_path
        }
        self.post("/dbfs/mkdirs", dbfs_mkdirs_params)

        try:
            dirpath, _dirnames, filenames = next(os.walk(source_local_path))
            for filename in filenames:
                local_filepath = os.path.join(dirpath, filename)
                dbfs_filepath = os.path.join(target_dbfs_path, filename)
                self.copy_file_to_dbfs(local_filepath, dbfs_filepath)
        except StopIteration:
            raise Exception(f"not a valid source path: '{source_local_path}'")

    def copy_files_from_dbfs_path(self, source_dbfs_path, target_local_path, target_file_name):
        dbfs_list_params = {
            "path": source_dbfs_path
        }
        file_infos = self.get("/dbfs/list", dbfs_list_params).get('files', None)
        if file_infos:
            # delete all files in target_local_path
            shutil.rmtree(target_local_path, ignore_errors=True)
            os.mkdir(target_local_path)
            file_count=0
            for file_info in file_infos:
                if not file_info['is_dir']:
                    dbfs_path = file_info['path']
                    filename = dbfs_path[dbfs_path.rindex('/') + 1:]
                    if not filename[0] == '_':
                        length = file_info['file_size']
                        offset = 0
                        with open(f"{target_local_path}{file_count:02}_{target_file_name}", 'wb') as local_file:
                            while offset < length:
                                dbfs_read_params = {
                                    "path": dbfs_path,
                                    "offset": offset,
                                    "length": length
                                }
                                resp = self.get("/dbfs/read", dbfs_read_params)

                                bytes_read = resp['bytes_read']
                                data = resp['data']
                                offset += bytes_read
                                local_file.write(base64.b64decode(data))
                        file_count = file_count+1

    def delete_files_on_dbfs(self, dbfs_acls_input_path):
        dbfs_delete_params = {
            "path": dbfs_acls_input_path,
            "recursive": True
        }
        self.post("/dbfs/delete", dbfs_delete_params)

    def wait_for_notebook_to_terminate(self, run_id, max_num_retries = 5):
        """
        Polls a job run until the job terminates
        :param run_id: the run to wait to terminate for
        :return: result object of the run; successfull if res["state"]["result_state"] == "SUCCESS"
        """
        cur_num_retries = 0
        while True:  # TODO add a timeout here
            try:
                res = self.get('/jobs/runs/get', {'run_id': run_id}, print_json=False)
                if self.is_verbose():
                    print(f"polling for job to finish: {res['run_page_url']}")
                if res["http_status_code"] != 200 or res["state"]['life_cycle_state'] == 'TERMINATED' or \
                        res["state"]['life_cycle_state'] == "INTERNAL_ERROR":
                    break
                time.sleep(self.NOTEBOOK_RUN_POLLING_INTERVAL_SECONDS)
            except Exception as e:
                print(f"caught Exception while polling for job to finish: {str(e)}")
                if cur_num_retries < max_num_retries:
                    #TODO sleep some time, backoff more for retries - make this configurable
                    time.sleep(self.NOTEBOOK_RUN_POLLING_INTERVAL_SECONDS * (cur_num_retries+3))
                    cur_num_retries = cur_num_retries+1
                else:
                    print(f"max num retries exceeeded : giving up {str(e)}")
                    raise

        return res

    def run_notebook_on_cluster(self, cid, notebook_path, notebook_params):
        runs_submit_params = {
            "run_name": f"migrate runs submit (notebook_path)",
            "existing_cluster_id": cid,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": notebook_params
            }
        }
        res = self.post('/jobs/runs/submit', runs_submit_params, print_json=True)

        if res["http_status_code"] != 200:
            # TODO add more error_text
            raise Exception(f"Could not run submit notebook {notebook_path}")

        return res["run_id"]

    def get_current_username(self, must_be_admin=False):
        user_info = self.get("/preview/scim/v2/Me")
        user_name = user_info.get("userName", None)

        if must_be_admin:
            is_admin = False
            for group in user_info.get("groups", []):
                if group.get('display', None) == 'admins':
                    is_admin = True
                    break
            if not is_admin:
                raise Exception(f"The table acl notebooks need to be run by an active admin user, {user_info}")

        return user_name

    def interpret_notebook_run_metadata(self, notebook_run_metadata):

        notebook_exit_value = None
        if "state" in notebook_run_metadata and notebook_run_metadata["state"]["result_state"] == "SUCCESS":

            # pickup notebook exit value set using dbutils.notebook.exit()
            runs_submit_params = {
                "run_id": notebook_run_metadata["run_id"]
            }
            res = self.get('/jobs/runs/get-output', runs_submit_params, print_json=True)
            if 'notebook_output' in res and 'result' in res['notebook_output']:
                notebook_exit_value = json.loads(res['notebook_output']['result'])
                print(f'Successfull notebook run with notebook_output: {json.dumps(notebook_exit_value)}')
            else:
                print(f'Successfull notebook run without notebook_output')

        else:
            if "state" in notebook_run_metadata:
                result_state = notebook_run_metadata['state']['result_state']
            else:
                result_state = "undefined"

            print(f"ERROR : Notebook run failed: {notebook_run_metadata['run_page_url']}")
            print(f"        ... http_status code {notebook_run_metadata['http_status_code']} ")
            print(f'        ... notebook execution result_state:  {result_state}')
            #TODO: Consider setting a non 0 exit code for some cases

        return notebook_exit_value

    def export_table_acls(self, db_name='', table_alcs_dir='table_acls/'):
        """Exports all table ACLs or just for a single database

        :param db_name: if set to empty strins, export ACLs for all databases
        :param table_alcs_dir: overwrite export path for table ACLs
        :return:
        """
        # TODO check whether this logic supports unicode (metadata had to do something to support it

        # as the IAM role is not used for Table ACLS, only metastore access required
        cid = self.launch_cluster(iam_role=None, enable_table_acls=True)
        self.wait_for_cluster(cid)

        user_name = self.get_current_username(must_be_admin=True)
        if self.DB_ADMIN_SUFFIX in user_name:
            notebook_parent_path = ""
        else:
            notebook_parent_path = f"/Users/{user_name}"
        export_table_acls_workspace_path = f"{notebook_parent_path}/tmp/migrate/Export_Table_ACLs.py"

        self.import_file_to_workspace(self.EXPORT_TABLE_ACLS_LOCAL_PATH, export_table_acls_workspace_path)

        dbfs_acls_output_path = "dbfs:/tmp/migrate/table_acl_perms.json.gz"

        run_notebook_params = {
            "Databases": db_name,
            "OutputPath": dbfs_acls_output_path
        }
        run_id = self.run_notebook_on_cluster(cid, export_table_acls_workspace_path, run_notebook_params)
        notebook_run_metadata = self.wait_for_notebook_to_terminate(run_id)

        # download all files inside output path from dbfs
        self.copy_files_from_dbfs_path(dbfs_acls_output_path, self._export_dir + table_alcs_dir, "table_acls.json.gz")

        # leave notebook there, so it can be removed later
        # we leave the cluster running, makes trouble shooting more efficient
        print(f"We leave the cluster running, in case you needed again: cluster_id: {cid}")

        notebook_exit_value = self.interpret_notebook_run_metadata(notebook_run_metadata)

        if notebook_exit_value is None:
            notebook_exit_value = { "total_num_acls": -1, "num_errors": -1 }

        return notebook_exit_value

    def import_table_acls(self, table_alcs_dir='table_acls/'):
        """
        Imports table ACLS exported before using export_table_acls

        :param table_alcs_dir:
        :return:
        """
        # TODO check whether this logic supports unicode (metadata had to do something to support it

        cid = self.launch_cluster(enable_table_acls=True)
        self.wait_for_cluster(cid)

        user_name = self.get_current_username(must_be_admin=True)
        if self.DB_ADMIN_SUFFIX in user_name:
            notebook_parent_path = ""
        else:
            notebook_parent_path = f"/Users/{user_name}"
        import_table_acls_workspace_path = f"{notebook_parent_path}/tmp/migrate/Import_Table_ACLs.py"
        self.import_file_to_workspace(self.IMPORT_TABLE_ACLS_LOCAL_PATH, import_table_acls_workspace_path)

        dbfs_acls_input_path = "dbfs:/tmp/migrate/table_acl_perms.json.gz"
        self.copy_files_to_dbfs_path(self._export_dir + table_alcs_dir, dbfs_acls_input_path)

        run_notebook_params = {
            "InputPath": dbfs_acls_input_path
        }
        run_id = self.run_notebook_on_cluster(cid, import_table_acls_workspace_path, run_notebook_params)
        notebook_run_metadata = self.wait_for_notebook_to_terminate(run_id)

        # cleanup dbfs: remove one directory above dbfs_acls_input_path
        self.delete_files_on_dbfs(dbfs_acls_input_path[0:dbfs_acls_input_path.rindex('/')])

        # leave notebook there, so it can be removed later
        # we leave the cluster running, makes trouble shooting more efficient
        print(f"We leave the cluster running, in case you needed again: cluster_id: {cid}")

        notebook_exit_value = self.interpret_notebook_run_metadata(notebook_run_metadata)

        if notebook_exit_value is None:
            notebook_exit_value = {
                  "total_num_acls": -1
                  ,"num_sucessfully_executed": -1
                  ,"num_execution_errors": -1
                  ,"num_error_entries_acls": -1
            }

        return notebook_exit_value
