import base64
from dbclient import *
import wmconstants
import concurrent
from concurrent.futures import ThreadPoolExecutor
from thread_safe_writer import ThreadSafeWriter
from threading_utils import propagate_exceptions
from timeit import default_timer as timer
from datetime import timedelta
import logging_utils
import logging
import os

WS_LIST = "/workspace/list"
WS_STATUS = "/workspace/get-status"
WS_MKDIRS = "/workspace/mkdirs"
WS_IMPORT = "/workspace/import"
WS_EXPORT = "/workspace/export"
LS_ZONES = "/clusters/list-zones"


class WorkspaceClient(dbclient):
    def __init__(self, configs, checkpoint_service):
        super().__init__(configs)
        self._checkpoint_service = checkpoint_service

    _languages = {'.py': 'PYTHON',
                  '.scala': 'SCALA',
                  '.r': 'R',
                  '.sql': 'SQL'}

    def get_language(self, file_ext):
        return self._languages[file_ext]

    def get_top_level_folders(self):
        # get top level folders excluding the /Users path
        supported_types = ('NOTEBOOK', 'DIRECTORY')
        root_items = self.get(WS_LIST, {'path': '/'}).get('objects', [])
        # filter out Projects and Users folders
        non_users_dir = list(filter(lambda x: (x.get('path') != '/Users' and x.get('path') != '/Projects'),
                                    root_items))
        dirs_and_nbs = list(filter(lambda x: (x.get('object_type') in supported_types),
                                    non_users_dir))
        return dirs_and_nbs

    def export_top_level_folders(self):
        ls_tld = self.get_top_level_folders()
        logged_nb_count = 0
        workspace_log_writer = ThreadSafeWriter(self.get_export_dir() + 'user_workspace.log', "a")
        libs_log_writer = ThreadSafeWriter(self.get_export_dir() + 'libraries.log', "a")
        dir_log_writer = ThreadSafeWriter(self.get_export_dir() + 'user_dirs.log', "a")
        checkpoint_item_log_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_ITEM_LOG_OBJECT
        )
        try:
            for tld_obj in ls_tld:
                # obj has 3 keys, object_type, path, object_id
                tld_path = tld_obj.get('path')
                log_count = self.log_all_workspace_items(
                    tld_path, workspace_log_writer, libs_log_writer, dir_log_writer, checkpoint_item_log_set)
                logged_nb_count += log_count
        finally:
            workspace_log_writer.close()
            libs_log_writer.close()
            dir_log_writer.close()
        dl_nb_count = self.download_notebooks()
        print(f'Total logged notebooks: {logged_nb_count}')
        print(f'Total Downloaded notebooks: {dl_nb_count}')

    def get_user_import_args(self, full_local_path, nb_full_path):
        """
        helper function to define the import parameters to upload a notebook object
        :param full_local_path: full local path of the notebook to read
        :param nb_full_path: full destination path, e.g. /Users/foo@db.com/bar.dbc . Includes extension / type
        :return: return the full input args to upload to the destination system
        """
        fp = open(full_local_path, "rb")
        (nb_path_dest, nb_type) = os.path.splitext(nb_full_path)
        in_args = {
            "content": base64.encodebytes(fp.read()).decode('utf-8'),
            "path": nb_path_dest,
            "format": self.get_file_format()
        }
        if self.is_source_file_format():
            if self.is_overwrite_notebooks():
                in_args['overwrite'] = True
            if nb_type == '.dbc':
                raise ValueError('Export is in DBC default format. Must export as SOURCE')
            in_args['language'] = self.get_language(nb_type)
            in_args['object_type'] = 'NOTEBOOK'
        return in_args

    @staticmethod
    def build_ws_lookup_table(success_ws_logfile):
        ws_hashmap = set()
        with open(success_ws_logfile, 'r') as fp:
            for line in fp:
                ws_hashmap.add(line.rstrip())
        return ws_hashmap

    @staticmethod
    def is_user_ws_item(ws_dir):
        """
        Checks if this is a user artifact / notebook.
        We can't create user home folders, hence we need to identify user items
        """
        path_list = [x for x in ws_dir.split('/') if x]
        if len(path_list) >= 2 and path_list[0] == 'Users':
            return True
        return False

    @staticmethod
    def is_user_ws_root(ws_dir):
        """
        Check if we're at the users home folder to skip folder creation
        """
        if ws_dir == '/Users/' or ws_dir == '/Users':
            return True
        path_list = [x for x in ws_dir.split('/') if x]
        if len(path_list) == 2 and path_list[0] == 'Users':
            return True
        return False

    @staticmethod
    def get_user(ws_dir):
        """
        returns the username of the workspace / folder path
        """
        path_list = [x for x in ws_dir.split('/') if x]
        if len(path_list) < 2:
            raise ValueError("Error: Not a users workspace directory")
        return path_list[1]

    @staticmethod
    def is_user_trash(ws_path):
        """
        checks if this is the users home folder trash directory, which is a special dir
        """
        path_list = ws_path.split('/')
        if len(path_list) == 4:
            if path_list[1] == 'Users' and path_list[3] == 'Trash':
                return True
        return False

    def is_user_home_empty(self, username):
        user_root = '/Users/' + username.rstrip().lstrip()
        get_args = {'path': user_root}
        items = self.get(WS_LIST, get_args).get('objects', None)
        if items:
            folders = self.filter_workspace_items(items, 'DIRECTORY')
            notebooks = self.filter_workspace_items(items, 'NOTEBOOK')
            # if both notebooks and directories are empty, return true
            if not folders and not notebooks:
                return True
            return False
        return True

    def get_num_of_saved_users(self, export_dir):
        """
        returns the number of exported user items to check against number of created users in the new workspace
        this helps identify if the new workspace is ready for the import, or if we should skip / archive failed imports
        """
        # get current number of saved workspaces
        user_home_dir = export_dir + 'Users'
        num_of_users = 0
        if os.path.exists(user_home_dir):
            ls = self.listdir(user_home_dir)
            for x in ls:
                if os.path.isdir(user_home_dir + '/' + x):
                    num_of_users += 1
        return num_of_users

    def export_user_home(self, username, local_export_dir, num_parallel=4):
        """
        Export the provided user's home directory
        :param username: user's home directory to export
        :param local_export_dir: folder location to do single user exports
        :return: None
        """
        original_export_dir = self.get_export_dir()
        user_export_dir = self.get_export_dir() + local_export_dir
        user_root = '/Users/' + username.rstrip().lstrip()
        self.set_export_dir(user_export_dir + '/{0}/'.format(username))
        print("Export path: {0}".format(self.get_export_dir()))
        os.makedirs(self.get_export_dir(), exist_ok=True)
        workspace_log_writer = ThreadSafeWriter(self.get_export_dir() + 'user_workspace.log', "a")
        libs_log_writer = ThreadSafeWriter(self.get_export_dir() + 'libraries.log', "a")
        dir_log_writer = ThreadSafeWriter(self.get_export_dir() + 'user_dirs.log', "a")
        checkpoint_item_log_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_ITEM_LOG_OBJECT
        )
        try:
            num_of_nbs = self.log_all_workspace_items(
                user_root, workspace_log_writer, libs_log_writer, dir_log_writer, checkpoint_item_log_set)
        finally:
            workspace_log_writer.close()
            libs_log_writer.close()
            dir_log_writer.close()

        if num_of_nbs == 0:
            raise ValueError('User does not have any notebooks in this path. Please verify the case of the email')
        num_of_nbs_dl = self.download_notebooks(ws_dir='user_artifacts/')
        print(f"Total notebooks logged: {num_of_nbs}")
        print(f"Total notebooks downloaded: {num_of_nbs_dl}")
        if num_of_nbs != num_of_nbs_dl:
            print(f"Notebooks logged != downloaded. Check the failed download file at: {user_export_dir}")
        print(f"Exporting the notebook permissions for {username}")
        acl_notebooks_writer = ThreadSafeWriter("acl_notebooks.log", "w")
        acl_notebooks_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT, self.get_export_dir())
        try:
            self.log_acl_to_file(
                'notebooks', 'user_workspace.log', acl_notebooks_writer, acl_notebooks_error_logger, num_parallel)
        finally:
            acl_notebooks_writer.close()

        print(f"Exporting the directories permissions for {username}")
        acl_directories_writer = ThreadSafeWriter("acl_directories.log", "w")
        acl_directories_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_DIRECTORY_ACL_OBJECT, self.get_export_dir())
        try:
            self.log_acl_to_file(
                'directories', 'user_dirs.log', acl_directories_writer, acl_directories_error_logger, num_parallel)
        finally:
            acl_directories_writer.close()
        # reset the original export dir for other calls to this method using the same client
        self.set_export_dir(original_export_dir)

    def import_user_home(self, username, local_export_dir):
        """
        Import the provided user's home directory
        logs/user_exports/{{USERNAME}}/ stores the log files to understand what was exported
        logs/user_exports/{{USERNAME}}/user_artifacts/ stores the notebook contents
        :param username: user's home directory to export
        :param local_export_dir: the log directory for this users workspace items
        :return: None
        """
        original_export_dir = self.get_export_dir()
        user_import_dir = self.get_export_dir() + local_export_dir
        if self.does_user_exist(username):
            print("Yes, we can upload since the user exists")
        else:
            print("User must exist before we upload the notebook contents. Please add the user to the platform first")
        user_root = '/Users/' + username.rstrip().lstrip()
        self.set_export_dir(user_import_dir + '/{0}/'.format(username))
        print("Import local path: {0}".format(self.get_export_dir()))
        notebook_dir = self.get_export_dir() + 'user_artifacts/'
        for root, subdirs, files in self.walk(notebook_dir):
            upload_dir = '/' + root.replace(notebook_dir, '')
            # if the upload dir is the 2 root directories, skip and continue
            if upload_dir == '/' or upload_dir == '/Users':
                continue
            if not self.is_user_ws_root(upload_dir):
                # if it is not the /Users/example@example.com/ root path, don't create the folder
                resp_mkdirs = self.post(WS_MKDIRS, {'path': upload_dir})
                print(resp_mkdirs)
            for f in files:
                # get full path for the local notebook file
                local_file_path = os.path.join(root, f)
                # create upload path and remove file format extension
                ws_file_path = upload_dir + '/' + f
                # generate json args with binary data for notebook to upload to the workspace path
                nb_input_args = self.get_user_import_args(local_file_path, ws_file_path)
                # call import to the workspace
                if self.is_verbose():
                    print("Path: {0}".format(nb_input_args['path']))
                resp_upload = self.post(WS_IMPORT, nb_input_args)
                if self.is_verbose():
                    print(resp_upload)

        # import the user's workspace ACLs
        notebook_acl_logs = user_import_dir + f'/{username}/acl_notebooks.log'
        acl_notebooks_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT, self.get_export_dir())
        if os.path.exists(notebook_acl_logs):
            print(f"Importing the notebook acls for {username}")
            with open(notebook_acl_logs) as nb_acls_fp:
                for nb_acl_str in nb_acls_fp:
                    self.apply_acl_on_object(nb_acl_str, acl_notebooks_error_logger)

        dir_acl_logs = user_import_dir + f'/{username}/acl_directories.log'
        acl_dir_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_DIRECTORY_ACL_OBJECT, self.get_export_dir())
        if os.path.exists(dir_acl_logs):
            print(f"Importing the directory acls for {username}")
            with open(dir_acl_logs) as dir_acls_fp:
                for dir_acl_str in dir_acls_fp:
                    self.apply_acl_on_object(dir_acl_str, acl_dir_error_logger)
        self.set_export_dir(original_export_dir)

    def download_notebooks(self, ws_log_file='user_workspace.log', ws_dir='artifacts/', num_parallel=4):
        """
        Loop through all notebook paths in the logfile and download individual notebooks
        :param ws_log_file: logfile for all notebook paths in the workspace
        :param ws_dir: export directory to store all notebooks
        :return: None
        """
        checkpoint_notebook_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        ws_log = self.get_export_dir() + ws_log_file
        notebook_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT, self.get_export_dir())
        num_notebooks = 0
        if not os.path.exists(ws_log):
            raise Exception("Run --workspace first to download full log of all notebooks.")
        with open(ws_log, "r") as fp:
            # notebook log metadata file now contains object_id to help w/ ACL exports
            # pull the path from the data to download the individual notebook contents
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = [executor.submit(self.download_notebook_helper, notebook_data, checkpoint_notebook_set, notebook_error_logger, self.get_export_dir() + ws_dir) for notebook_data in fp]
                for future in concurrent.futures.as_completed(futures):
                    dl_resp = future.result()
                    if 'error' not in dl_resp:
                        num_notebooks += 1
        return num_notebooks

    def download_notebook_helper(self, notebook_data, checkpoint_notebook_set, error_logger, export_dir='artifacts/'):
        """
        Helper function to download an individual notebook, or log the failure in the failure logfile
        :param notebook_path: an individual notebook path
        :param export_dir: directory to store all notebooks
        :return: return the notebook path that's successfully downloaded
        """
        notebook_path = json.loads(notebook_data).get('path', None).rstrip('\n')
        if checkpoint_notebook_set.contains(notebook_path):
            return {'path': notebook_path}
        get_args = {'path': notebook_path, 'format': self.get_file_format()}
        if self.is_verbose():
            logging.info("Downloading: {0}".format(get_args['path']))
        resp = self.get(WS_EXPORT, get_args)
        if resp.get('error', None):
            resp['path'] = notebook_path
            logging_utils.log_reponse_error(error_logger, resp)
            return resp
        if resp.get('error_code', None):
            resp['path'] = notebook_path
            logging_utils.log_reponse_error(error_logger, resp)
            return resp
        nb_path = os.path.dirname(notebook_path)
        if nb_path != '/':
            # path is NOT empty, remove the trailing slash from export_dir
            save_path = export_dir[:-1] + nb_path + '/'
        else:
            save_path = export_dir
        save_filename = save_path + os.path.basename(notebook_path) + '.' + resp.get('file_type')
        # If the local path doesn't exist,we create it before we save the contents
        if not os.path.exists(save_path) and save_path:
            os.makedirs(save_path, exist_ok=True)
        with open(save_filename, "wb") as f:
            f.write(base64.b64decode(resp['content']))
        checkpoint_notebook_set.write(notebook_path)
        return {'path': notebook_path}

    def filter_workspace_items(self, item_list, item_type):
        """
        Helper function to filter on different workspace types.
        :param item_list: iterable of workspace items
        :param item_type: DIRECTORY, NOTEBOOK, LIBRARY
        :return: list of items filtered by type
        """
        supported_types = {'DIRECTORY', 'NOTEBOOK', 'LIBRARY'}
        if item_type not in supported_types:
            raise ValueError('Unsupported type provided: {0}.\n. Supported types: {1}'.format(item_type,
                                                                                              str(supported_types)))
        filtered_list = list(self.my_map(lambda y: {'path': y.get('path', None),
                                                    'object_id': y.get('object_id', None)},
                                         filter(lambda x: x.get('object_type', None) == item_type, item_list)))
        return filtered_list

    def init_workspace_logfiles(self, workspace_log_file='user_workspace.log',
                                libs_log_file='libraries.log', workspace_dir_log_file='user_dirs.log'):
        """
        initialize the logfile locations since we run a recursive function to download notebooks
        """
        workspace_log = self.get_export_dir() + workspace_log_file
        libs_log = self.get_export_dir() + libs_log_file
        workspace_dir_log = self.get_export_dir() + workspace_dir_log_file
        if not self._checkpoint_service.checkpoint_file_exists(wmconstants.WM_EXPORT, wmconstants.WORKSPACE_ITEM_LOG_OBJECT):
            if os.path.exists(workspace_log):
                os.remove(workspace_log)
            if os.path.exists(workspace_dir_log):
                os.remove(workspace_dir_log)
            if os.path.exists(libs_log):
                os.remove(libs_log)

    def log_all_workspace_items_entry(self, ws_path='/', workspace_log_file='user_workspace.log', libs_log_file='libraries.log', dir_log_file='user_dirs.log', exclude_prefixes=[]):
        logging.info(f"Skip all paths with the following prefixes: {exclude_prefixes}")

        workspace_log_writer = ThreadSafeWriter(self.get_export_dir() + workspace_log_file, "a")
        libs_log_writer = ThreadSafeWriter(self.get_export_dir() + libs_log_file, "a")
        dir_log_writer = ThreadSafeWriter(self.get_export_dir() + dir_log_file, "a")
        checkpoint_item_log_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_ITEM_LOG_OBJECT
        )
        try:
            num_nbs = self.log_all_workspace_items(ws_path=ws_path, workspace_log_writer=workspace_log_writer,
                                        libs_log_writer=libs_log_writer, dir_log_writer=dir_log_writer, checkpoint_set=checkpoint_item_log_set, exclude_prefixes=exclude_prefixes)
        finally:
            workspace_log_writer.close()
            libs_log_writer.close()
            dir_log_writer.close()

        return num_nbs

    def log_all_workspace_items(self, ws_path, workspace_log_writer, libs_log_writer, dir_log_writer, checkpoint_set, exclude_prefixes=[]):
        """
        Loop and log all workspace items to download them at a later time
        :param ws_path: root path to log all the items of the notebook workspace
        :param workspace_log_file: logfile to store all the paths of the notebooks
        :param libs_log_file: library logfile to store workspace libraries
        :param dir_log_file: log directory for users
        :return:
        """
        # define log file names for notebooks, folders, and libraries
        if ws_path == '/':
            # default is the root path
            get_args = {'path': '/'}
        else:
            get_args = {'path': ws_path}

        if not os.path.exists(self.get_export_dir()):
            os.makedirs(self.get_export_dir(), exist_ok=True)
        items = self.get(WS_LIST, get_args).get('objects', None)
        num_nbs = 0
        if self.is_verbose():
            logging.info("Listing: {0}".format(get_args['path']))
        if items is not None:
            # list all the users folders only
            folders = self.filter_workspace_items(items, 'DIRECTORY')
            # should be no notebooks, but lets filter and can check later
            notebooks = self.filter_workspace_items(items, 'NOTEBOOK')
            libraries = self.filter_workspace_items(items, 'LIBRARY')
            for x in notebooks:
                # notebook objects has path and object_id
                path = x.get('path')
                if not checkpoint_set.contains(path) and not path.startswith(tuple(exclude_prefixes)):
                    if self.is_verbose():
                        logging.info("Saving path: {0}".format(x.get('path')))
                    workspace_log_writer.write(json.dumps(x) + '\n')
                    checkpoint_set.write(path)
                num_nbs += 1
            for y in libraries:
                path = y.get('path')
                if not checkpoint_set.contains(path) and not path.startswith(tuple(exclude_prefixes)):
                    libs_log_writer.write(json.dumps(y) + '\n')
                    checkpoint_set.write(path)
            # log all directories to export permissions
            if folders:
                def _recurse_log_all_workspace_items(folder):
                    dir_path = folder.get('path', None)
                    if not WorkspaceClient.is_user_trash(dir_path):
                        dir_log_writer.write(json.dumps(folder) + '\n')
                        return self.log_all_workspace_items(ws_path=dir_path,
                                                            workspace_log_writer=workspace_log_writer,
                                                            libs_log_writer=libs_log_writer,
                                                            dir_log_writer=dir_log_writer,
                                                            checkpoint_set=checkpoint_set,
                                                            exclude_prefixes=exclude_prefixes)

                for folder in folders:
                    path = folder.get('path', None)
                    if not checkpoint_set.contains(path) and not path.startswith(tuple(exclude_prefixes)):
                        num_nbs_plus = _recurse_log_all_workspace_items(folder)
                        checkpoint_set.write(path)
                        if num_nbs_plus:
                            num_nbs += num_nbs_plus


        return num_nbs

    def get_obj_id_by_path(self, input_path):
        resp = self.get(WS_STATUS, {'path': input_path})
        obj_id = resp.get('object_id', None)
        return obj_id

    def log_acl_to_file(self, artifact_type, read_log_filename, writer, error_logger, num_parallel):
        """
        generic function to log the notebook/directory ACLs to specific file names
        :param artifact_type: set('notebooks', 'directories') ACLs to be logged
        :param read_log_filename: the list of the notebook paths / object ids
        :param write_log_filename: output file to store object_id acls
        :param error_logger: logger to log errors
        """
        read_log_path = self.get_export_dir() + read_log_filename
        if not os.path.exists(read_log_path):
            logging.info(f"No log exists for {read_log_path}. Skipping ACL export ...")
            return
        def _acl_log_helper(json_data):
            data = json.loads(json_data)
            obj_id = data.get('object_id', None)
            api_endpoint = '/permissions/{0}/{1}'.format(artifact_type, obj_id)
            acl_resp = self.get(api_endpoint)
            acl_resp['path'] = data.get('path')
            if logging_utils.log_reponse_error(error_logger, acl_resp):
                return
            acl_resp.pop('http_status_code')
            writer.write(json.dumps(acl_resp) + '\n')

        with open(read_log_path, 'r') as read_fp:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = [executor.submit(_acl_log_helper, json_data) for json_data in read_fp]
                concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
                propagate_exceptions(futures)

    def log_all_workspace_acls(self, workspace_log_file='user_workspace.log',
                               dir_log_file='user_dirs.log',
                               num_parallel=4):
        """
        loop through all notebooks and directories to store their associated ACLs
        :param workspace_log_file: input file for user notebook listing
        :param dir_log_file: input file for user directory listing
        """
        # define log file names for notebooks, folders, and libraries
        logging.info("Exporting the notebook permissions")
        start = timer()
        acl_notebooks_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT, self.get_export_dir())
        acl_notebooks_writer = ThreadSafeWriter(self.get_export_dir() + "acl_notebooks.log", "w")
        try:
            self.log_acl_to_file('notebooks', workspace_log_file, acl_notebooks_writer, acl_notebooks_error_logger, num_parallel)
        finally:
            acl_notebooks_writer.close()
        end = timer()
        logging.info("Complete Notebook ACLs Export Time: " + str(timedelta(seconds=end - start)))

        logging.info("Exporting the directories permissions")
        start = timer()
        acl_directory_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.WORKSPACE_DIRECTORY_ACL_OBJECT, self.get_export_dir())
        acl_directory_writer = ThreadSafeWriter(self.get_export_dir() + "acl_directories.log", "w")
        try:
            self.log_acl_to_file('directories', dir_log_file, acl_directory_writer, acl_directory_error_logger, num_parallel)
        finally:
            acl_directory_writer.close()
        end = timer()
        logging.info("Complete Directories ACLs Export Time: " + str(timedelta(seconds=end - start)))

    def apply_acl_on_object(self, acl_str, error_logger, checkpoint_key_set):
        """
        apply the acl definition to the workspace object
        object_id comes from the export data which contains '/type/id' format for this key
        the object_id contains the {{/type/object_id}} format which helps craft the api endpoint
        setting acl definitions uses the patch rest api verb
        :param acl_str: the complete string from the logfile. contains object defn and acl lists
        """
        object_acl = json.loads(acl_str)
        # the object_type
        object_type = object_acl.get('object_type', None)
        obj_path = object_acl['path']
        logging.info(f"Working on ACL for path: {obj_path}")

        if not checkpoint_key_set.contains(obj_path):
            # We cannot modify '/Shared' directory's ACL
            if obj_path == "/Shared" and object_type == "directory":
                logging.info("We cannot modify /Shared directory's ACL. Skipping..")
                checkpoint_key_set.write(obj_path)
                return

            if self.is_user_ws_item(obj_path):
                ws_user = self.get_user(obj_path)
                if not self.does_user_exist(ws_user):
                    logging.info(f"User workspace does not exist: {obj_path}, skipping ACL")
                    return
            obj_status = self.get(WS_STATUS, {'path': obj_path})
            if logging_utils.log_reponse_error(error_logger, obj_status):
                return
            logging.info("ws-stat: ", obj_status)
            current_obj_id = obj_status.get('object_id', None)
            if not current_obj_id:
                error_logger.error(f'Object id missing from destination workspace: {obj_status}')
                return
            if object_type == 'directory':
                object_id_with_type = f'/directories/{current_obj_id}'
            elif object_type == 'notebook':
                object_id_with_type = f'/notebooks/{current_obj_id}'
            else:
                error_logger.error(f'Object for Workspace ACLs is Undefined: {obj_status}')
                return
            api_path = '/permissions' + object_id_with_type
            acl_list = object_acl.get('access_control_list', None)
            access_control_list = self.build_acl_args(acl_list)
            if access_control_list:
                api_args = {'access_control_list': access_control_list}
                resp = self.patch(api_path, api_args)
                if not logging_utils.log_reponse_error(error_logger, resp):
                    checkpoint_key_set.write(obj_path)
        return

    def import_workspace_acls(self, workspace_log_file='acl_notebooks.log',
                              dir_log_file='acl_directories.log', num_parallel=1):
        """
        import the notebook and directory acls by looping over notebook and dir logfiles
        """
        dir_acl_logs = self.get_export_dir() + dir_log_file
        notebook_acl_logs = self.get_export_dir() + workspace_log_file
        acl_notebooks_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT, self.get_export_dir())

        checkpoint_notebook_acl_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_ACL_OBJECT)
        with open(notebook_acl_logs) as nb_acls_fp:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = [executor.submit(self.apply_acl_on_object, nb_acl_str, acl_notebooks_error_logger, checkpoint_notebook_acl_set) for nb_acl_str in nb_acls_fp]
                concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
                propagate_exceptions(futures)

        acl_dir_error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_DIRECTORY_ACL_OBJECT, self.get_export_dir())
        checkpoint_dir_acl_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_DIRECTORY_ACL_OBJECT)

        with open(dir_acl_logs) as dir_acls_fp:
            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = [executor.submit(self.apply_acl_on_object, dir_acl_str, acl_dir_error_logger, checkpoint_dir_acl_set) for dir_acl_str in dir_acls_fp]
                concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
                propagate_exceptions(futures)
        print("Completed import ACLs of Notebooks and Directories")

    def get_current_users(self):
        """
        get the num of defined user home directories in the new workspace
        if this is 0, we must create the users before importing the notebooks over.
        we cannot create the users home directory since its a special type of directory
        """
        ws_users = self.get(WS_LIST, {'path': '/Users/'}).get('objects', None)
        if ws_users:
            return len(ws_users)
        else:
            return 0

    def does_user_exist(self, username):
        """
        check if the users home dir exists
        """
        stat = self.get(WS_STATUS, {'path': '/Users/{0}'.format(username)})
        if stat.get('object_type', None) == 'DIRECTORY':
            return True
        return False

    def does_path_exist(self, dir_path):
        status_resp = self.get(WS_STATUS, {'path': dir_path})
        if 'error_code' in status_resp:
            if status_resp.get('error_code') == 'RESOURCE_DOES_NOT_EXIST':
                return False
            else:
                print('Failure:' + json.dumps(status_resp))
                return False
        return True

    def import_current_workspace_items(self,artifact_dir='artifacts/'):
        src_dir = self.get_export_dir() + artifact_dir
        error_logger = logging_utils.get_error_logger(wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT,
                                                      self.get_export_dir())
        for root, subdirs, files in self.walk(src_dir):
            # replace the local directory with empty string to get the notebook workspace directory
            nb_dir = '/' + root.replace(src_dir, '')
            upload_dir = nb_dir
            if not nb_dir == '/':
                upload_dir = nb_dir + '/'
            if not self.does_path_exist(upload_dir):
                resp_mkdirs = self.post(WS_MKDIRS, {'path': upload_dir})
                if 'error_code' in resp_mkdirs:
                    logging_utils.log_reponse_error(error_logger, resp_mkdirs)
            for f in files:
                logging.info("Uploading: {0}".format(f))
                # create the local file path to load the DBC file
                local_file_path = os.path.join(root, f)
                # create the ws full file path including filename
                ws_file_path = upload_dir + f
                # generate json args with binary data for notebook to upload to the workspace path
                nb_input_args = self.get_user_import_args(local_file_path, ws_file_path)
                # call import to the workspace
                if self.is_verbose():
                    logging.info("Path: {0}".format(nb_input_args['path']))
                resp_upload = self.post(WS_IMPORT, nb_input_args)
                if 'error_code' in resp_upload:
                    resp_upload['path'] = nb_input_args['path']
                    logging_utils.log_reponse_error(error_logger, resp_upload)

    def import_all_workspace_items(self, artifact_dir='artifacts/',
                                   archive_missing=False, num_parallel=4):
        """
        import all notebooks into a new workspace. Walks the entire artifacts/ directory in parallel, and also
        upload all the files in each of the directories in parallel.

        WARNING: Because it parallelizes both on directory walking and file uploading, it can spawn as many threads as
                 num_parallel * num_parallel

        :param artifact_dir: notebook download directory
        :param failed_log: failed import log
        :param archive_missing: whether to put missing users into a /Archive/ top level directory
        """
        src_dir = self.get_export_dir() + artifact_dir
        error_logger = logging_utils.get_error_logger(wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT,
                                                      self.get_export_dir())

        checkpoint_notebook_set = self._checkpoint_service.get_checkpoint_key_set(
            wmconstants.WM_IMPORT, wmconstants.WORKSPACE_NOTEBOOK_OBJECT)
        num_exported_users = self.get_num_of_saved_users(src_dir)
        num_current_users = self.get_current_users()
        if num_current_users == 0:
            logging.info("No registered users in existing environment. Please import users / groups first.")
            raise ValueError("No registered users in the current environment")
        if (num_current_users < num_exported_users) and (not archive_missing):
            logging.info("Exported number of user workspaces: {0}".format(num_exported_users))
            logging.info("Current number of user workspaces: {0}".format(num_current_users))
            logging.info("Re-run with the `--archive-missing` flag to load missing users into a separate directory")
            raise ValueError("Current number of users is less than number of user workspaces to import.")
        archive_users = set()

        def _upload_all_files(root, subdirs, files):
            '''
            Upload all files in parallel in root (current) directory.
            '''
            # replace the local directory with empty string to get the notebook workspace directory
            nb_dir = '/' + root.replace(src_dir, '')
            upload_dir = nb_dir
            if not nb_dir == '/':
                upload_dir = nb_dir + '/'
            if self.is_user_ws_item(upload_dir):
                ws_user = self.get_user(upload_dir)
                if archive_missing:
                    if ws_user in archive_users:
                        upload_dir = upload_dir.replace('Users', 'Archive', 1)
                    elif not self.does_user_exist(ws_user):
                        # add the user to the cache / set of missing users
                        logging.info("User workspace does not exist, adding to archive cache: {0}".format(ws_user))
                        archive_users.add(ws_user)
                        # append the archive path to the upload directory
                        upload_dir = upload_dir.replace('Users', 'Archive', 1)
                    else:
                        logging.info("User workspace exists: {0}".format(ws_user))
                elif not self.does_user_exist(ws_user):
                    logging.info("User {0} is missing. "
                                 "Please re-run with --archive-missing flag "
                                 "or first verify all users exist in the new workspace".format(ws_user))
                    return
                else:
                    logging.info("Uploading for user: {0}".format(ws_user))
            # make the top level folder before uploading files within the loop
            if not self.is_user_ws_root(upload_dir):
                # if it is not the /Users/example@example.com/ root path, don't create the folder
                resp_mkdirs = self.post(WS_MKDIRS, {'path': upload_dir})
                if 'error_code' in resp_mkdirs:
                    resp_mkdirs['path'] = upload_dir
                    logging_utils.log_reponse_error(error_logger, resp_mkdirs)

            def _file_upload_helper(f):
                logging.info("Uploading: {0}".format(f))
                # create the local file path to load the DBC file
                local_file_path = os.path.join(root, f)
                # create the ws full file path including filename
                ws_file_path = upload_dir + f
                if checkpoint_notebook_set.contains(ws_file_path):
                    return
                # generate json args with binary data for notebook to upload to the workspace path
                nb_input_args = self.get_user_import_args(local_file_path, ws_file_path)
                # call import to the workspace
                if self.is_verbose():
                    logging.info("Path: {0}".format(nb_input_args['path']))
                resp_upload = self.post(WS_IMPORT, nb_input_args)
                if 'error_code' in resp_upload:
                    resp_upload['path'] = ws_file_path
                    logging.info(f'Error uploading file: {ws_file_path}')
                    logging_utils.log_reponse_error(error_logger, resp_upload)
                else:
                    checkpoint_notebook_set.write(ws_file_path)

            with ThreadPoolExecutor(max_workers=num_parallel) as executor:
                futures = [executor.submit(_file_upload_helper, file) for file in files]
                concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
                propagate_exceptions(futures)

        with ThreadPoolExecutor(max_workers=num_parallel) as executor:
            futures = [executor.submit(_upload_all_files, walk[0], walk[1], walk[2]) for walk in self.walk(src_dir)]
            concurrent.futures.wait(futures, return_when="FIRST_EXCEPTION")
            propagate_exceptions(futures)
