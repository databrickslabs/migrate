import base64
from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta
import os

WS_LIST = "/workspace/list"
WS_STATUS = "/workspace/get-status"
WS_MKDIRS = "/workspace/mkdirs"
WS_IMPORT = "/workspace/import"
WS_EXPORT = "/workspace/export"
LS_ZONES = "/clusters/list-zones"


def get_user_import_args(full_local_path, nb_full_path):
    fp = open(full_local_path, "rb")

    in_args = {
        "content": base64.encodebytes(fp.read()).decode('utf-8'),
        "path": nb_full_path[:-4],
        "format": "DBC"
    }
    return in_args


class WorkspaceClient(ScimClient):


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
        Checke if we're at the users home folder to skip folder creation
        """
        if ws_dir == '/Users/' or ws_dir == '/Users':
            return True
        path_list = [x for x in ws_dir.split('/') if x]
        print(path_list)
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

    @staticmethod
    def get_num_of_saved_users(export_dir):
        """
        returns the number of exported user items to check against number of created users in the new workspace
        this helps identify if the new workspace is ready for the import, or if we should skip / archive failed imports
        """
        # get current number of saved workspaces
        user_home_dir = export_dir + 'Users'
        ls = os.listdir(user_home_dir)
        num_of_users = 0
        for x in ls:
            if os.path.isdir(user_home_dir + '/' + x):
                num_of_users += 1
        return num_of_users

    def export_user_home(self, username, local_export_dir):
        """
        Export the provided user's home directory
        :param username: user's home directory to export
        :return: None
        """
        user_export_dir = self.get_export_dir() + local_export_dir
        user_root = '/Users/' + username.rstrip().lstrip()
        self.set_export_dir(user_export_dir + '/{0}/'.format(username))
        print("Export path: {0}".format(self.get_export_dir()))
        self.log_all_workspace_items(ws_path=user_root)
        self.download_notebooks(ws_dir='user_artifacts/')

    def import_user_home(self, username, local_export_dir):
        """
        Import the provided user's home directory
        logs/user_exports/{{USERNAME}}/ stores the log files to understand what was exported
        logs/user_exports/{{USERNAME}}/user_artifacts/ stores the notebook contents
        :param username: user's home directory to export
        :return: None
        """
        user_import_dir = self.get_export_dir() + local_export_dir
        if self.does_user_exist(username):
            print("Yes, we can upload since the user exists")
        else:
            print("User must exist before we upload the notebook contents. Please add the user to the platform first")
        user_root = '/Users/' + username.rstrip().lstrip()
        self.set_export_dir(user_import_dir + '/{0}/'.format(username))
        print("Import local path: {0}".format(self.get_export_dir()))
        notebook_dir = self.get_export_dir() + 'user_artifacts/'
        for root, subdirs, files in os.walk(notebook_dir):
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
                nb_input_args = get_user_import_args(local_file_path, ws_file_path)
                # call import to the workspace
                if self.is_verbose():
                    print("Path: {0}".format(nb_input_args['path']))
                resp_upload = self.post(WS_IMPORT, nb_input_args)
                if self.is_verbose():
                    print(resp_upload)

    def download_notebooks(self, ws_log_file='user_workspace.log', ws_dir='artifacts/'):
        """
        Loop through all notebook paths in the logfile and download individual notebooks
        :param ws_log_file: logfile for all notebook paths in the workspace
        :param ws_dir: export directory to store all notebooks
        :return: None
        """
        ws_log = self.get_export_dir() + ws_log_file
        if not os.path.exists(ws_log):
            raise Exception("Run --workspace first to download full log of all notebooks.")
        with open(ws_log, "r") as fp:
            # notebook log metadata file now contains object_id to help w/ ACL exports
            # pull the path from the data to download the individual notebook contents
            for notebook_data in fp:
                notebook_path = json.loads(notebook_data).get('path', None).rstrip()
                self.download_notebook_helper(notebook_path, export_dir=self.get_export_dir() + ws_dir)

    def download_notebook_helper(self, notebook_path, export_dir='artifacts/'):
        """
        Helper function to download an individual notebook, or log the failure in the failure logfile
        :param notebook_path: an individual notebook path
        :param export_dir: directory to store all notebooks
        :return: return the notebook path that's succssfully downloaded
        """
        get_args = {'path': notebook_path, 'format': 'DBC'}
        if self.is_verbose():
            print("Downloading: {0}".format(get_args['path']))
        resp = self.get(WS_EXPORT, get_args)
        with open(self.get_export_dir() + 'failed_notebooks.log', 'a') as err_log:
            if resp.get('error_code', None):
                err_msg = {'error_code': resp.get('error_code'), 'path': notebook_path}
                err_log.write(json.dumps(err_msg) + '\n')
                return err_msg
        nb_path = os.path.dirname(notebook_path)
        if nb_path != '/':
            # path is NOT empty, remove the trailing slash from export_dir
            save_path = export_dir[:-1] + nb_path + '/'
        else:
            save_path = export_dir
        save_filename = save_path + os.path.basename(notebook_path) + '.dbc'
        # If the local path doesn't exist,we create it before we save the contents
        if not os.path.exists(save_path) and save_path:
            os.makedirs(save_path, exist_ok=True)
        with open(save_filename, "wb") as f:
            f.write(base64.b64decode(resp['content']))
        return {'path': notebook_path}

    def filter_workspace_items(self, item_list, item_type):
        """
        Helper function to filter on different workspace types.
        :param item_list: iterable of workspace items
        :param item_type: DIRECTORY, NOTEBOOK, LIBRARY
        :return: list of items filtered by type
        """
        supported_types = set(('DIRECTORY', 'NOTEBOOK', 'LIBRARY'))
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
        if os.path.exists(workspace_log):
            os.remove(workspace_log)
        if os.path.exists(workspace_dir_log):
            os.remove(workspace_dir_log)
        if os.path.exists(libs_log):
            os.remove(libs_log)

    def log_all_workspace_items(self, ws_path='/', workspace_log_file='user_workspace.log',
                                libs_log_file='libraries.log', dir_log_file='user_dirs.log'):
        """
        Loop and log all workspace items to download them at a later time
        :param ws_path: root path to log all the items of the notebook workspace
        :param workspace_log_file: logfile to store all the paths of the notebooks
        :param libs_log_file: library logfile to store workspace libraries
        :return:
        """
        # define log file names for notebooks, folders, and libraries
        workspace_log = self.get_export_dir() + workspace_log_file
        workspace_dir_log = self.get_export_dir() + dir_log_file
        libs_log = self.get_export_dir() + libs_log_file
        if ws_path == '/':
            # default is the root path
            get_args = {'path': '/'}
        else:
            get_args = {'path': ws_path}

        if not os.path.exists(self.get_export_dir()):
            os.makedirs(self.get_export_dir(), exist_ok=True)
        items = self.get(WS_LIST, get_args).get('objects', None)
        if self.is_verbose():
            print("Listing: {0}".format(get_args['path']))
        if items is not None:
            # list all the users folders only
            folders = self.filter_workspace_items(items, 'DIRECTORY')
            # should be no notebooks, but lets filter and can check later
            notebooks = self.filter_workspace_items(items, 'NOTEBOOK')
            libraries = self.filter_workspace_items(items, 'LIBRARY')
            with open(workspace_log, "a") as ws_fp, open(libs_log, "a") as libs_fp:
                for x in notebooks:
                    if self.is_verbose():
                        print("Saving path: {0}".format(x))
                    ws_fp.write(json.dumps(x) + '\n')
                for y in libraries:
                    libs_fp.write(json.dumps(y) + '\n')
            # log all directories to export permissions
            with open(workspace_dir_log, "a") as dir_fp:
                if folders:
                    for f in folders:
                        dir_path = f.get('path', None)
                        if not WorkspaceClient.is_user_trash(dir_path):
                            dir_fp.write(json.dumps(f) + '\n')
                            self.log_all_workspace_items(ws_path=dir_path, workspace_log_file=workspace_log_file,
                                                         libs_log_file=libs_log_file)

    def get_obj_id_by_path(self, path):
        resp = self.get(WS_STATUS, {'path': path})
        obj_id = resp.get('object_id', None)
        return obj_id

    def log_acl_to_file(self, artifact_type, read_log_filename, write_log_filename):
        """
        generic function to log the notebook/directory ACLs to specific file names
        :param artifact_type: set('notebooks', 'directories') ACLs to be logged
        :param read_log_filename: the list of the notebook paths / object ids
        :param write_log_filename: output file to store object_id acls
        """
        read_log_path = self.get_export_dir() + read_log_filename
        write_log_path = self.get_export_dir() + write_log_filename
        with open(read_log_path, 'r') as read_fp, open(write_log_path, 'w') as write_fp:
            for x in read_fp:
                data = json.loads(x)
                obj_id = data.get('object_id', None)
                api_endpoint = '/permissions/{0}/{1}'.format(artifact_type, obj_id)
                acl_resp = self.get(api_endpoint)
                acl_resp['path'] = data.get('path')
                acl_resp.pop('http_status_code')
                write_fp.write(json.dumps(acl_resp) + '\n')

    def log_all_workspace_acls(self, workspace_log_file='user_workspace.log',
                               dir_log_file='user_dirs.log'):
        """
        loop through all notebooks and directories to store their associated ACLs
        :param workspace_log_file: input file for user notebook listing
        :param dir_log_file: input file for user directory listing
        """
        # define log file names for notebooks, folders, and libraries
        print("Exporting the notebook permissions")
        start = timer()
        self.log_acl_to_file('notebooks', workspace_log_file, 'acl_notebooks.log')
        end = timer()
        print("Complete Notebook ACLs Export Time: " + str(timedelta(seconds=end - start)))
        print("Exporting the directories permissions")
        start = timer()
        self.log_acl_to_file('directories', dir_log_file, 'acl_directories.log')
        end = timer()
        print("Complete Directories ACLs Export Time: " + str(timedelta(seconds=end - start)))

    def apply_acl_on_object(self, acl_str):
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
        obj_id = self.get(WS_STATUS, {'path': object_acl['path']}).get('object_id', None)
        if object_type == 'directory':
            object_id_with_type = f'/directories/{obj_id}'
        elif object_type == 'notebook':
            object_id_with_type = f'/notebooks/{obj_id}'
        else:
            raise ValueError('Object for Workspace ACLs is Undefined')
        api_path = '/permissions' + object_id_with_type
        acl_list = object_acl.get('access_control_list', None)
        api_args = {'access_control_list': self.build_acl_args(acl_list)}
        resp = self.patch(api_path, api_args)
        print(resp)
        return resp

    def import_workspace_acls(self, workspace_log_file='acl_notebooks.log',
                              dir_log_file='acl_directories.log'):
        """
        import the notebook and directory acls by looping over notebook and dir logfiles
        """
        dir_acl_logs = self.get_export_dir() + dir_log_file
        notebook_acl_logs = self.get_export_dir() + workspace_log_file
        with open(notebook_acl_logs) as nb_acls_fp:
            for nb_acl_str in nb_acls_fp:
                self.apply_acl_on_object(nb_acl_str)
        with open(dir_acl_logs) as dir_acls_fp:
            for dir_acl_str in dir_acls_fp:
                self.apply_acl_on_object(dir_acl_str)
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

    def import_all_workspace_items(self, artifact_dir='artifacts/', archive_missing=False):
        """
        import all notebooks into a new workspace
        :param artifact_dir: notebook download directory
        :param archive_missing: whether to put missing users into a /Archive/ top level directory
        """
        src_dir = self.get_export_dir() + artifact_dir
        num_exported_users = self.get_num_of_saved_users(src_dir)
        num_current_users = self.get_current_users()
        if num_current_users == 0:
            print("No registered users in existing environment. Please import users / groups first.")
            raise ValueError("No registered users in the current environment")
        if (num_current_users < num_exported_users) and (not archive_missing):
            print("Exported number of user workspaces: {0}".format(num_exported_users))
            print("Current number of user workspaces: {0}".format(num_current_users))
            print("Re-run with the `--archive-missing` flag to load missing users into a separate directory")
            raise ValueError("Current number of users is less than number of user workspaces to import.")
        archive_users = set()
        for root, subdirs, files in os.walk(src_dir):
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
                        print("User workspace does not exist, adding to archive cache: {0}".format(ws_user))
                        archive_users.add(ws_user)
                        # append the archive path to the upload directory
                        upload_dir = upload_dir.replace('Users', 'Archive', 1)
                    else:
                        print("User workspace exists: {0}".format(ws_user))
                elif not self.does_user_exist(ws_user):
                    print("User {0} is missing. "
                          "Please re-run with --archive-missing flag "
                          "or first verify all users exist in the new workspace".format(ws_user))
                    return
                else:
                    print("Uploading for user: {0}".format(ws_user))
            # make the top level folder before uploading files within the loop
            if not self.is_user_ws_root(upload_dir):
                # if it is not the /Users/example@example.com/ root path, don't create the folder
                resp_mkdirs = self.post(WS_MKDIRS, {'path': upload_dir})
            for f in files:
                print("Uploading: {0}".format(f))
                # create the local file path to load the DBC file
                local_file_path = os.path.join(root, f)
                # create the ws full file path including filename
                ws_file_path = upload_dir + f
                # generate json args with binary data for notebook to upload to the workspace path
                nb_input_args = get_user_import_args(local_file_path, ws_file_path)
                # call import to the workspace
                if self.is_verbose():
                    print("Path: {0}".format(nb_input_args['path']))
                resp_upload = self.post(WS_IMPORT, nb_input_args)
