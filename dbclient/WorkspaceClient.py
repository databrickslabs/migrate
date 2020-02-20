from dbclient import *
import os, base64

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


class WorkspaceClient(dbclient):

    def export_user_home(self, username, local_export_dir):
        """
        Export the provided user's home directory
        :param username: user's home directory to export
        :return: None
        """
        user_export_dir = self._export_dir + local_export_dir
        user_root = '/Users/' + username.rstrip().lstrip()
        self._export_dir = user_export_dir + '/{0}/'.format(username)
        print("Export path: {0}".format(self._export_dir))
        self.log_all_workspace_items(ws_path=user_root)
        self.download_notebooks(ws_dir='user_artifacts/')

    def download_notebooks(self, ws_log_file='user_workspace.log', ws_dir='artifacts/'):
        """
        Loop through all notebook paths in the logfile and download individual notebooks
        :param ws_log_file: logfile for all notebook paths in the workspace
        :param ws_dir: export directory to store all notebooks
        :return: None
        """
        ws_log = self._export_dir + ws_log_file
        if not os.path.exists(ws_log):
            raise Exception("Run --workspace first to download full log of all notebooks.")
        with open(ws_log, "r") as fp:
            for nb in fp:
                self.download_notebook_helper(nb.rstrip(), export_dir=self._export_dir + ws_dir)

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
        with open(self._export_dir + 'failed_notebooks.log', 'a') as err_log:
            if resp.get('error_code', None):
                err_log.write(notebook_path + '\n')
                return {'error_code': resp.get('error_code'), 'path': notebook_path}
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
        supported_types = set('DIRECTORY', 'NOTEBOOK', 'LIBRARY')
        if item_type not in supported_types:
            raise ValueError('Unsupported type provided: {0}.\n. Supported types: {1}'.format(item_type,
                                                                                              str(supported_types)))
        filtered_list = list(self.my_map(lambda y: y.get('path', None),
                                         filter(lambda x: x.get('object_type', None) == item_type, item_list)))
        return filtered_list

    def log_all_workspace_items(self, ws_path='/', workspace_log_file='user_workspace.log',
                                libs_log_file='libraries.log'):
        """
        Loop and log all workspace items to download them at a later time
        :param ws_path: root path to log all the items of the notebook workspace
        :param workspace_log_file: logfile to store all the paths of the notebooks
        :param libs_log_file: library logfile to store workspace libraries
        :return:
        """
        if ws_path == '/':
            # default is the root path
            get_args = {'path': '/'}
        else:
            get_args = {'path': ws_path}

        if not os.path.exists(self._export_dir):
            os.makedirs(self._export_dir, exist_ok=True)
        items = self.get(WS_LIST, get_args).get('objects', None)
        if self.is_verbose():
            print("Listing: {0}".format(get_args['path']))
        if items is not None:
            # list all the users folders only
            folders = self.filter_workspace_items(items, 'DIRECTORY')
            # should be no notebooks, but lets filter and can check later
            notebooks = self.filter_workspace_items(items, 'NOTEBOOK')
            libraries = self.filter_workspace_items(items, 'LIBRARY')
            # log file for
            workspace_log = self._export_dir + workspace_log_file
            libs_log = self._export_dir + libs_log_file
            with open(workspace_log, "a") as ws_fp, open(libs_log, "a") as libs_fp:
                for x in notebooks:
                    if self.is_verbose():
                        print("Saving path: {0}".format(x))
                    ws_fp.write(x + '\n')
                for y in libraries:
                    libs_fp.write(y + '\n')

            if folders:
                for f in folders:
                    self.log_all_workspace_items(ws_path=f, workspace_log_file=workspace_log_file,
                                                 libs_log_file=libs_log_file)

    @staticmethod
    def get_num_of_saved_users(export_dir):
        # get current number of saved workspaces
        user_home_dir = export_dir + 'Users'
        ls = os.listdir(user_home_dir)
        num_of_users = 0
        for x in ls:
            if os.path.isdir(user_home_dir + '/' + x):
                num_of_users += 1
        return num_of_users

    def get_current_users(self):
        # get the num of defined user home directories in the new workspace
        # if this is 0, we must create the users before importing the notebooks over.
        # we cannot create the users home directory since its a special type of directory
        ws_users = self.get(WS_LIST, {'path': '/Users/'}).get('objects', None)
        if ws_users:
            return len(ws_users)
        else:
            return 0

    @staticmethod
    def is_user_ws_item(ws_dir):
        path_list = [x for x in ws_dir.split('/') if x]
        if len(path_list) >= 2 and path_list[0] == 'Users':
            return True
        return False

    @staticmethod
    def is_user_ws_root(ws_dir):
        if ws_dir == '/Users/' or ws_dir == '/Users':
            return True
        path_list = [x for x in ws_dir.split('/') if x]
        if len(path_list) == 2 and path_list[0] == 'Users':
            return True
        return False

    @staticmethod
    def get_user(ws_dir):
        path_list = [x for x in ws_dir.split('/') if x]
        if len(path_list) < 2:
            raise ValueError("Error: Not a users workspace directory")
        return path_list[1]

    def does_user_exist(self, username):
        stat = self.get(WS_STATUS, {'path': '/Users/{0}'.format(username)})
        if stat.get('object_type', None) == 'DIRECTORY':
            return True
        return False

    def import_all_workspace_items(self, load_dir='logs/', export_dir='artifacts/', archive_missing=False):
        src_dir = load_dir + export_dir
        num_exported_users = self.get_num_of_saved_users(src_dir)
        num_current_users = self.get_current_users()
        if num_current_users == 0:
            print("No registered users in existing environment. Please import users / groups first.")
            raise ValueError("No registered users in the current environment")
        if (num_current_users < num_exported_users) and (not archive_missing):
            print("Exported number of user workspaces: {0}".format(num_exported_users))
            print("Current number of user workspaces: {0}".format(num_current_users))
            print("Re-run with the archive flag to load missing users into a separate directory")
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
