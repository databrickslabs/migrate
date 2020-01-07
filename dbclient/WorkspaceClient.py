from dbclient import *
import os, base64

WS_LIST = "/workspace/list"
WS_STATUS = "/workspace/get-status"
WS_MKDIRS = "/workspace/mkdirs"
WS_IMPORT = "/workspace/import"
WS_EXPORT = "/workspace/export"
LS_ZONES = "/clusters/list-zones"


class WorkspaceClient(dbclient):

    def get_user_import_args(self, full_local_path, nb_full_path):
        fp = open(full_local_path, "rb")

        in_args = {
            "content": base64.encodebytes(fp.read()),
            "path": nb_full_path[:-4],
            "format": "DBC"
        }
        return in_args

    def download_notebooks(self, ws_log_file='user_workspace.log', ws_dir='artifacts/'):
        ws_log = self._export_dir + ws_log_file
        if not os.path.exists(ws_log):
            raise Exception("Run --workspace first to download full log of all notebooks.")
        with open(ws_log, "r") as fp:
            for nb in fp:
                self.download_notebook_helper(nb.rstrip(), export_dir=self._export_dir + ws_dir)

    def download_notebook_helper(self, notebook_path, export_dir='artifacts/'):
        get_args = {'path': notebook_path, 'format': 'DBC'}
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

    def log_all_workspace_items(self, ws_path='/', workspace_log_file='user_workspace.log',
                                libs_log_file='libraries.log'):
        if ws_path == '/':
            # default is the root path
            get_args = {'path': '/'}
        else:
            get_args = {'path': ws_path}

        items = self.get(WS_LIST, get_args).get('objects', None)
        if items is not None:
            # list all the users folders only
            folders = list(self.my_map(lambda y: y.get('path', None),
                                       filter(lambda x: x.get('object_type', None) == 'DIRECTORY', items)))
            # should be no notebooks, but lets filter and can check later
            notebooks = list(self.my_map(lambda y: y.get('path', None),
                                         filter(lambda x: x.get('object_type', None) == 'NOTEBOOK', items)))
            libraries = list(self.my_map(lambda y: y.get('path', None),
                                         filter(lambda x: x.get('object_type', None) == 'LIBRARY', items)))

            # log file for
            workspace_log = self._export_dir + workspace_log_file
            libs_log = self._export_dir + libs_log_file
            with open(workspace_log, "a") as ws_fp, open(libs_log, "a") as libs_fp:
                for x in notebooks:
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
        if archive_missing:
            archive_users = set()
            for root, subdirs, files in os.walk(src_dir):
                # replace the local directory with empty string to get the notebook workspace directory
                nb_dir = '/' + root.replace(src_dir, '')
                upload_dir = nb_dir
                if not nb_dir == '/':
                    upload_dir = nb_dir + '/'
                if self.is_user_ws_item(upload_dir):
                    ws_user = self.get_user(upload_dir)
                    if ws_user in archive_users:
                        upload_dir = upload_dir.replace('Users', 'Archive', 1)
                    elif not self.does_user_exist(ws_user):
                        # add the user to the cache / set of missing users
                        archive_users.add(ws_user)
                        # append the archive path to the upload directory
                        upload_dir = upload_dir.replace('Users', 'Archive', 1)
                    else:
                        print("User workspace exists: {0}".format(ws_user))
                # make the top level folder before uploading files within the loop
                if not self.is_user_ws_root(upload_dir):
                    # if it is not the /Users/example@example.com/ root path, don't create the folder
                    resp_mkdirs = self.post(WS_MKDIRS, {'path': upload_dir})
                for f in files:
                    # create the local file path to load the DBC file
                    localFilePath = os.path.join(root, f)
                    # create the ws full file path including filename
                    wsFilePath = upload_dir + f
                    # generate json args with binary data for notebook to upload to the workspace path
                    nb_input_args = self.get_user_import_args(localFilePath, wsFilePath)
                    # call import to the workspace
                    resp_upload = self.post(WS_IMPORT, nb_input_args)
