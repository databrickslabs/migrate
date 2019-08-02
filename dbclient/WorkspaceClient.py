from dbclient import *
import os, base64

WS_LIST = "/workspace/list"
WS_STATUS = "/workspace/get-status"
WS_MKDIRS = "/workspace/mkdirs"
WS_IMPORT = "/workspace/import"
WS_EXPORT = "/workspace/export"
LS_ZONES = "/clusters/list-zones"

class WorkspaceClient(dbclient):

  def log_all_users(self, log_file='users.log'):
    user_log = self._export_dir + log_file
    users = self.get('/preview/scim/v2/Users')['Resources']
    with open(user_log, "w") as fp:
      for x in users:
        fp.write(json.dumps(x) + '\n')
  
  def log_all_groups(self, group_log_dir='groups/'):
    group_dir = self._export_dir + group_log_dir
    os.makedirs(group_dir, exist_ok=True)
    group_list = self.get("/groups/list")['group_names']
    for x in group_list:
      with open(group_dir + x, "w") as fp:
        get_args = {'group_name': x}
        members = self.get('/groups/list-members', get_args)
        fp.write(json.dumps(members))

  def log_all_secrets(self, log_file='secrets.log'):
    secrets_log = self._export_dir + log_file
    secrets = self.get('/secrets/scopes/list')['scopes']
    with open(secerts_logs, "w") as fp:
      for x in secrets:
        fp.write(json.dumps(x) + '\n')

  def download_notebooks(self, ws_log_file='user_workspace.log', ws_dir='artifacts/'):
    ws_log = self._export_dir + ws_log_file
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

  def log_all_workspace_items(self, ws_path='/', workspace_log_file='user_workspace.log', libs_log_file='libraries.log'):
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
          self.log_all_workspace_items(ws_path=f, workspace_log_file=workspace_log_file, libs_log_file=libs_log_file)

