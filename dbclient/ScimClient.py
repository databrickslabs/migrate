from dbclient import *
import os


class ScimClient(dbclient):

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
        with open(secrets_log  , "w") as fp:
            for x in secrets:
                fp.write(json.dumps(x) + '\n')
