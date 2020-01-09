from dbclient import *
import os


class ScimClient(dbclient):

    def log_all_users(self, log_file='users.log'):
        user_log = self._export_dir + log_file
        users = self.get('/preview/scim/v2/Users')['Resources']
        with open(user_log, "w") as fp:
            for x in users:
                fp.write(json.dumps(x) + '\n')

    @staticmethod
    def is_member_a_user(member_json):
        if 'scim/v2/Users' in member_json['$ref']:
            return True
        return False

    def add_username_to_group(self, group_json):
        # add the userName field to json since ids across environments may not match
        members = group_json.get('members', None)
        new_members = []
        for m in members:
            m_id = m['value']
            if self.is_member_a_user(m):
                user_resp = self.get('/preview/scim/v2/Users/{0}'.format(m_id))
                m['userName'] = user_resp['userName']
                m['type'] = 'user'
            else:
                m['type'] = 'group'
            new_members.append(m)
        group_json['members'] = new_members
        return group_json

    def log_all_groups(self, group_log_dir='groups/'):
        group_dir = self._export_dir + group_log_dir
        os.makedirs(group_dir, exist_ok=True)
        group_list = self.get("/preview/scim/v2/Groups").get('Resources', None)
        for x in group_list:
            group_name = x['displayName']
            with open(group_dir + group_name, "w") as fp:
                fp.write(json.dumps(self.add_username_to_group(x)))

    def log_all_secrets(self, log_file='secrets.log'):
        secrets_log = self._export_dir + log_file
        secrets = self.get('/secrets/scopes/list')['scopes']
        with open(secrets_log, "w") as fp:
            for x in secrets:
                fp.write(json.dumps(x) + '\n')

    def get_user_id_mapping(self):
        # return a dict of the userName to id mapping of the new env
        user_list = self.get('/preview/scim/v2/Users').get('Resources', None)
        if user_list:
            user_id_dict = {}
            for user in user_list:
                user_id_dict[user['userName']] = user['id']
            return user_id_dict
        return None

    @staticmethod
    def assign_roles_args(roles_list):
        assign_args = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                       "Operations": [{"op": "add",
                                       "path": "roles",
                                       "value": roles_list}]}
        return assign_args

    def assign_group_roles(self, group_dir):
        # assign group role ACLs, which are only available via SCIM apis
        group_ids = self.get_group_ids()
        groups = os.listdir(group_dir)
        for group_name in groups:
            with open(group_dir + group_name, 'r') as fp:
                roles = json.loads(fp.read()).get('roles', None)
                if roles:
                    g_id = group_ids[group_name]
                    update_roles = self.assign_roles_args(roles)
                    up_resp = self.patch('/preview/scim/v2/Groups/{0}'.format(g_id), update_roles)
                    print(up_resp)

    def get_user_ids(self):
        # return a dict of username to user id mappings
        users = self.get('/preview/scim/v2/Users')['Resources']
        user_id = {}
        for user in users:
            user_id[user['userName']] = user['id']
        return user_id

    def get_group_ids(self):
        # return a dict of group displayName and id mappings
        groups = self.get('/preview/scim/v2/Groups').get('Resources', None)
        group_ids = {}
        for group in groups:
            group_ids[group['displayName']] = group['id']
        return group_ids

    def assign_user_roles(self, user_log_file='users.log'):
        user_log = self._export_dir + user_log_file
        # keys to filter from the user log to get the user / role mapping
        role_keys = ('userName', 'roles')
        # get current user id of the new environment, k,v = email, id
        user_ids = self.get_user_id_mapping()
        with open(user_log, 'r') as fp:
            for x in fp:
                user = json.loads(x)
                user_roles = {k: user[k] for k in role_keys if k in user}
                user_id = user_ids[user['userName']]
                role_keys = ('schemas', 'userName', 'entitlements', 'roles', 'groups')
                cur_user = self.get('/preview/scim/v2/Users/{0}'.format(user_id))
                update_user = {k: cur_user[k] for k in role_keys if k in cur_user}
                if 'roles' in update_user:
                    update_user['roles'] = update_user['roles'] + user_roles['roles']
                else:
                    update_user['roles'] = user_roles['roles']
                update_resp = self.put('/preview/scim/v2/Users/{0}'.format(user_id), update_user)

    @staticmethod
    def get_member_args(member_id_list):
        member_id_list_json = []
        for m_id in member_id_list:
            member_id_list_json.append({'value': '{0}'.format(m_id)})

        add_members_args = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op":"add",
                "value": {"members": member_id_list_json}
                }
            ]
        }
        return add_members_args

    def import_groups(self, group_dir):
        # list all the groups and create groups first
        groups = os.listdir(group_dir)
        create_args = {
            "schemas": [ "urn:ietf:params:scim:schemas:core:2.0:Group" ],
            "displayName": "default"
        }
        for x in groups:
            print('Creating group: {0}'.format(x))
            # set the create args displayName property aka group name
            create_args['displayName'] = x
            group_resp = self.post('/preview/scim/v2/Groups', create_args)

        # assign membership of users into the groups
        current_group_ids = self.get_group_ids()
        current_user_ids = self.get_user_ids()
        for group_name in groups:
            with open(group_dir + group_name, 'r') as fp:
                members = json.loads(fp.read()).get('members', None)
                if members:
                    member_id_list = []
                    for m in members:
                        if m['type'] == 'user':
                           member_id_list.append(current_user_ids[m['userName']])
                        else:
                            member_id_list.append(current_group_ids[m['display']])
                    add_members_json = self.get_member_args(member_id_list)
                    group_id = current_group_ids[group_name]
                    add_resp = self.patch('/preview/scim/v2/Groups/{0}'.format(group_id), add_members_json)

    def import_users(self, user_log):
        # first create the user identities with the required fields
        create_keys = ('emails', 'entitlements', 'displayName', 'name', 'userName')
        with open(user_log, 'r') as fp:
            for x in fp:
                user = json.loads(x)
                print("Creating user: {0}".format(user['userName']))
                user_create = {k: user[k] for k in create_keys if k in user}
                create_resp = self.post('/preview/scim/v2/Users', user_create)

    def import_all_users_and_groups(self, user_log_file='users.log', group_log_dir='groups/', is_aws=True):
        user_log = self._export_dir + user_log_file
        group_dir = self._export_dir + group_log_dir

        self.import_users(user_log)
        self.import_groups(group_dir)
        # assign the users to IAM roles if on AWS
        if is_aws:
            self.assign_group_roles(group_dir)
            print("Done")
#            self.assign_user_roles(user_log_file)
