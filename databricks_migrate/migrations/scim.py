import json
import os

from databricks_cli.sdk import ApiClient, SecretService

from databricks_migrate import log
from databricks_migrate.migrations import BaseMigrationClient


class ScimMigrations(BaseMigrationClient):

    def __init__(self, api_client: ApiClient, api_client_v1_2: ApiClient, export_dir, is_aws, skip_failed, verify_ssl):
        super().__init__(api_client, api_client_v1_2, export_dir, is_aws, skip_failed, verify_ssl)
        self.secret_service = SecretService(api_client)

    def log_all_users(self, log_file='users.log'):
        user_log = self._export_dir + log_file
        users = self.api_client.perform_query("GET", '/preview/scim/v2/Users', headers=self.scim_read_headers).get(
            'Resources', None)
        if users:
            with open(user_log, "w") as fp:
                for x in users:
                    fullname = x.get('name', None)
                    if fullname:
                        given_name = fullname.get('givenName', None)
                        # if user is an admin, skip this user entry
                        if x['userName'] == 'admin' and given_name == 'Administrator':
                            continue
                    fp.write(json.dumps(x) + '\n')
        else:
            log.info("Users returned an empty object")

    @staticmethod
    def is_member_a_user(member_json):
        if 'scim/v2/Users' in member_json['$ref']:
            return True
        return False

    def add_username_to_group(self, group_json):
        # add the userName field to json since ids across environments may not match
        members = group_json.get('members', None)
        new_members = []
        if members:
            for m in members:
                m_id = m['value']
                if self.is_member_a_user(m):
                    user_resp = self.api_client.perform_query("GET", '/preview/scim/v2/Users/{0}'.format(m_id),
                                                              headers=self.scim_read_headers)
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
        group_list = self.api_client.perform_query("GET", "/preview/scim/v2/Groups",
                                                   headers=self.scim_read_headers).get('Resources', None)
        if group_list:
            for x in group_list:
                group_name = x['displayName']
                with open(group_dir + group_name, "w") as fp:
                    fp.write(json.dumps(self.add_username_to_group(x)))

    def log_all_secrets(self, log_file='secrets.log'):
        secrets_log = self._export_dir + log_file
        secrets = self.secret_service.list_scopes()['scopes']
        with open(secrets_log, "w") as fp:
            for x in secrets:
                fp.write(json.dumps(x) + '\n')

    def get_user_id_mapping(self):
        # return a dict of the userName to id mapping of the new env
        user_list = self.api_client.perform_query("GET", '/preview/scim/v2/Users',
                                                  headers=self.scim_read_headers).get('Resources', None)
        if user_list:
            user_id_dict = {}
            for user in user_list:
                user_id_dict[user['userName']] = user['id']
            return user_id_dict
        return None

    @staticmethod
    def assign_roles_args(roles_list):
        # roles list passed from file, which is in proper patch arg format already
        # this method is used to patch the group IAM roles
        assign_args = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                       "Operations": [{"op": "add",
                                       "path": "roles",
                                       "value": roles_list}]}
        return assign_args

    def assign_group_roles(self, group_dir):
        # assign group role ACLs, which are only available via SCIM apis
        group_ids = self.get_group_ids()
        if not os.path.exists(group_dir):
            log.info("No groups defined. Skipping group entitlement assignment")
            return
        groups = os.listdir(group_dir)
        for group_name in groups:
            with open(group_dir + group_name, 'r') as fp:
                roles = json.loads(fp.read()).get('roles', None)
                if roles:
                    g_id = group_ids[group_name]
                    update_roles = self.assign_roles_args(roles)
                    up_resp = self.api_client.perform_query("PATCH", '/preview/scim/v2/Groups/{0}'.format(g_id),
                                                            data=update_roles, headers=self.scim_read_headers)

    def get_user_ids(self):
        # return a dict of username to user id mappings
        users = self.api_client.perform_query("GET", '/preview/scim/v2/Users',
                                              headers=self.scim_read_headers)['Resources']
        user_id = {}
        for user in users:
            user_id[user['userName']] = user['id']
        return user_id

    def get_group_ids(self):
        # return a dict of group displayName and id mappings
        groups = self.api_client.perform_query("GET", '/preview/scim/v2/Groups',
                                               headers=self.scim_read_headers).get('Resources', None)
        group_ids = {}
        for group in groups:
            group_ids[group['displayName']] = group['id']
        return group_ids

    @staticmethod
    def add_roles_arg(roles_list):
        # this builds the args from a list of IAM roles. diff built from user logfile
        role_values = [{'value': x} for x in roles_list]
        patch_roles_arg = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "add",
                    "path": "roles",
                    "value": role_values
                }
            ]
        }
        return patch_roles_arg

    def assign_user_roles(self, user_log_file='users.log'):
        """
        assign user roles that are missing after adding group assignment
        Note: There is a limitation in the exposed API. If a user is assigned a role permission & the permission
        is granted via a group, we can't distinguish the difference. Only group assignment will be migrated.
        :param user_log_file: logfile of all user properties
        :return:
        """
        user_log = self._export_dir + user_log_file
        if not os.path.exists(user_log):
            log.info("Skipping user entitlement assignment. Logfile does not exist")
            return
        # keys to filter from the user log to get the user / role mapping
        old_role_keys = ('userName', 'roles')
        cur_role_keys = ('schemas', 'userName', 'entitlements', 'roles', 'groups')
        # get current user id of the new environment, k,v = email, id
        user_ids = self.get_user_id_mapping()
        with open(user_log, 'r') as fp:
            # loop through each user in the file
            for line in fp:
                user = json.loads(line)
                user_roles = {k: user[k] for k in old_role_keys if k in user}
                # get the current registered user id
                user_id = user_ids[user['userName']]
                # get the current users settings
                cur_user = self.api_client.perform_query("GET", '/preview/scim/v2/Users/{0}'.format(user_id),
                                                         headers=self.scim_read_headers)
                # get the current users IAM roles
                current_roles = cur_user.get('roles', None)
                if current_roles:
                    cur_role_values = set([x['value'] for x in current_roles])
                else:
                    cur_role_values = set()
                # get the users saved IAM roles from the export
                saved_roles = user_roles.get('roles', None)
                if saved_roles:
                    saved_role_values = set([y['value'] for y in saved_roles])
                else:
                    saved_role_values = set()
                roles_needed = list(saved_role_values - cur_role_values)
                if roles_needed:
                    # get the json to add the roles to the user profile
                    patch_roles = self.add_roles_arg(roles_needed)
                    update_resp = self.api_client.perform_query("PATCH", '/preview/scim/v2/Users/{0}'.format(user_id),
                                                                data=patch_roles, headers=self.scim_read_headers)

    @staticmethod
    def get_member_args(member_id_list):
        member_id_list_json = []
        for m_id in member_id_list:
            member_id_list_json.append({'value': '{0}'.format(m_id)})

        add_members_args = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op": "add",
                "value": {"members": member_id_list_json}
            }
            ]
        }
        return add_members_args

    def import_groups(self, group_dir):
        # list all the groups and create groups first
        if not os.path.exists(group_dir):
            log.info("No groups to import.")
            return
        groups = os.listdir(group_dir)
        create_args = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": "default"
        }
        for x in groups:
            log.info('Creating group: {0}'.format(x))
            # set the create args displayName property aka group name
            create_args['displayName'] = x
            group_resp = self.api_client.perform_query("POST", '/preview/scim/v2/Groups',
                                                       data=create_args, headers=self.scim_read_headers)

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
                    add_resp = self.api_client.perform_query("PATCH", '/preview/scim/v2/Groups/{0}'.format(group_id),
                                                             data=add_members_json, headers=self.scim_read_headers)

    def import_users(self, user_log):
        # first create the user identities with the required fields
        create_keys = ('emails', 'entitlements', 'displayName', 'name', 'userName')
        if not os.path.exists(user_log):
            log.info("No users to import.")
            return
        with open(user_log, 'r') as fp:
            for x in fp:
                user = json.loads(x)
                log.info("Creating user: {0}".format(user['userName']))
                user_create = {k: user[k] for k in create_keys if k in user}
                create_resp = self.api_client.perform_query("POST", '/preview/scim/v2/Users',
                                                            data=user_create, headers=self.scim_read_headers)

    def import_all_users_and_groups(self, user_log_file='users.log', group_log_dir='groups/'):
        user_log = self._export_dir + user_log_file
        group_dir = self._export_dir + group_log_dir

        self.import_users(user_log)
        self.import_groups(group_dir)
        # assign the users to IAM roles if on AWS
        if self.is_aws():
            log.info("Update group role assignment")
            self.assign_group_roles(group_dir)
            log.info("Update user role assignment")
            self.assign_user_roles(user_log_file)
            log.info("Done")
