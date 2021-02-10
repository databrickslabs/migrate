from dbclient import *
import os
import json


class ScimClient(dbclient):

    def get_active_users(self):
        users = self.get('/preview/scim/v2/Users').get('Resources', None)
        return users if users else None

    def log_all_users(self, log_file='users.log'):
        user_log = self.get_export_dir() + log_file
        users = self.get('/preview/scim/v2/Users').get('Resources', None)
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
            print("Users returned an empty object")

    def log_single_user(self, user_email, log_file='single_user.log'):
        single_user_log = self.get_export_dir() + log_file
        users = self.get_active_users()
        found_user = False
        for user in users:
            current_email = user['emails'][0]['value']
            if user_email == current_email:
                found_user = True
                print(user)
                with open(single_user_log, 'w') as fp:
                    fp.write(json.dumps(user) + '\n')
        if not found_user:
            print("User not found. Emails are case sensitive. Please verify email address")

    def import_single_user(self, user_email, log_file='single_user.log'):
        single_user_log = self.get_export_dir() + log_file
        resp = self.import_users(single_user_log)

    def get_users_from_log(self, users_log='users.log'):
        """
        fetch a list of user names from the users log file
        meant to be used during group exports where the user list is a subset of users
        :param users_log:
        :return: a list of usernames that help identify their workspace paths
        """
        user_logfile = self.get_export_dir() + users_log
        username_list = []
        with open(user_logfile, 'r') as fp:
            for u in fp:
                user_json = json.loads(u)
                username_list.append(user_json.get('userName'))
        return username_list

    @staticmethod
    def is_member_a_user(member_json):
        if 'Users/' in member_json['$ref']:
            return True
        return False

    @staticmethod
    def is_member_a_group(member_json):
        if 'Groups/' in member_json['$ref']:
            return True
        return False

    @staticmethod
    def is_member_a_service_principal(member_json):
        if 'ServicePrincipals/' in member_json['$ref']:
            return True
        return False

    def add_username_to_group(self, group_json):
        # add the userName field to json since ids across environments may not match
        members = group_json.get('members', [])
        new_members = []
        for m in members:
            m_id = m['value']
            if self.is_member_a_user(m):
                user_resp = self.get('/preview/scim/v2/Users/{0}'.format(m_id))
                m['userName'] = user_resp['userName']
                m['type'] = 'user'
            elif self.is_member_a_group(m):
                m['type'] = 'group'
            elif self.is_member_a_service_principal(m):
                m['type'] = 'service-principal'
            else:
                m['type'] = 'unknown'
            new_members.append(m)
        group_json['members'] = new_members
        return group_json

    def log_all_groups(self, group_log_dir='groups/'):
        group_dir = self.get_export_dir() + group_log_dir
        os.makedirs(group_dir, exist_ok=True)
        group_list = self.get("/preview/scim/v2/Groups").get('Resources', [])
        for x in group_list:
            group_name = x['displayName']
            with open(group_dir + group_name, "w") as fp:
                fp.write(json.dumps(self.add_username_to_group(x)))

    @staticmethod
    def build_group_dict(group_list):
        group_dict = {}
        for group in group_list:
            group_dict[group.get('displayName')] = group
        return group_dict

    def log_groups_from_list(self, group_name_list, group_log_dir='groups/', users_logfile='users.log'):
        """
        take a list of groups and log all the members
        :param group_name_list: a list obj of group names
        :param group_log_dir:
        :param users_logfile: logfile to store the user log data
        :return: return a list of userNames to export their notebooks for the next api call
        """
        group_dir = self.get_export_dir() + group_log_dir
        os.makedirs(group_dir, exist_ok=True)
        group_list = self.get("/preview/scim/v2/Groups").get('Resources', [])
        group_dict = self.build_group_dict(group_list)
        member_id_list = []
        for group_name in group_name_list:
            group_details = group_dict[group_name]
            members_list = group_details.get('members', [])
            filtered_users = list(filter(lambda y: 'Users' in y.get('$ref', None), members_list))
            filtered_sub_groups = list(filter(lambda y: 'Groups' in y.get('$ref', None), members_list))
            if filtered_sub_groups:
                sub_group_names = list(map(lambda z: z.get('display'), filtered_sub_groups))
                group_name_list.extend(sub_group_names)
            member_id_list.extend(list(map(lambda y: y['value'], filtered_users)))
            with open(group_dir + group_name, "w") as fp:
                group_details.pop('roles', None)  # removing the roles field from the groups arg
                fp.write(json.dumps(self.add_username_to_group(group_details)))
        users_log = self.get_export_dir() + users_logfile
        user_names_list = []
        with open(users_log, 'w') as u_fp:
            for mid in member_id_list:
                print('Exporting', mid)
                api = f'/preview/scim/v2/Users/{mid}'
                user_resp = self.get(api)
                user_resp.pop('roles', None)  # remove roles since those can change during the migration
                user_resp.pop('http_status_code', None)  # remove unnecessary params
                user_names_list.append(user_resp.get('userName'))
                u_fp.write(json.dumps(user_resp) + '\n')
        return user_names_list

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
        # roles list passed from file, which is in proper patch arg format already
        # this method is used to patch the group IAM roles
        assign_args = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                       "Operations": [{"op": "add",
                                       "path": "roles",
                                       "value": roles_list}]}
        return assign_args

    @staticmethod
    def assign_entitlements_args(entitlements_list):
        # roles list passed from file, which is in proper patch arg format already
        # this method is used to patch the group IAM roles
        assign_args = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                       "Operations": [{"op": "add",
                                       "path": "entitlements",
                                       "value": entitlements_list}]}
        return assign_args

    def assign_group_entitlements(self, group_dir):
        # assign group role ACLs, which are only available via SCIM apis
        group_ids = self.get_current_group_ids()
        if not os.path.exists(group_dir):
            print("No groups defined. Skipping group entitlement assignment")
            return
        groups = os.listdir(group_dir)
        for group_name in groups:
            with open(group_dir + group_name, 'r') as fp:
                group_data = json.loads(fp.read())
                entitlements = group_data.get('entitlements', None)
                if entitlements:
                    g_id = group_ids[group_name]
                    update_entitlements = self.assign_entitlements_args(entitlements)
                    up_resp = self.patch(f'/preview/scim/v2/Groups/{g_id}', update_entitlements)
                    print(up_resp)

    def assign_group_roles(self, group_dir):
        # assign group role ACLs, which are only available via SCIM apis
        group_ids = self.get_current_group_ids()
        if not os.path.exists(group_dir):
            print("No groups defined. Skipping group entitlement assignment")
            return
        groups = os.listdir(group_dir)
        for group_name in groups:
            with open(group_dir + group_name, 'r') as fp:
                group_data = json.loads(fp.read())
                roles = group_data.get('roles', None)
                if roles:
                    g_id = group_ids[group_name]
                    update_roles = self.assign_roles_args(roles)
                    up_resp = self.patch(f'/preview/scim/v2/Groups/{g_id}', update_roles)
                    print(up_resp)
                entitlements = group_data.get('entitlements', None)
                if entitlements:
                    g_id = group_ids[group_name]
                    update_entitlements = self.assign_entitlements_args(entitlements)
                    up_resp = self.patch(f'/preview/scim/v2/Groups/{g_id}', update_entitlements)
                    print(up_resp)

    def get_current_user_ids(self):
        # return a dict of user email to user id mappings
        users = self.get('/preview/scim/v2/Users')['Resources']
        user_id = {}
        for user in users:
            user_id[user['emails'][0]['value']] = user['id']
        return user_id

    def get_old_user_emails(self, users_logfile='users.log'):
        # return a dictionary of { old_id : email } from the users log
        users_log = self.get_export_dir() + users_logfile
        email_dict = {}
        with open(users_log, 'r') as fp:
            for x in fp:
                user = json.loads(x)
                email_dict[user['id']] = user['emails'][0]['value']
        return email_dict

    def get_current_group_ids(self):
        # return a dict of group displayName and id mappings
        groups = self.get('/preview/scim/v2/Groups').get('Resources', None)
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

    def assign_user_entitlements(self, user_log_file='users.log'):
        """
        assign user entitlements to allow cluster create, job create, sql analytics etc
        :param user_log_file:
        :return:
        """
        user_log = self.get_export_dir() + user_log_file
        if not os.path.exists(user_log):
            print("Skipping user entitlement assignment. Logfile does not exist")
            return
        user_ids = self.get_user_id_mapping()
        with open(user_log, 'r') as fp:
            # loop through each user in the file
            for line in fp:
                user = json.loads(line)
                # add the users entitlements
                user_entitlements = user.get('entitlements', None)
                # get the current registered user id
                user_id = user_ids[user['userName']]
                if user_entitlements:
                    entitlements_args = self.assign_entitlements_args(user_entitlements)
                    update_resp = self.patch(f'/preview/scim/v2/Users/{user_id}', entitlements_args)

    def assign_user_roles(self, user_log_file='users.log'):
        """
        assign user roles that are missing after adding group assignment
        Note: There is a limitation in the exposed API. If a user is assigned a role permission & the permission
        is granted via a group, we can't distinguish the difference. Only group assignment will be migrated.
        :param user_log_file: logfile of all user properties
        :return:
        """
        user_log = self.get_export_dir() + user_log_file
        if not os.path.exists(user_log):
            print("Skipping user entitlement assignment. Logfile does not exist")
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
                cur_user = self.get('/preview/scim/v2/Users/{0}'.format(user_id))
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
                    update_resp = self.patch(f'/preview/scim/v2/Users/{user_id}', patch_roles)

    @staticmethod
    def get_member_args(member_id_list):
        """
        helper function to form the json args to the patch request to update group memberships
        :param member_id_list: member ids to add to a specific group
        :return: dict args for the patch operation
        """
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

    @staticmethod
    def is_user(member_json):
        # currently a workaround to get whether the member is a user or group
        # check the ref instead of the type field
        # once fixed, the type should be `user` or `group` in lowercase
        if 'Users/' in member_json['$ref']:
            return True
        return False

    @staticmethod
    def is_group(member_json):
        # currently a workaround to get whether the member is a user or group
        # check the ref instead of the type field
        # once fixed, the type should be `user` or `group` in lowercase
        if 'Groups/' in member_json['$ref']:
            return True
        return False

    def import_groups(self, group_dir):
        # list all the groups and create groups first
        if not os.path.exists(group_dir):
            print("No groups to import.")
            return
        groups = os.listdir(group_dir)
        create_args = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": "default"
        }
        for x in groups:
            print('Creating group: {0}'.format(x))
            # set the create args displayName property aka group name
            create_args['displayName'] = x
            group_resp = self.post('/preview/scim/v2/Groups', create_args)

        # dict of { group_name : group_id }
        current_group_ids = self.get_current_group_ids()
        # dict of { email : current_user_id }
        current_user_ids = self.get_current_user_ids()
        # dict of { old_user_id : email }
        old_user_emails = self.get_old_user_emails()
        for group_name in groups:
            with open(group_dir + group_name, 'r') as fp:
                members = json.loads(fp.read()).get('members', None)
                if members:
                    # grab a list of ids to add either groups or users to this current group
                    member_id_list = []
                    for m in members:
                        if self.is_user(m):
                            old_email = old_user_emails[m['value']]
                            this_user_id = current_user_ids.get(old_email, '')
                            if not this_user_id:
                                raise ValueError(f'Unable to find user {old_email} in the new workspace. '
                                                 f'This users email case has changed and needs to be updated with '
                                                 f'the --replace-old-email and --update-new-email options')
                            member_id_list.append(this_user_id)
                        elif self.is_group(m):
                            this_group_id = current_group_ids.get(m['display'])
                            member_id_list.append(this_group_id)
                        else:
                            print("Skipping service principal members and other identities not within users/groups")
                    add_members_json = self.get_member_args(member_id_list)
                    group_id = current_group_ids[group_name]
                    add_resp = self.patch('/preview/scim/v2/Groups/{0}'.format(group_id), add_members_json)

    def import_users(self, user_log):
        # first create the user identities with the required fields
        create_keys = ('emails', 'entitlements', 'displayName', 'name', 'userName')
        if not os.path.exists(user_log):
            print("No users to import.")
            return
        with open(user_log, 'r') as fp:
            for x in fp:
                user = json.loads(x)
                print("Creating user: {0}".format(user['userName']))
                user_create = {k: user[k] for k in create_keys if k in user}
                create_resp = self.post('/preview/scim/v2/Users', user_create)

    def import_all_users_and_groups(self, user_log_file='users.log', group_log_dir='groups/'):
        user_log = self.get_export_dir() + user_log_file
        group_dir = self.get_export_dir() + group_log_dir

        self.import_users(user_log)
        self.import_groups(group_dir)
        # assign the users to IAM roles if on AWS
        if self.is_aws():
            print("Update group role assignments")
            self.assign_group_roles(group_dir)
            print("Update user role assignments")
            self.assign_user_roles(user_log_file)
            print("Done")
        # need to separate role assignment and entitlements to support Azure
        print("Updating groups entitlements")
        self.assign_group_entitlements(group_dir)
        print("Updating users entitlements")
        self.assign_user_entitlements(user_log_file)
