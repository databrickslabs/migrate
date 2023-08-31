from dbclient import *
import os
import time
from timeit import default_timer as timer
import base64
import logging_utils
import logging
import wmconstants

class SecretsClient(ClustersClient):

    def get_secret_scopes_list(self):
        scopes_list = self.get('/secrets/scopes/list').get('scopes', [])
        return scopes_list

    def get_secrets(self, scope_name):
        secrets_list = self.get('/secrets/list', {'scope': scope_name}).get('secrets', [])
        return secrets_list

    def get_secret_value(self, scope_name, secret_key, cid, ec_id, error_logger):
        cmd_set_value = f"value = dbutils.secrets.get(scope = '{scope_name}', key = '{secret_key}')"
        cmd_convert_b64 = "import base64; b64_value = base64.b64encode(value.encode('ascii'))"
        cmd_get_b64 = "print(b64_value.decode('ascii'))"
        results_set = self.submit_command(cid, ec_id, cmd_set_value)
        results_convert = self.submit_command(cid, ec_id, cmd_convert_b64)
        results_get = self.submit_command(cid, ec_id, cmd_get_b64)
        if logging_utils.log_response_error(error_logger, results_set) \
                or logging_utils.log_response_error(error_logger, results_convert) \
                or logging_utils.log_response_error(error_logger, results_get):
            return None
        else:
            return results_get.get('data')

    def log_all_secrets(self, cluster_name=None, log_dir='secret_scopes/'):
        scopes_dir = self.get_export_dir() + log_dir
        scopes_list = self.get_secret_scopes_list()
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.SECRET_OBJECT, self.get_export_dir())
        os.makedirs(scopes_dir, exist_ok=True)
        start = timer()
        cid = self.start_cluster_by_name(cluster_name) if cluster_name else self.launch_cluster()
        time.sleep(5)
        ec_id = self.get_execution_context(cid)
        for scope_json in scopes_list:
            scope_name = scope_json.get('name')
            secrets_list = self.get_secrets(scope_name)
            if logging_utils.log_response_error(error_logger, secrets_list):
                continue
            scopes_logfile = scopes_dir + scope_name
            try:
                with open(scopes_logfile, 'w', encoding="utf-8") as fp:
                    for secret_json in secrets_list:
                        secret_name = secret_json.get('key')
                        b64_value = self.get_secret_value(scope_name, secret_name, cid, ec_id, error_logger)
                        s_json = {'name': secret_name, 'value': b64_value}
                        fp.write(json.dumps(s_json) + '\n')
            except ValueError as error:
                if "embedded null byte" in str(error):
                    error_msg = f"{scopes_logfile} has bad name and hence cannot open: {str(error)} Skipping.."
                    logging.error(error_msg)
                    error_logger.error(error_msg)
                else:
                    raise error


    def log_all_secrets_acls(self, log_name='secret_scopes_acls.log'):
        acls_file = self.get_export_dir() + log_name
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_EXPORT, wmconstants.SECRET_OBJECT, self.get_export_dir())
        scopes_list = self.get_secret_scopes_list()
        with open(acls_file, 'w', encoding="utf-8") as fp:
            for scope_json in scopes_list:
                scope_name = scope_json.get('name', None)
                resp = self.get('/secrets/acls/list', {'scope': scope_name})
                if logging_utils.log_response_error(error_logger, resp):
                    return
                else:
                    resp['scope_name'] = scope_name
                    fp.write(json.dumps(resp) + '\n')

    def load_acl_dict(self, acls_log_name='secret_scopes_acls.log'):
        acls_log = self.get_export_dir() + acls_log_name
        # create a dict by scope name to lookup and fetch the ACLs easily
        acls_dict = {} # d[scope_name] = {'MANAGED' : [list_of_members], 'READ': [list_of_members] .. }
        with open(acls_log, 'r', encoding="utf-8") as log_fp:
            for acl in log_fp:
                acl_json = json.loads(acl)
                s_name = acl_json.get('scope_name')
                all_perms = acl_json.get('items', [])
                scope_perms = {}
                for x in all_perms:
                    principal = x.get('principal')
                    perm = x.get('permission')
                    if perm in scope_perms:
                        scope_perms[perm].append(principal)
                    else:
                        scope_perms[perm] = [principal]
                acls_dict[s_name] = scope_perms
#        print(json.dumps(acls_dict, indent=True))
        return acls_dict

    @staticmethod
    def has_users_can_manage_permission(scope_name, acl_dict, non_premium):
        """
        returns whether the users group has access permissions
        returns the true if we can have them
        :scope_name: string for the secret scope name
        :acl_dict: ACLs dict ordered by permission keys, e.g. (MANAGE, USE, etc.)
        :non_premium: bool indicating that this is not a premium+ workspace
        """
        if non_premium:
            return True

        scope_perms = acl_dict.get(scope_name)
        # list of users/groups to manage the permissions
        manage_perms = scope_perms.get('MANAGE', [])
        if 'users' in manage_perms:
            return True
        return False

    @staticmethod
    def get_all_other_permissions(scope_name, acl_dict):
        """
        get the rest of the permissions for the secret scope besides `users` who can manage
        """
        scope_perms = acl_dict.get(scope_name)
        can_manage_perms = set(scope_perms.get('MANAGE', []))
        if 'users' in can_manage_perms:
            can_manage_perms.remove('users')
            if can_manage_perms:
                # if the can_manage_perms is not empty, set it with `users` removed
                scope_perms['MANAGE'] = can_manage_perms
            else:
                scope_perms.pop('MANAGE')
        return scope_perms

    def import_all_secrets(self, log_dir='secret_scopes/'):
        scopes_dir = self.get_export_dir() + log_dir
        error_logger = logging_utils.get_error_logger(
            wmconstants.WM_IMPORT, wmconstants.SECRET_OBJECT, self.get_export_dir())
        scopes_acl_dict = self.load_acl_dict()
        for root, subdirs, files in self.walk(scopes_dir):
            for scope_name in files:
                file_path = root + scope_name
                # print('Log file: ', file_path)
                # check if scopes acls are empty, then skip
                if scopes_acl_dict.get(scope_name, None) is None and not self.bypass_secret_acl:
                    print("Scope is empty with no manage permissions. Skipping...")
                    continue
                # check if users has can manage perms then we can add during creation time
                has_user_manage = self.has_users_can_manage_permission(scope_name, scopes_acl_dict, self.bypass_secret_acl)
                create_scope_args = {'scope': scope_name}
                if has_user_manage:
                    create_scope_args['initial_manage_principal'] = 'users'
                other_permissions = self.get_all_other_permissions(scope_name, scopes_acl_dict)
                create_resp = self.post('/secrets/scopes/create', create_scope_args)
                logging_utils.log_response_error(
                    error_logger, create_resp, ignore_error_list=['RESOURCE_ALREADY_EXISTS'])
                if other_permissions:
                    # use this dict minus the `users:MANAGE` permissions and apply the other permissions to the scope
                    for perm, principal_list in other_permissions.items():
                        put_acl_args = {"scope": scope_name,
                                        "permission": perm}
                        for x in principal_list:
                            put_acl_args["principal"] = x
                            logging.info(put_acl_args)
                            put_resp = self.post('/secrets/acls/put', put_acl_args)
                            logging_utils.log_response_error(error_logger, put_resp)
                # loop through the scope and create the k/v pairs
                with open(file_path, 'r', encoding="utf-8") as fp:
                    for s in fp:
                        s_dict = json.loads(s)
                        k = s_dict.get('name')
                        v = s_dict.get('value')
                        if 'WARNING: skipped' in v:
                            error_logger.error(f"Skipping scope {scope_name} as value is corrupted due to being too large \n")
                            continue
                        try:
                            put_secret_args = {'scope': scope_name,
                                               'key': k,
                                               'string_value': base64.b64decode(v.encode('ascii')).decode('ascii')}
                            put_resp = self.post('/secrets/put', put_secret_args)
                            logging_utils.log_response_error(error_logger, put_resp)
                        except Exception as error:
                            if "Invalid base64-encoded string" in str(error) or 'decode' in str(error) or "padding" in str(error):
                                error_msg = f"secret_scope: {scope_name} has invalid invalid data characters: {str(error)} skipping.. and logging to error file."
                                logging.error(error_msg)
                                error_logger.error(error_msg)

                            else:
                                raise error
