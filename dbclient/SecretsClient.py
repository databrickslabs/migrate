from dbclient import *
import os
import time
from timeit import default_timer as timer


class SecretsClient(ClustersClient):

    def get_secret_scopes_list(self):
        scopes_list = self.get('/secrets/scopes/list').get('scopes', [])
        return scopes_list

    def get_secrets(self, scope_name):
        secrets_list = self.get('/secrets/list', {'scope': scope_name}).get('secrets', [])
        return secrets_list

    def get_secret_value(self, scope_name, secret_key, cid, ec_id):
        cmd_set_value = f"value = dbutils.secrets.get(scope = '{scope_name}', key = '{secret_key}')"
        cmd_convert_b64 = "import base64; b64_value = base64.b64encode(value.encode('ascii'))"
        cmd_get_b64 = "print(b64_value.decode('ascii'))"
        results_set = self.submit_command(cid, ec_id, cmd_set_value)
        results_convert = self.submit_command(cid, ec_id, cmd_convert_b64)
        results_get = self.submit_command(cid, ec_id, cmd_get_b64)
        if results_set['resultType'] == 'error' \
                or results_convert['resultType'] == 'error'\
                or results_get['resultType'] == 'error':
            print("Error:")
            print(results_set)
            print(results_convert)
            print(results_get)
        s_value = results_get.get('data')
        return s_value

    def log_all_secrets(self, cluster_name, log_dir='secret_scopes/'):
        scopes_dir = self.get_export_dir() + log_dir
        scopes_list = self.get_secret_scopes_list()
        os.makedirs(scopes_dir, exist_ok=True)
        start = timer()
        cid = self.start_cluster_by_name(cluster_name)
        time.sleep(5)
        ec_id = self.get_execution_context(cid)
        for scope_json in scopes_list:
            scope_name = scope_json.get('name')
            secrets_list = self.get_secrets(scope_name)
            scopes_logfile = scopes_dir + scope_name
            with open(scopes_logfile, 'w') as fp:
                for secret_json in secrets_list:
                    secret_name = secret_json.get('key')
                    b64_value = self.get_secret_value(scope_name, secret_name, cid, ec_id)
                    s_json = {'name': secret_name, 'value': b64_value}
                    fp.write(json.dumps(s_json) + '\n')

    def log_all_secrets_acls(self, log_name='secret_scopes_acls.log'):
        acls_file = self.get_export_dir() + log_name
        scopes_list = self.get_secret_scopes_list()
        with open(acls_file, 'w') as fp:
            for scope_json in scopes_list:
                scope_name = scope_json.get('name', None)
                resp = self.get('/secrets/acls/list', {'scope': scope_name})
                resp['scope_name'] = scope_name
                fp.write(json.dumps(resp) + '\n')