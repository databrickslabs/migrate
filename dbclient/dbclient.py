import json
import os
import requests
import fileinput
import re
from dbclient import parser
import time
import requests.packages.urllib3

global pprint_j

requests.packages.urllib3.disable_warnings()

# Helper to pretty print json
def pprint_j(i):
    print(json.dumps(i, indent=4, sort_keys=True))


def url_validation(url):
    if '/?o=' in url:
        # if the workspace_id exists, lets remove it from the URL
        url = re.sub("\/\?o=.*", '', url)
    elif 'net/' == url[-4:]:
        url = url[:-1]
    elif 'com/' == url[-4:]:
        url = url[:-1]
    return url.rstrip("/")


class dbclient:
    """
    Rest API Wrapper for Databricks APIs
    """
    # set of http error codes to throw an exception if hit. Handles client and auth errors
    http_error_codes = [401]

    def __init__(self, configs):
        self._profile = configs['profile']
        self._url = ''
        self._token = ''
        self._raw_token = ''
        self._update_url_and_token(configs['url'], configs['token'])
        self._export_dir = configs['export_dir']
        self._is_aws = configs['is_aws']
        self._skip_failed = configs['skip_failed']
        self._is_verbose = configs['verbose']
        self._verify_ssl = configs['verify_ssl']
        self._file_format = configs['file_format']
        self._is_overwrite_notebook = configs['overwrite_notebooks']
        if self._verify_ssl:
            # set these env variables if skip SSL verification is enabled
            os.environ['REQUESTS_CA_BUNDLE'] = ""
            os.environ['CURL_CA_BUNDLE'] = ""
        os.makedirs(self._export_dir, exist_ok=True)

    def _update_url_and_token(self, url, token):
        self._url = url_validation(url)
        self._raw_token = token
        self._token = {
            'Authorization': 'Bearer {0}'.format(token),
            'User-Agent': 'databrickslabs-migrate/0.1.0'
        }

    def is_aws(self):
        return self._is_aws

    def is_verbose(self):
        return self._is_verbose

    def is_skip_failed(self):
        return self._skip_failed

    def get_file_format(self):
        return self._file_format

    def is_source_file_format(self):
        if self._file_format == 'SOURCE':
            return True
        return False

    def is_overwrite_notebooks(self):
        return self._is_overwrite_notebook

    @staticmethod
    def listdir(f_path):
        ls = os.listdir(f_path)
        for x in ls:
            # remove hidden directories / files from function
            if x.startswith('.'):
                continue
            yield x

    @staticmethod
    def walk(f_path):
        for my_root, my_subdir, my_files in os.walk(f_path):
            # filter out files starting with a '.'
            filtered_files = list(filter(lambda x: not x.startswith('.'), my_files))
            yield my_root, my_subdir, filtered_files

    def test_connection(self):
        # verify the proper url settings to configure this client
        if self._url[-4:] != '.com' and self._url[-4:] != '.net':
            print("Hostname should end in '.com'")
            return -1
        results = requests.get(self._url + '/api/2.0/clusters/spark-versions', headers=self._token,
                               verify=self._verify_ssl)
        http_status_code = results.status_code
        if http_status_code != 200:
            print("Error. Either the credentials have expired or the credentials don't have proper permissions.")
            print("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
            print(results.text)
            return -1
        return 0

    @staticmethod
    def delete_dir_if_empty(local_dir):
        if len(os.listdir(local_dir)) == 0:
            os.rmdir(local_dir)

    def _loop_until_token_renewed(self):
        interval = 20
        timeout = 86400

        for x in range(0, int(timeout / interval)):
            print(f"#{x} Migration paused due to expired token. Trying to renew...")
            login_args = parser.get_login_credentials(profile=self._profile)
            url = login_args['host']
            token = login_args['token']
            if token == self._raw_token:
                print("No new token found. Please renew token by running:\n" +
                      f"$ databricks configure --token --profile {self._profile}\n" +
                      f"Will try again in {interval} seconds.")
                time.sleep(interval)
                continue

            print("New token found. Migration resumed.")
            self._update_url_and_token(url, token)
            return

        raise Exception(f"Failed to renew token after {timeout}s. Please rerun the pipeline.")

    def _should_retry_with_new_token(self, raw_results):
        if raw_results.status_code == 403 and "Error 403 Invalid access token." in raw_results.text:
            self._loop_until_token_renewed()
            return True
        else:
            return False

    def get(self, endpoint, json_params=None, version='2.0', print_json=False):
        if version:
            ver = version

        while True:
            full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
            if self.is_verbose():
                print("Get: {0}".format(full_endpoint))
            if json_params:
                raw_results = requests.get(full_endpoint, headers=self._token, params=json_params, verify=self._verify_ssl)
            else:
                raw_results = requests.get(full_endpoint, headers=self._token, verify=self._verify_ssl)

            if self._should_retry_with_new_token(raw_results):
                continue

            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
            results = raw_results.json()
            if print_json:
                print(json.dumps(results, indent=4, sort_keys=True))
            if type(results) == list:
                results = {'elements': results}
            results['http_status_code'] = http_status_code
            return results

    def http_req(self, http_type, endpoint, json_params, version='2.0', print_json=False, files_json=None):
        if version:
            ver = version
        while True:
            full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
            if self.is_verbose():
                print("{0}: {1}".format(http_type, full_endpoint))
            if json_params:
                if http_type == 'post':
                    if files_json:
                        raw_results = requests.post(full_endpoint, headers=self._token,
                                                    data=json_params, files=files_json, verify=self._verify_ssl)
                    else:
                        raw_results = requests.post(full_endpoint, headers=self._token,
                                                    json=json_params, verify=self._verify_ssl)
                if http_type == 'put':
                    raw_results = requests.put(full_endpoint, headers=self._token,
                                               json=json_params, verify=self._verify_ssl)
                if http_type == 'patch':
                    raw_results = requests.patch(full_endpoint, headers=self._token,
                                                 json=json_params, verify=self._verify_ssl)
            else:
                print("Must have a payload in json_args param.")
                return {}

            if self._should_retry_with_new_token(raw_results):
                continue

            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: {0} request failed with code {1}\n{2}".format(http_type,
                                                                                      http_status_code,
                                                                                      raw_results.text))
            results = raw_results.json()
            if print_json:
                print(json.dumps(results, indent=4, sort_keys=True))
            # if results are empty, let's return the return status
            if results:
                results['http_status_code'] = raw_results.status_code
                return results
            else:
                return {'http_status_code': raw_results.status_code}

    def post(self, endpoint, json_params, version='2.0', print_json=False, files_json=None):
        return self.http_req('post', endpoint, json_params, version, print_json, files_json)

    def put(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('put', endpoint, json_params, version, print_json)

    def patch(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('patch', endpoint, json_params, version, print_json)

    @staticmethod
    def get_key(http_resp, key_name):
        value = http_resp.get(key_name, None)
        if value is None:
            print(http_resp)
            raise ValueError('Unable to find key')
        return value

    @staticmethod
    def my_map(F, items):
        to_return = []
        for elem in items:
            to_return.append(F(elem))
        return to_return

    def whoami(self):
        """
        get current user userName from SCIM API
        :return: username string
        """
        user_name = self.get('/preview/scim/v2/Me').get('userName')
        return user_name

    def build_acl_args(self, full_acl_list, is_jobs=False):
        """
        Take the ACL json and return a json that corresponds to the proper input with permission level one level higher
        { 'acl': [ { (user_name, group_name): {'permission_level': '*'}, ... ] }
        for job ACLs, we need to reset the OWNER, so set the admin as CAN_MANAGE instead
        :param full_acl_list:
        :return:
        """
        acls_list = []
        current_owner = ''
        for member in full_acl_list:
            permissions = member.get('all_permissions')[0].get('permission_level')
            if 'user_name' in member:
                acls_list.append({'user_name': member.get('user_name'),
                                  'permission_level': permissions})
                if permissions == 'IS_OWNER':
                    current_owner = member.get('user_name')
            else:
                if member.get('group_name') != 'admins':
                    acls_list.append({'group_name': member.get('group_name'),
                                      'permission_level': permissions})
                    if permissions == 'IS_OWNER':
                        current_owner = member.get('group_name')

        if is_jobs:
            me = self.whoami()
            if current_owner != me:
                update_admin = {'user_name': self.whoami(),
                                'permission_level': 'CAN_MANAGE'}
                acls_list.append(update_admin)
        return acls_list

    def set_export_dir(self, dir_location):
        self._export_dir = dir_location

    def get_export_dir(self):
        return self._export_dir

    def get_url(self):
        return self._url

    def get_latest_spark_version(self):
        versions = self.get('/clusters/spark-versions')['versions']
        v_sorted = sorted(versions, key=lambda i: i['key'], reverse=True)
        for x in v_sorted:
            img_type = x['key'].split('-')[1][0:5]
            if img_type == 'scala':
                return x

    def replace_file_contents(self, old_str, new_str, filename):
        """
        regex replace all occurrences of a string with a new value
        :param old_str: old value to replace, e.g. account id, old email, etc.
        :param new_str: new value
        :param filename: logfile path relative to the export dir
        :return:
        """
        log_dir = self.get_export_dir()
        update_filename = log_dir + filename
        with fileinput.FileInput(update_filename, inplace=True, backup='.bak') as fp:
            for line in fp:
                print(line.replace(old_str, new_str), end='')
        # cleanup old backup file once completed
        f_backup = log_dir + filename + '.bak'
        os.remove(f_backup)

    def update_account_id(self, new_aws_account_id, old_account_id):
        log_dir = self.get_export_dir()
        logs_to_update = ['users.log',
                          'instance_profiles.log', 'clusters.log', 'cluster_policies.log',
                          'jobs.log']
        # update individual logs first
        for log_name in logs_to_update:
            if os.path.exists(log_dir + log_name):
                self.replace_file_contents(old_account_id, new_aws_account_id, log_name)
        # # update group logs
        group_dir = log_dir + 'groups/'
        groups = self.listdir(group_dir)
        for group_name in groups:
            group_file = 'groups/' + group_name
            if os.path.exists(log_dir + group_file):
                self.replace_file_contents(old_account_id, new_aws_account_id, group_file)

    def update_email_addresses(self, old_email_address, new_email_address):
        """
        :param old_email_address:
        :param new_email_address:
        :return:
        """
        log_dir = self.get_export_dir()
        logs_to_update = ['users.log',
                          'acl_jobs.log',
                          'acl_clusters.log', 'acl_cluster_policies.log',
                          'acl_notebooks.log', 'acl_directories.log']
        for logfile in logs_to_update:
            if os.path.exists(log_dir + logfile):
                self.replace_file_contents(old_email_address, new_email_address, logfile)
        # update the path for user notebooks in bulk export mode
        bulk_export_dir = log_dir + 'artifacts/Users/'
        old_bulk_export_dir = bulk_export_dir + old_email_address
        new_bulk_export_dir = bulk_export_dir + new_email_address
        if os.path.exists(old_bulk_export_dir):
            os.rename(old_bulk_export_dir, new_bulk_export_dir)
        # update the path for user notebooks in single user export mode
        single_user_dir = log_dir + 'user_exports/'
        old_single_user_dir = single_user_dir + old_email_address
        new_single_user_dir = single_user_dir + new_email_address
        if os.path.exists(old_single_user_dir):
            os.rename(old_single_user_dir, new_single_user_dir)
        old_single_user_nbs_dir = new_single_user_dir + '/user_artifacts/Users/' + old_email_address
        new_single_user_nbs_dir = new_single_user_dir + '/user_artifacts/Users/' + new_email_address
        if os.path.exists(old_single_user_nbs_dir):
            os.rename(old_single_user_nbs_dir, new_single_user_nbs_dir)
        print("Update email address complete")
