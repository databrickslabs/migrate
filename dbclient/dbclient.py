import json, requests, datetime
from os import makedirs

global pprint_j


# Helper to pretty print json
def pprint_j(i):
    print(json.dumps(i, indent=4, sort_keys=True))


class dbclient:
    """A class to define wrappers for the REST API"""

    def __init__(self, token='foobarfoobar', url="https://myenv.cloud.databricks.com", export_dir='logs/'):
        self._token = {'Authorization': 'Bearer {0}'.format(token)}
        self._url = url.rstrip("/")
        self._export_dir = export_dir
        makedirs(self._export_dir, exist_ok=True)

    def test_connection(self):
        # verify the proper url settings to configure this client
        if self._url[-4:] != '.com':
            print("Hostname should end in '.com'")
            return -1
        results = requests.get(self._url + '/api/2.0/clusters/spark-versions', headers=self._token)
        http_status_code = results.status_code
        if http_status_code != 200:
            print("Error. Either the credentails have expired or the credentials don't have proper permissions.")
            print("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
            print(results.text)
            return -1
        return 0

    def get(self, endpoint, json_params=None, version='2.0', print_json=False):
        if version:
            ver = version
        full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
        if json_params:
            raw_results = requests.get(full_endpoint, headers=self._token, params=json_params)
            results = raw_results.json()
        else:
            raw_results = requests.get(full_endpoint, headers=self._token)
            results = raw_results.json()
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        results['http_status_code'] = raw_results.status_code
        return results

    def http_req(self, http_type, endpoint, json_params, version='2.0', print_json=False):
        if version:
            ver = version
        full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
        if json_params:
            if http_type == 'post':
                raw_results = requests.post(full_endpoint, headers=self._token, json=json_params)
            if http_type == 'put':
                raw_results = requests.put(full_endpoint, headers=self._token, json=json_params)
            if http_type == 'patch':
                raw_results = requests.patch(full_endpoint, headers=self._token, json=json_params)
            results = raw_results.json()
        else:
            print("Must have a payload in json_args param.")
            return {}
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        # if results are empty, let's return the return status
        if results:
            results['http_status_code'] = raw_results.status_code
            return results
        else:
            return {'http_status_code': raw_results.status_code}

    def post(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('post', endpoint, json_params, version, print_json)

    def put(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('put', endpoint, json_params, version, print_json)

    def patch(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('patch', endpoint, json_params, version, print_json)

    @staticmethod
    def my_map(F, items):
        to_return = []
        for elem in items:
            to_return.append(F(elem))
        return to_return
