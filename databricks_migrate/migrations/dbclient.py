import functools
import json
import os

from databricks_cli.sdk import ClusterService, ApiClient
from requests import HTTPError

from databricks_migrate import log

global pprint_j


# Helper to pretty print json
def pprint_j(i):
    log.info(json.dumps(i, indent=4, sort_keys=True))


class BaseMigrationClient:
    def __init__(self, api_client: ApiClient, api_client_v1_2: ApiClient, export_dir, is_aws, skip_failed, verify_ssl):
        self.api_client_v1_2 = api_client_v1_2
        self.api_client = api_client
        self._export_dir = export_dir
        self._is_aws = is_aws
        self._skip_failed = skip_failed
        self._verify_ssl = verify_ssl
        if self._verify_ssl:
            # set these env variables if skip SSL verification is enabled
            os.environ['REQUESTS_CA_BUNDLE'] = ""
            os.environ['CURL_CA_BUNDLE'] = ""
        os.makedirs(self._export_dir, exist_ok=True)

    def __repr__(self):
        return json.dumps({
            "class": self.__class__.__name__,
            "host": self.api_client.url,
            "export_dir": self._export_dir,
            "is_aws": self._is_aws,
            "skip_failed": self._skip_failed,
            "verify_ssl": self._verify_ssl,
        })

    @property
    def scim_read_headers(self):
        return {"Accept": "application/scim+json", "Content-Type": "application/scim+json"}

    def is_aws(self):
        return self._is_aws

    def is_skip_failed(self):
        return self._skip_failed

    @staticmethod
    def my_map(F, items):
        to_return = []
        for elem in items:
            to_return.append(F(elem))
        return to_return

    def test_connection(self):
        # verify the proper url settings to configure this client
        # if self.api_client.url[-4:] != '.com':
        #     log.info(f"Hostname: {self.api_client.url} should end in '.com'")
        #     return -1
        cluster_service = ClusterService(self.api_client)
        try:
            cluster_service.list_spark_versions()
        except HTTPError as e:
            log.error("Error. Either the credentials have expired or the credentials don't have proper permissions.")
            log.error("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
            log.error(e)

    def get_latest_spark_version(self):
        cluster_service = ClusterService(self.api_client)
        versions = cluster_service.list_spark_versions()['versions']
        v_sorted = sorted(versions, key=lambda i: i['key'], reverse=True)
        for x in v_sorted:
            img_type = x['key'].split('-')[1][0:5]
            if img_type == 'scala':
                return x

    def skip_failed_handler(self, function):
        """
            Injects the api_client keyword argument to the wrapped function.
            All callbacks wrapped by provide_api_client expect the argument ``profile`` to be passed in.
            """

        @functools.wraps(function)
        def decorator(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except HTTPError as e:
                log.error(str(e))
                if self.is_skip_failed():
                    return {}
                else:
                    raise e

        decorator.__doc__ = function.__doc__
        return decorator
