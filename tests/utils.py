import logging
from typing import List, Callable, Union

from pytest_httpserver import HTTPServer, WaitingSettings


class HTTPFixture:
    POST = "POST"
    GET = "GET"
    PATCH = "PATCH"
    PUT = "PUT"

    def __init__(self,
                 uri: str,
                 method: str,
                 request_data: Union[str, bytes, None] = None,
                 query_string: Union[None, str, bytes] = None,
                 status: int = 200,
                 response=None):
        """

        :param uri: it is the uri without the host information
        :param method: method is a string value for the HTTP method
        :param request_data: the data sent along with a POST or PUT request
        :param query_string: the query string provided as a string along with the uri
        :param status: the status of the response as an integer
        :param response: the response is a python object that can be serialized to json
        """
        self.status = status
        self.response = response
        self.query_string = query_string
        self.request_data = request_data
        self.method = method
        self.uri = uri
        assert self.method is not None, "method in fixture is None"
        assert self.uri is not None, "method in fixture is None"


def assert_api_calls(http_fixtures: List[HTTPFixture], method_to_test: Callable, **method_to_test_kwargs):
    wait_settings = WaitingSettings(raise_assertions=True, stop_on_nohandler=True)
    with HTTPServer(default_waiting_settings=wait_settings) as httpserver:
        for fixture in http_fixtures:
            httpserver.expect_ordered_request(
                uri=fixture.uri,
                method=fixture.method,
                data=fixture.request_data,
                query_string=fixture.query_string
            ).respond_with_json(
                response_json=fixture.response,
                status=fixture.status
            )
        if "client_config" in method_to_test_kwargs and type(method_to_test_kwargs["client_config"]) == dict:
            method_to_test_kwargs["client_config"]["url"] = httpserver.url_for("/")
        else:
            logging.getLogger().info("%s", "arg")("unable to find client_config")
        method_to_test(**method_to_test_kwargs)
        httpserver.check_assertions()
        assert len(httpserver.ordered_handlers) == 0, \
            f"The following expected methods were not executed: {[handler.matcher for handler in httpserver.ordered_handlers]}"


def assert_content(file_path: str, expected_body: str):
    with open(file_path) as f:
        content = f.read()
    assert expected_body == content, f"content in {file_path} does not match expected content"