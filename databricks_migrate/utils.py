import functools
import uuid
from datetime import timedelta, datetime
from timeit import default_timer as timer
from typing import Text

import click
from databricks_cli.configure.config import get_profile_from_context
from databricks_cli.configure.provider import ProfileConfigProvider, get_config
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import InvalidConfigurationError

from databricks_migrate import api_client_var_dict, log


def _get_api_client(config, command_name="", api_version="2.0"):
    verify = config.insecure is None
    if config.is_valid_with_token:
        return ApiClient(host=config.host, token=config.token, verify=verify, apiVersion=api_version,
                         command_name=command_name)
    return ApiClient(user=config.username, password=config.password, apiVersion=api_version,
                     host=config.host, verify=verify, command_name=command_name)


def provide_api_client(api_version):
    def api_client(function):
        """
        Injects the api_client keyword argument to the wrapped function. The api_version is to create different clients
        All callbacks wrapped by provide_api_client expect the argument ``profile`` to be passed in.
        """

        @functools.wraps(function)
        def decorator(*args, **kwargs):
            ctx = click.get_current_context()
            command_name = "-".join(ctx.command_path.split(" ")[1:])
            command_name += "-" + str(uuid.uuid1())
            profile = get_profile_from_context()
            if profile:
                # If we request a specific profile, only get credentials from tere.
                config = ProfileConfigProvider(profile).get_config()
            else:
                # If unspecified, use the default provider, or allow for user overrides.
                config = get_config()
            if not config or not config.is_valid:
                raise InvalidConfigurationError.for_profile(profile)
            _api_client = _get_api_client(config, api_version=api_version, command_name=command_name)
            variable_name = api_client_var_dict[api_version]
            kwargs[variable_name] = _api_client

            return function(*args, **kwargs)

        decorator.__doc__ = function.__doc__
        return decorator

    return api_client


def log_action(action_name: Text, debug_params: bool = False):
    def action(function):
        """
        Log an action given the action name and also debug parameters.
        """

        @functools.wraps(function)
        def decorator(*args, **kwargs):
            now = str(datetime.now())
            if debug_params:
                log.debug(
                    f"Executing action: '{action_name}'; function: '{function.__name__}'; args: '{args}'; kwargs: '{kwargs}'; time: {now}")
            log.info(f"Executing action: '{action_name}' at time: {now}")

            start = timer()
            res = function(*args, **kwargs)
            end = timer()
            log.info(f"Duration to execute function '{action_name}': " + str(timedelta(seconds=end - start)))
            return res

        decorator.__doc__ = function.__doc__
        return decorator

    return action
