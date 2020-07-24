import logging
import click_log

log = logging.getLogger(__name__)
click_log.basic_config(log)
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
API_VERSION_2_0 = "2.0"
API_VERSION_1_2 = "1.2"

api_client_var_dict = {
    API_VERSION_2_0: "api_client",
    API_VERSION_1_2: "api_client_v1_2"
}
