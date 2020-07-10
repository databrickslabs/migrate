
import logging

import click_log

log = logging.getLogger(__name__)
click_log.basic_config(log)
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])