import logging
import json
import os
import re
import wmconstants


def set_default_logging(parent_dir, level=logging.INFO):
    os.makedirs(_get_log_dir(parent_dir), exist_ok=True)
    log_file = f"{_get_log_dir(parent_dir)}/wm_logs.log"
    all_log_handler = logging.FileHandler(log_file, encoding='utf-8')
    console_handler = logging.StreamHandler()

    logging.basicConfig(format="%(asctime)s;%(levelname)s;%(message)s",
                        datefmt='%Y-%m-%d,%H:%M:%S',
                        level=level,
                        handlers=[all_log_handler, console_handler])


def get_error_logger(action_type, object_type, log_dir):
    """
    Failures are written to object specific log file.
    """
    logger = logging.getLogger(f"workspace_migration_{object_type}")

    failed_log_file = get_error_log_file(action_type, object_type, log_dir)
    os.makedirs(_get_log_dir(log_dir), exist_ok=True)

    error_handler = logging.FileHandler(failed_log_file, 'w+')
    error_handler.setLevel(logging.ERROR)

    logger.addHandler(error_handler)
    return logger


def get_error_log_file(action_type, object_type, parent_dir):
    return f"{_get_log_dir(parent_dir)}/failed_{action_type}_{object_type}.log"


def _get_log_dir(parent_dir):
    return parent_dir + "/app_logs"


def log_response_error(error_logger,
                       response,
                       error_msg=None,
                       ignore_error_list=None):
    """
    Logs errors based on the response. Usually used when the response is the http response.
    """
    if ignore_error_list is None:
        ignore_error_list = wmconstants.IGNORE_ERROR_LIST

    if check_error(response, ignore_error_list):
        if error_msg:
            error_logger.error(error_msg)
        else:
            error_logger.error(json.dumps(response))
        return True
    else:
        return False


def check_error(response, ignore_error_list=None):
    if ignore_error_list is None:
        ignore_error_list = wmconstants.IGNORE_ERROR_LIST

    if type(response) is list:
        for resp in response:
            if _check_error_helper(resp, ignore_error_list):
                return True
        return False
    else:
        return _check_error_helper(response, ignore_error_list)


def _check_error_helper(response, ignore_error_list):
    # suppress cluster warning for already-running clusters
    if re.match("Cluster .*? is in unexpected state (Running|Pending)\\.", response.get("message", "")):
        return False

    return ('error_code' in response and response['error_code'] not in ignore_error_list) \
            or ('error' in response and response['error'] not in ignore_error_list) \
            or (response.get('resultType', None) == 'error' and 'already exists' not in response.get('summary', None))


def raise_if_failed_task_file_exists(failed_task_log, task_name):
    if os.path.exists(failed_task_log) and os.path.getsize(failed_task_log) > 0:
        msg = f'{task_name} has failures. Refer to {failed_task_log} to see failures. Terminating pipeline.'
        logging.info(msg)
        raise RuntimeError(msg)
