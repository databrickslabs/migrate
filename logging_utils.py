import logging
import json

def set_default_logging(parent_dir, level=logging.INFO):
    log_file = f"{_get_log_dir(parent_dir)}/wm_logs.log"
    all_log_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler()

    logging.basicConfig(format="%(asctime)s;%(levelname)s;%(message)s",
                        datefmt='%Y-%m-%d,%H:%M:%S',
                        level=level,
                        handlers=[all_log_handler, console_handler])


def get_error_logger(action_type, object_type, log_dir):
    """
    Failures are written to object specific log file.
    """
    logger = logging.getLogger("workspace_migration")

    failed_log_file = get_error_log_file(action_type, object_type, log_dir)
    error_handler = logging.FileHandler(failed_log_file, 'w+')
    error_handler.setLevel(logging.ERROR)

    logger.addHandler(error_handler)
    return logger

def get_error_log_file(action_type, object_type, parent_dir):
    return f"{_get_log_dir(parent_dir)}/failed_{action_type}_{object_type}.log"

def _get_log_dir(parent_dir):
    return parent_dir + "/app_logs"

def log_reponse_error(error_logger, response, error_msg=None):
    if 'error_code' in response or 'error' in response:
        if error_msg:
            error_logger.error(error_msg + '\n')
        else:
            error_logger.error(json.dumps(response) + '\n')
        return True
    else:
        return False
