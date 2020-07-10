import argparse
from os import path

auth_key = ['host',
            'username',
            'token']


def is_azure_creds(creds):
    if 'azuredatabricks.net' in creds['host']:
        return True
    return False


def get_login_credentials(creds_path='~/.databrickscfg', profile='DEFAULT'):
    if profile is None:
        profile = 'DEFAULT'
    if creds_path == '~/.databrickscfg':
        fname = path.expanduser(creds_path)
    elif creds_path[0] == '~':
        # relative path, still need to resolve using the above os api
        fname = path.expanduser(creds_path)
    else:
        fname = creds_path
    hit_profile = False
    cred_dict = {}
    with open(fname, 'r') as fp:
        for line in fp:
            # if line is not empty
            if line:
                # strip chars and clean up the string
                clean_l = line.rstrip().lstrip()
                if clean_l:
                    # non-empty lines processing below
                    if hit_profile:
                        if clean_l[0] == '[':
                            return cred_dict
                        else:
                            # can't split on = due to community edition code
                            split_index = clean_l.find('=')
                            x = clean_l[:split_index].rstrip()
                            cred_dict[x] = clean_l[split_index + 1:].lstrip()
                    if clean_l[0] == '[':
                        if clean_l[1:-1] == profile:
                            # next couple of lines are the credentials
                            hit_profile = True
                            continue
    if cred_dict:
        return cred_dict
    else:
        raise ValueError('Unable to find credentials to load.')

def build_client_config(url, token, is_azure, no_ssl_verification, skip_failed):
    # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    # aws is the default
    config = {'url': url,
              'token': token,
              'is_aws': (not is_azure),
              'verify_ssl': (not no_ssl_verification),
              'skip_failed': skip_failed,
              }
    if config['is_aws']:
        config['export_dir'] = 'logs/'
    else:
        config['export_dir'] = 'azure_logs/'
    return config
