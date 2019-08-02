import argparse
from os import path

auth_key = ['host',
            'username',
            'token']

def get_login_credentials(creds_path='~/.databrickscfg', profile='DEFAULT'):
  if creds_path == '~/.databrickscfg':
    fname = path.expanduser(creds_path)
  elif creds_path[0] == '~':
    # relative path, still need to resolve using the above os api
    fname = path.expanduser(creds_path)
  else:
    fname = cred_path
  hit_profile = False
  cred_dict = {}
  with open(fname, 'r') as fp:
    for l in fp:
      if l:
        clean_l = l[:-1].rstrip().lstrip()
        if clean_l:
          if hit_profile:
            if clean_l[0] == '[':
              return cred_dict
            else: 
              # can't split on = due to community edition code
              split_index = clean_l.find('=')
              x = clean_l[:split_index].rstrip()
              cred_dict[x] = clean_l[split_index + 1:].lstrip()
              
          if (clean_l[0] == '['):
            if (clean_l[1:-1] == profile):
              # next couple of lines are the credentials
              hit_profile = True
              continue
    return cred_dict

def get_migration_parser():
  # export workspace items
  parser = argparse.ArgumentParser(description='Migrate user workspaces in Databricks')

  # export all users and groups
  parser.add_argument('--users', action='store_true',
                      help='Collect all the users and groups in the workspace')

  # add all user workspace
  parser.add_argument('--workspace', action='store_true',
                      help='Collect all the users workspaces for the environment')

  # add all user workspace
  parser.add_argument('--download', action='store_true',
                      help='Download all notebooks for the environment')

  # add all lib configs
  parser.add_argument('--libs', action='store_true',
                      help='Collect all the libs for the environment')

  # add all clusters configs
  parser.add_argument('--clusters', action='store_true',
                      help='Collect all the clusters for the environment')

  # get all job configs
  parser.add_argument('--jobs', action='store_true',
                      help='Collect all the job configs for the environment')
  # get all metastore
  parser.add_argument('--metastore', action='store_true',
                      help='Collect all the metastore details')

  # get azure logs 
  parser.add_argument('--azure', action='store_true',
                      help='Run on Azure. (Default is AWS)')
  # 
  parser.add_argument('--profile', action='store',
                      help='Profile to parse the credentials')
  return parser
