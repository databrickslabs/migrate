from dbclient import *
from timeit import default_timer as timer
from datetime import timedelta
from os import makedirs, path
from datetime import datetime
# python 3.6

def main():

  # define a parser to identify what component to import / export 
  parser = get_migration_parser() 
  # parse the args
  args = parser.parse_args()
  p = args.profile 

  # parse the path location of the Databricks CLI configuration
  login_creds = get_login_credentials(profile=p)
  print(not args.azure)

  print(login_creds)
if __name__ == '__main__':
    main()
