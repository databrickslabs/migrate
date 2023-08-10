from dbclient import *
import sys, requests

# python 3.6

def main():
    # define a parser to identify what component to import / export
    parser = get_export_parser()
    # parse the args
    args = parser.parse_args()
    profile = args.profile

    # parse the path location of the Databricks CLI configuration
    login_args = get_login_credentials(profile=profile)

    # parse the credentials
    url = login_args['host']
    token = login_args.get('token', login_args.get('password'))
    client_config = build_client_config(profile, url, token, args)

    print("Test connection at {0} with profile {1}\n".format(url, args.profile))
    db_client = dbclient(client_config)
    try:
        is_successful = db_client.test_connection()
    except requests.exceptions.RequestException as e:
        print(e)
        print("\nUnsuccessful connection. Verify credentials.\n")
        sys.exit(1)
    if is_successful == 0:
        print("Connection successful!")
    else:
        print("\nUnsuccessful connection. Verify credentials.\n")

if __name__ == '__main__':
    main()
