from databricks_migrate import log
from databricks_migrate.dbclient import get_login_credentials, is_azure_creds


def get_login_args(profile, azure):
    login_args = None
    if profile is not None:
        log.debug(f"Using profile: {profile}")
        login_args = get_login_credentials(profile=profile)
        if is_azure_creds(login_args) and not azure:
            raise ValueError('Login credentials do not match args. Please provide --azure flag for azure environments.')

    # # cant use netrc credentials because requests module tries to load the credentials into http basic auth headers
    if login_args is None:
        raise ValueError('Login credentials not found please provide a profile')

    return login_args
