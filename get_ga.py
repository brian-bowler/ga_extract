from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials

import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

def get_service(api_name, api_version, scope, key_file_location, service_account_email):
    """api_name:            The name of the api to connect to.
    api_version:            The api version to connect to.
    scope:                  A list auth scopes to authorize for the application.
    key_file_location:      The path to a valid service account p12 key file.
    service_account_email:  The service account email address.

    Returns:
    A service that is connected to the specified API."""

    f = open(key_file_location, 'rb')
    key = f.read()
    f.close()
    credentials = SignedJwtAssertionCredentials(service_account_email, key, scope=scope)
    http = credentials.authorize(httplib2.Http())
    service = build(api_name, api_version, http=http)

    return service