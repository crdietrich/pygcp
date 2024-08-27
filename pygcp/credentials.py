"""Credentials for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""


import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account


def create_credentials(cred_file=None, project_id=None, scopes=None, verbose=False):
    """Create Google credentials for use either locally with OAuth2 or
    locally within a GCP VM.

    Notes
    -----
    See https://github.com/googleapis/google-auth-library-python
    for api library

    See https://developers.google.com/identity/protocols/oauth2/scopes
    for a complete list of scopes that can be added to the defaults set here
    * cloud-platform
    * drive
    * spreadsheets

    Parameters
    ----------
    cred_file : str, (optional if in a VM) The OAuth2 Credentials to use for this
                client. If not passed, falls back to the default inferred from the
                environment.
    project_id : str, (optional if in a VM)
    scopes : list of str, (optional)
    verbose : bool, default is False

    Returns
    -------
    If within a GCP VM, an instance of:
        google.auth.compute_engine.credentials.Credentials
    OR
    If from a JSON service account file, an instance of:
        google.oauth2.service_account.Credentials
    """

    scopes_list = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    if scopes is not None:
        for s in scopes:
            scopes_list.append(str(s))

    # VM or session with environmental variables
    try:
        gcp_credentials, project_id = google.auth.default(scopes=scopes_list)
        return gcp_credentials, project_id

    except DefaultCredentialsError:
        if verbose:
            print("DefaultCredentialsError, trying to create from JSON file...")

    # from a service account JSON file
    assert (cred_file is not None) & (project_id is not None)

    gcp_credentials = service_account.Credentials.from_service_account_file(cred_file)
    gcp_credentials = gcp_credentials.with_scopes(scopes_list)

    # set the internal attribute so the credentials object matches our needs
    # this is important for other services that might infer default project_id
    # from the credentials
    # TODO: consider if this pattern is unneeded when client project
    # default is never taken from the credential instance
    gcp_credentials._project_id = project_id

    if verbose:
        print("JSON credential file successfully created client.")

    return gcp_credentials, project_id
