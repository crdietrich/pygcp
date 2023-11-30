"""Secret Manager for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.

Note: 
module name is 'secret.py' rather than 'secrets.py' to avoid 
namespace collision with numpy module requirement.

References:
https://cloud.google.com/python/docs/reference/secretmanager/latest
https://github.com/googleapis/python-secret-manager
https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets
"""


import google_crc32c
import pandas as pd
from google.cloud import secretmanager

response_columns = ["project", "name", "creation", "labels"]


def response_parser(response):
    name = response.name
    name = name.split("/")[-1]
    create_time = response.create_time.isoformat()
    labels = dict(response.labels.items())
    return [name, create_time, labels]


def list_all(gcp_credentials, project_id):
    """List all secrets in a Project

    Parameters
    ----------
    gcp_credentials : google.cloud.XX credentials
    project_id : str, GCP project name

    Returns
    -------
    df_response : Pandas DataFrame with columns
        project : str, name of GCP project to list secrets
        name : str, secret name
        creation : str, ISO timestamp when secret was created
        labels : dict, labels
    """

    client = secretmanager.SecretManagerServiceClient(credentials=gcp_credentials)
    request = secretmanager.ListSecretsRequest(parent=f"projects/{project_id}")
    page_result = client.list_secrets(request=request)

    response_list = []
    for response in page_result:
        response = response_parser(response)
        response_list.append([project_id] + response)

    df_response = pd.DataFrame(response_list, columns=response_columns)
    return df_response


def create(gcp_credentials, project_id, secret_id, labels=None):
    """Add a Secret

    Parameters
    ----------
    gcp_credentials : google.cloud.XX credentials
    project_id : str, GCP project name
    secret_id : str, secret name
    labels : dict, key value labels to apply to the secret
    """
    client = secretmanager.SecretManagerServiceClient(credentials=gcp_credentials)

    secret_attributes = {"replication": {"automatic": {}}}
    if labels is not None:
        secret_attributes["labels"] = labels

    request = {
        "parent": f"projects/{project_id}",
        "secret_id": f"{secret_id}",
        "secret": secret_attributes,
    }

    response = client.create_secret(request=request)

    return response


def add_version(gcp_credentials, project_id, secret_id, payload):
    """

    Parameters
    ----------
    gcp_credentials : google.cloud.XX credentials
    project_id : str, GCP project name
    secret_id : str, secret name
    payload : str, value set secret

    Returns
    -------
    str, secret version
    """

    client = secretmanager.SecretManagerServiceClient(credentials=gcp_credentials)

    parent = client.secret_path(project_id, secret_id)
    payload = payload.encode("UTF-8")
    crc32c = google_crc32c.Checksum()
    crc32c.update(payload)

    response = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload, "data_crc32c": int(crc32c.hexdigest(), 16)},
        }
    )

    return response.name


def access(gcp_credentials, project_id, secret_id, version_id="latest"):
    """Use Secret Manager to retrieve information

    Parameters
    ----------
    gcp_credentials :
    project_id :
    secret_id :
    version_id :

    Returns
    -------
    str, UTF-8 decoded payload stored in Secret Manager
    """

    version_id = str(version_id)
    client = secretmanager.SecretManagerServiceClient(credentials=gcp_credentials)
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)

    return response.payload.data.decode("UTF-8")


def delete(gcp_credentials, project_id, secret_id):
    """Delete a Secret

    Parameters
    ----------
    gcp_credentials : google.cloud.XX credentials
    project_id : str, GCP project name
    secret_id : str, secret name
    """

    client = secretmanager.SecretManagerServiceClient(credentials=gcp_credentials)
    name = f"projects/{project_id}/secrets/{secret_id}"
    request = secretmanager.DeleteSecretRequest(name=name)
    client.delete_secret(request=request)
