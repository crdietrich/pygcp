"""Cloud Logging for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""

import json
from collections.abc import Iterable

import google.cloud.logging
import pandas as pd


def log_flattener(r):
    """Flatten a single Logging record class instance

    Parameters
    ---------
    r : Logging result class
        Cloud Logging API query result

    Returns
    -------
    list of str, with items
        log_name
            the name of the log
        timestamp
            the time of the event
        severity
            the severity of the event
        labels
            label key:value pairs
        payload
            user data or message included in the logged event
    """
    # TODO: document the format of the timestamp returned
    # TODO: get the exact type of input r
    # TODO: copy docstring to label_flattener
    log_name = r.log_name
    timestamp = r.timestamp
    severity = r.severity
    labels = json.dumps(r.labels)
    payload = r.payload
    return log_name, timestamp, severity, labels, payload


def label_flattener(r):
    labels = r.labels
    labels = json.loads(labels)

    if pd.isnull(labels):
        return None, None
    else:
        labels = list(labels.items())[0]
        _source, _logger = labels
        return _source, _logger


def query(
    gcp_credentials,
    project_id=None,
    log_name=None,
    severity=None,
    labels=None,
    order_by="ascending",
    query_language=None,
    output="pandas",
):
    """Retrieve Cloud Logging data from a specific GCP Project

    Parameters
    ----------
    gcp_credentials :
        google.auth.compute_engine.credentials.Credentials
        OR
        google.oauth2.service_account.Credentials
    project_id : str
        GCP project to run the query in
    log_name : str
        the name of the log. Must be one of these formats:
            projects/[PROJECT_ID]/logs/[LOG_ID]
            organizations/[ORGANIZATION_ID]/logs/[LOG_ID]
            billingAccounts/[BILLING_ACCOUNT_ID]/logs/[LOG_ID]
            folders/[FOLDER_ID]/logs/[LOG_ID]
    severity : str
        the severity of the event. Must be one of (in descending
        order of severity): Emergency, Alert, Critical, Error, Warning,
        Notice, Info, Debug, Default
    labels : dict
        label keys and values to filter results
    order_by : str, {'ascending', 'descending'}
    query_language : str
        A Logging Query Language query. Overrides all other
        filters set with kwargs log_name, severity and labels.

    Returns
    -------
    Pandas DataFrame, logging data with columns
        log_name : str
            name of log source
        timestamp : str
            timestamp of logging event
        severity : str
            severity level of the logging event
        labels : dict
            unique key:value labels applied to events
        payload : str
            user data to include in the logging event
    """

    gcp_logging_client = google.cloud.logging.Client(
        credentials=gcp_credentials, project=project_id
    )

    order_mapper = {
        "ascending": google.cloud.logging.ASCENDING,
        "descending": google.cloud.logging.DESCENDING,
    }
    order_by = order_by.lower()
    assert (
        order_by in order_mapper.keys()
    ), "order not specified correctly, either 'ascending' or 'descending'"
    order_by = order_mapper[order_by]

    filter_str = ""
    if severity is not None:
        if isinstance(severity, str):
            filter_str += f"severity={severity}\n"
        elif isinstance(severity, Iterable):
            severity = " OR ".join(severity)
            filter_str += f"severity=({severity})\n"
    if project_id is not None:
        filter_str += f'resource.labels.project_id="{project_id}"'
    if log_name is not None:
        filter_str += f"\nlogName=projects/{project_id}/logs/{log_name}"
    if (labels is not None) and (isinstance(labels, dict)):
        for k, v in labels.items():
            filter_str += f'''\nlabels.{k}="{v}"'''
    if query_language is not None:
        filter_str = query_language
    if isinstance(project_id, str):
        project_id = [project_id]
    resource_names_list = [f"projects/{s}" for s in project_id]

    result = gcp_logging_client.list_entries(
        resource_names=resource_names_list,
        filter_=filter_str,
        order_by=order_by,
    )
    result = list(result)
    result = [log_flattener(r) for r in result]
    columns = ["log_name", "timestamp", "severity", "labels", "payload"]
    df_result = pd.DataFrame(result, columns=columns)
    df_result.labels = df_result.labels.apply(lambda r: json.loads(r))
    return df_result
