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

    Returns
    -------
    list of str, with items
        log_name : str
        severity : 
        order_by : 
        max_results : 
    """
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
    order_by="ascending"
):
    """Retrieve Cloud Logging data from a specific GCP Project

    Parameters
    ----------
    gcp_credentials :
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    project_id : str, GCP project to run the query in
    log_name : str
    severity : 
    order_by :

    Returns
    -------
    Pandas DataFrame, logging data with columns
        log_name : name of log source
        timestamp : timestamp of logging event
        severity : severity level of the logging event
        labels : unique labels applied to event
        payload : str, 
    """
    gcp_logging_client = google.cloud.logging.Client(
        credentials=gcp_credentials, project=project_id
    )

    order_mapper = {
        "ascending": google.cloud.logging.ASCENDING,
        "descending": google.cloud.logging.DESCENDING,
    }
    order_by = order_by.lower()
    order_by = order_mapper[order_by]

    filter_str = ""
    if severity is not None:
        if isinstance(severity, str):
            filter_str += f"severity={severity}\n"
        elif isinstance(severity, Iterable):
            severity = " OR ".join(severity)
            filter_str += f"severity=({severity})\n"
    if project_id is not None:
        filter_str += f'resource.labels.project_id="{project_id}"\n'
    if log_name is not None:
        filter_str += f"logName=projects/{project_id}/logs/{log_name}"

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

    return df_result
