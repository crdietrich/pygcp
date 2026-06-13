"""BigQuery on Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""

import importlib
import inspect
import time

import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

from . import job


def is_google_exception(e):
    """Decide if the Exception passed is a Google API Exception

    Parameters
    ----------
    e : Exception

    Returns
    -------
    bool
        True if the Exception is a Google API Exception
    """
    m = importlib.import_module("google.api_core.exceptions")
    exception_list = []
    for k in m.__dict__.keys():
        if inspect.isclass(m.__dict__[k]):
            exception_list.append(k)
    print(exception_list)
    if e.__class__.__name__ in exception_list:
        return True
    else:
        return False


def basic_query(gcp_credentials, query_text):
    """Basic BigQuery query using all defaults

    Parameters
    ----------
    gcp_credentials : an instance of either
        google.auth.compute_engine.credentials.Credentials
        google.oauth2.service_account.Credentials
    query_text : str
        SQL query to run

    Returns
    -------
    Pandas DataFrame
        Query job result
    """

    qjc = job.query_config()
    download_kwargs = {
        "gcp_credentials": gcp_credentials,
        "query_text": query_text,
        "query_job_config": qjc,
        "verbose": True,
    }
    metadata, df = to_df(**download_kwargs)
    return df


def job_timer(query_job, timeout, total_bytes=None, verbose=False):
    """Time a query job execution and cancel it if it exceeds a timeout

    Parameters
    ----------
    query_job : google.cloud.bigquery.job.query.QueryJob
        Currently running query job instance
    timeout : int, optional, defaults to None
        Timeout in minutes
    total_bytes : int
        Total number of bytes expected processed in the operation
    verbose : bool
        print debug statements
    """

    t0 = time.time()
    if timeout is not None:
        timeout = int(timeout) * 60  # minutes > seconds

    def progress_generator():
        while not query_job.done():
            yield

    with tqdm(
        progress_generator(), desc="BigQuery query bytes", total=total_bytes
    ) as pbar:
        dt = time.time() - t0
        if timeout is not None:
            if dt > timeout:
                query_job.cancel()
                time.sleep(1)
                return
        time.sleep(0.001)
        try:
            total_bytes = int(
                query_job._properties["statistics"]["totalBytesProcessed"]
            )
            pbar.update(total_bytes)
        except KeyError:
            if verbose:
                print("totalBytesProcessed key missing")
                print(query_job.__dict__)


def create_dataset(
    gcp_credentials,
    project_id,
    dataset_id,
    location_id="us",
    exists_ok=False,
    verbose=False,
):
    """Create a dataset on in the specified project

    Parameters
    ----------
    gcp_credentials :
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    project_id : str
        GCP project to run the query in
    dataset_id : str
        name of dataset to create
    location_id : str, default='us'
        GCP region to create dataset in
    exists_ok : bool, default=False
        True = ignore “already exists” errors when creating the dataset
    verbose : bool, default=True
        print debug statements

    Returns
    -------
    TBD Class instance
        A new BigQuery copy job instance
    """

    dataset = bigquery.Dataset(project_id + "." + dataset_id)
    dataset.location = location_id

    bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)
    copy_job = bq_client.create_dataset(dataset, exists_ok=exists_ok)

    if verbose:
        print("Created dataset {}.{}".format(bq_client.project, dataset.dataset_id))
    # TODO: get copy_job exact class type
    print("TEMP pygcp.bq.base.create_dataset type checking")
    print(type(copy_job))
    return copy_job


def upload_dataframe(
    gcp_credentials,
    df_upload,
    load_job_config,
    project_id,
    dataset_id,
    table_id,
    timeout=None,
    verbose=False,
):
    """Upload a dataframe to BigQuery

    Notes
    -----
    Best to set `schema`, `create_disposition and `write_disposition`
    attributes of the LoadJobConfig instance to avoid inferred errors
    or data overwriting. See bq.job.load_config.

    Parameters
    ----------
    gcp_credentials :
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    df_upload : Pandas DataFrame, data to upload to BigQuery
    load_job_config : class, google.cloud.bigquery.job.load.LoadJobConfig
        Load job configuration for this operation with
        matching bigquery.SchemaField attributes for all DataFrame columns
    project_id : str, GCP project to create dataset in
    dataset_id : str, BigQuery dataset to create table in
    table_id : str, table name to upload data to
    timeout : int, timeout in minutes. Optional, defaults to None
    verbose : bool, print debug statements

    Returns
    -------
    dict, state of google.cloud.bigquery.job.load.LoadJob._properties
        at completion of load job
    """

    bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)
    dataset_table_id = dataset_id + "." + table_id

    _load = bq_client.load_table_from_dataframe
    load_job = _load(
        df_upload, destination=dataset_table_id, job_config=load_job_config
    )
    job_timer(running_job=load_job, timeout=timeout, verbose=verbose)

    load_job_result = load_job.result()
    return load_job_result._properties


def upload_files(
    gcp_credentials,
    project_id,
    dataset_id,
    table_id,
    upload_type,
    source,
    job_config,
    verbose=False,
):
    """Upload all files in source directory to BigQuery

    Parameters
    ----------
    gcp_credentials : class instance of either
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    project_id : str
        GCP project to create dataset in
    dataset_id : str
        BigQuery dataset to create table in
    table_id : str
        table name to upload data to
    upload_type : str, default='local'
        type of upload, either: 'local' or 'gcs'
    source : str, or list of either
        1. paths to files to upload if upload_type is 'local'
        2. uris to upload if upload_type is 'gcs'
    job_config : BigQuery LoadJobConfig instance
        Load Job configuration, created with job.load_config
    verbose : bool, default=False
        print debug statements

    Returns
    -------
    google.cloud.bigquery.job.LoadJob

    See Also
    --------
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
    """

    bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)

    destination = ".".join([project_id, dataset_id, table_id])

    if type(source) is str:
        source = [source]

    load_job = None

    if upload_type == "local":
        for fp_source in source:
            with open(fp_source, "rb") as f_open:
                if verbose:
                    print("Starting Upload...")
                    print("From:", fp_source)
                    print("To:", destination)

                load_job = bq_client.load_table_from_file(
                    file_obj=f_open, destination=destination, job_config=job_config
                )
                if verbose:
                    print("Job Started: {}".format(load_job.job_id))
                load_job.result()  # Waits for table load to complete.
                if verbose:
                    print("Job finished: {}".format(load_job.done()))

    elif upload_type == "gcs":
        source_uris = source
        load_job = bq_client.load_table_from_uri(
            source_uris=source_uris, destination=destination, job_config=job_config
        )
        if verbose:
            print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        if verbose:
            print(f"Job finished: {load_job.done()}")

    return load_job


def to_df(
    gcp_credentials,
    query_text,
    query_job_config=None,
    project_id=None,
    timeout=None,
    verbose=False,
):
    """Upload a dataframe to BigQuery

    Notes
    -----

    Parameters
    ----------
    gcp_credentials : class instance of either
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    query_text : str
        query to execute and convert to a DataFrame
    query_job_config : google.cloud.bigquery.job.load.QueryJobConfig, optional
        Query job configuration for this operation. Default is
        an instance with no attributes.
    project_id : str, optional
        Project ID for the project which the client acts on behalf of.
        Will be passed when creating a dataset / job. If not passed,
        falls back to the default inferred from the environment.
    timeout : int, optional
        Timeout in minutes
    verbose : bool, default=False
        print debug statements

    Returns
    -------
    metadata : dict
        state of google.cloud.bigquery.job.load.QueryJob._properties
        at completion of query job
    df : Pandas DataFrame
        query output
    """
    bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)

    qjc_dry_run = bigquery.QueryJobConfig()
    qjc_dry_run.dry_run = True

    query_dry_run = bq_client.query(query_text, job_config=qjc_dry_run)

    total_bytes = int(query_dry_run._properties["statistics"]["totalBytesProcessed"])

    if query_job_config is None:
        query_job_config = job.query_config()

    query_job = bq_client.query(query_text, job_config=query_job_config)

    job_timer(
        query_job=query_job,
        timeout=timeout,
        verbose=verbose,
        total_bytes=total_bytes,
    )

    query_job_result = query_job.result()
    df_query = query_job_result.to_dataframe()
    return query_job._properties, df_query


def execute(
    gcp_credentials,
    query_text,
    query_job_config=None,
    project_id=None,
    timeout=None,
    verbose=False,
):
    """Execute an operation on BigQuery that will not return data such as
    dataset, table or BQML model creation.

    Parameters
    ----------
    gcp_credentials : class instance of either
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    query_text : str
        query to execute and convert to a DataFrame
    query_job_config : google.cloud.bigquery.job.load.QueryJobConfig, optional
        Query job configuration for this operation. Default is
        an instance with no attributes.
    project_id : str, optional
        Project ID for the project which the client acts on behalf of.
        Will be passed when creating a dataset / job. If not passed,
        falls back to the default inferred from the environment.
    timeout : int, optional
        Timeout in minutes
    verbose : bool, default=False
        print debug statements

    Returns
    -------
    metadata : dict
        state of google.cloud.bigquery.job.load.QueryJob._properties
        at completion of query job
    """
    query_job_metadata, _ = to_df(
        gcp_credentials,
        query_text,
        query_job_config,
        project_id,
        timeout=timeout,
        verbose=verbose,
    )
    return query_job_metadata


def query_result_dataframe(query_job):
    """Convert a query job result to Pandas DataFrame

    Parameters
    ----------
    query_job : google.cloud.bigquery.job.query.QueryJob
        query job class instance

    Returns
    -------
    df : Pandas DataFrame
        DataFrame output of BigQuery query
    """
    query_result = query_job.result()
    return query_result.to_dataframe()


def query_result_list(query_job):
    """Convert a query job result to list

    Parameters
    ----------
    query_job : google.cloud.bigquery.job.query.QueryJob
        query job class instance

    Returns
    -------
    list
        output of BigQuery query
    """
    query_result = query_job.result()
    return list(query_result)


def query_result_metadata_summary(query_job):
    """Print query job summary

    Parameters
    ----------
    query_job : bigquery.query_job
        query job class instance
    """

    t0 = query_job.started
    t1 = query_job.ended
    dt = t1 - t0

    print("Query Statistics")
    print("================")
    print(f"Total Time Elapsed: {dt}")
    print(f"Cache Hit: {query_job.cache_hit}")
    print(f"Bytes Processed: {query_job.total_bytes_processed}")
    print(f"Bytes Billed: {query_job.total_bytes_billed}")
    print(f"Slot Milliseconds: {query_job._properties['statistics']['totalSlotMs']}")


def inventory(gcp_credentials, project_id=None, verbose=False):
    """Inventory all datasets and tables in a GCP Project

    Parameters
    ----------
    gcp_credentials : class instance of either
        google.auth.compute_engine.credentials.Credentials
            OR
        google.oauth2.service_account.Credentials
    project_id : str, optional
        Project ID for the project which the client acts on behalf of.
        Will be passed when creating a dataset / job. If not passed,
        falls back to the default inferred from the environment.
    verbose : bool, default=False
        print debug statements

    Returns
    -------
    df : Pandas Dataframe with columns
        project : str
            project name
        location : str
            location of dataset
        dataset_id : str
            identifying name of dataset
        size_MB : float,

    """
    # TODO: add other column descriptions
    client_bq = bigquery.Client(credentials=gcp_credentials, project=project_id)
    dataset_list = list(client_bq.list_datasets())
    project_id = client_bq.project

    _data = []

    if dataset_list:
        if verbose:
            print("Datasets in project {}:".format(project_id))

        for dataset_obj in dataset_list:
            location = dataset_obj._properties["location"]
            dataset_id = dataset_obj.dataset_id
            if verbose:
                print("\t{}".format(dataset_id))

            query_text = f"""SELECT * FROM `{dataset_id}.__TABLES__`"""

            # returns a google.cloud.bigquery.job.query.QueryJob instance
            qjc = job.query_config()

            _metadata, _df = to_df(
                gcp_credentials=gcp_credentials,
                query_job_config=qjc,
                query_text=query_text,
                project_id=project_id,
                timeout=None,
                verbose=False,
            )

            _df["dataset_id"] = dataset_id
            _df["location"] = location
            _df["size_MB"] = _df.size_bytes / (1024 * 1024)
            _df["size_GB"] = _df.size_MB / 1024
            _df["size_TB"] = _df.size_GB / 1024
            _data.append(_df)

        if len(_data) == 0:
            return pd.DataFrame()

        _df_out = pd.concat(_data)
        return _df_out
    else:
        if verbose:
            print("{} project does not contain any datasets.".format(project_id))
        return
