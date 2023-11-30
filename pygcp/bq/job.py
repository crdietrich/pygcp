"""Bigquery Load and Query Job Wrappers for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""


from google.cloud import bigquery


def load_metadata(load_job):
    """Generate metadata for a completed BigQuery Load Job"""
    metadata = {
        "job_id": load_job.job_id,
        "clustering_fields": load_job.clustering_fields,
        "time_partitioning": load_job.time_partitioning,
        "created": load_job.created,
        "destination": load_job.destination,
        "input_file_bytes": load_job.input_file_bytes,
        "output_bytes": load_job.output_bytes,
        "t0": load_job.started.isoformat(),
        "t1": load_job.ended.isoformat(),
    }
    return metadata


def load_config(**kwargs):
    r"""Set common BigQuery LoadJobConfig attributes

    Notes
    -----
    1. Google Cloud Client API Documentation:
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJob.html
    https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html
    https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload

    2. Not all attributes supported by the Python API and BigQuery API are represented in this wrapper,
    see links for full list.


    Keyword Arguments
    -----------------
    allow_jagged_rows : bool, optional
        Accept rows that are missing trailing optional columns. The missing values are treated as nulls. If false, records with missing trailing columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. Only applicable to CSV, ignored for other formats.

    allow_quoted_newlines : bool, optional
        Indicates if BigQuery should allow quoted data sections that contain newline characters in a CSV file. The default value is false.

    autodetect : bool, optional
        Automatically infer the options and schema for CSV and JSON sources.

    clustering_fields : list of str, optional
        Fields defining clustering for the table, clustering fields are immutable after table creation. Note: BigQuery supports clustering for both partitioned and non-partitioned tables.

    create_disposition : str, optional
        Specifies whether the job is allowed to create new tables. The following values are supported:
        CREATE_IF_NEEDED: If the table does not exist, BigQuery creates the table.
        CREATE_NEVER: The table must already exist. If it does not, a 'notFound' error is returned in the job result. The default value is CREATE_IF_NEEDED. Creation, truncation and append actions occur as one atomic update upon job completion.

    encoding : str, optional
        The character encoding of the data. The supported values are UTF-8 or ISO-8859-1. The default value is UTF-8. BigQuery decodes the data after the raw, binary data has been split using the values of the quote and fieldDelimiter properties.

    field_delimiter : str, optional
        The separator character for fields in a CSV file. The separator is interpreted as a single byte. For files encoded in ISO-8859-1, any single character can be used as a separator. For files encoded in UTF-8, characters represented in decimal range 1-127 (U+0001-U+007F) can be used without any modification. UTF-8 characters encoded with multiple bytes (i.e. U+0080 and above) will have only the first byte used for separating fields. The remaining bytes will be treated as a part of the field. BigQuery also supports the escape sequence "\t" (U+0009) to specify a tab separator. The default value is comma (",", U+002C).

    ignore_unknown_values : bool, optional
        Indicates if BigQuery should allow extra values that are not represented in the table schema. If true, the extra values are ignored. If false, records with extra columns are treated as bad records, and if there are too many bad records, an invalid error is returned in the job result. The default value is false. The sourceFormat property determines what BigQuery treats as an extra value: CSV: Trailing columns JSON: Named values that don't match any column names in the table schema Avro, Parquet, ORC: Fields in the file schema that don't exist in the table schema.

    input_file_bytes
    input_files

    max_bad_records : int, optional
         The maximum number of bad records that BigQuery can ignore when running the job. If the number of bad records exceeds this value, an invalid error is returned in the job result. The default value is 0, which requires that all records are valid. This is only supported for CSV and NEWLINE_DELIMITED_JSON file formats.

    null_marker : str, optional
        Specifies a string that represents a null value in a CSV file. For example, if you specify "\N", BigQuery interprets "\N" as a null value when loading a CSV file. The default value is the empty string. If you set this property to a custom value, BigQuery throws an error if an empty string is present for all data types except for STRING and BYTE. For STRING and BYTE columns, BigQuery interprets the empty string as an empty value.

    output_bytes
    output_rows

    parquet_options : google.cloud.bigquery.format_options.ParquetOptions, optional
        Additional properties to set if sourceFormat is set to PARQUET.

    quote_character : str, optional
        The value that is used to quote data sections in a CSV file. BigQuery converts the string to ISO-8859-1 encoding, and then uses the first byte of the encoded string to split the data in its raw, binary state. The default value is a double-quote ('"'). If your data does not contain quoted sections, set the property value to an empty string. If your data contains quoted newline characters, you must also set the allowQuotedNewlines property to true. To include the specific quote character within a quoted value, precede it with an additional matching quote character. For example, if you want to escape the default character ' " ', use ' "" '. @default "

    schema : mapping of [str, any]
        The schema for the destination table. The schema can be omitted if the destination table already exists, or if you're loading data from Google Cloud Datastore.
        #TODO: clarify this!

    skip_leading_rows : int, optional
        The number of rows at the top of a CSV file that BigQuery will skip when loading the data. The default value is 0. This property is useful if you have header rows in the file that should be skipped. See API docs for details of when autodetect is on.

    source_format : str, optional
        The format of the data files.
        For CSV files, specify "CSV"
        For datastore backups, specify "DATASTORE_BACKUP"
        For newline-delimited JSON, specify "NEWLINE_DELIMITED_JSON"
        For Avro, specify "AVRO"
        For parquet, specify "PARQUET"
        For orc, specify "ORC"
        The default value is CSV

    time_partitioning : google.cloud.bigquery.table.TimePartitioning, optional
        Note: Only specify at most one of time_partitioning or range_partitioning.

    write_disposition : str, optional
        Specifies the action that occurs if the destination table already exists.
        The following values are supported:
        'WRITE_TRUNCATE': If the table already exists, BigQuery overwrites the table data and uses the schema from the load.
        'WRITE_APPEND': If the table already exists, BigQuery appends the data to the table.
        'WRITE_EMPTY': If the table already exists and contains data, a 'duplicate' error is returned in the job result.
        The default value is WRITE_APPEND. Each action is atomic and only occurs if BigQuery is able to complete the job successfully. Creation, truncation and append actions occur as one atomic update upon job completion.
    """

    ljc = bigquery.LoadJobConfig()
    for key, value in kwargs.items():
        setattr(ljc, key, value)
    return ljc


def load(job_id, source_uris, destination, client, job_config):
    """Asynchronous job for loading data into a table.

    Wrapper for google.cloud.bigquery.job.load.LoadJobConfig.
    Can load from Google Cloud Storage URIs or from a file.

    Parameters
    ----------
    job_id : str
        the job's ID

    source_uris : sequence of str, optional
        URIs of one or more data files to be loaded.  See
        https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad.FIELDS.source_uris
        for supported URI formats. Pass None for jobs that load from a file.

    destination : google.cloud.bigquery.table.TableReference
        reference to table into which data is to be loaded.

    client : google.cloud.bigquery.client.Client
        A client which holds credentials and project configuration
        for the dataset (which requires a project).
    job_config : google.cloud.bigquery.job.load.LoadJobConfig
        Configuration settings for this load operation
    """
    return bigquery.LoadJob(job_id, source_uris, destination, client, job_config)


def query_config(**kwargs):
    # noinspection GrazieInspection
    """Set common BigQuery QueryJobConfig attributes

    Notes
    -----
    1. Google Cloud Client API Documentation:
        https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJob.html

        https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html

    2. Attributes included are primarily for querying, some attributes
        for writing to new tables are excluded. See above links for details.

    Keyword Arguments
    -----------------
    allow_large_results : bool, optional
        If true and query uses legacy SQL dialect, allows the query to produce arbitrarily large result tables at a slight cost in performance. Requires destinationTable to be set. For standard SQL queries, this flag is ignored and large results are always allowed. However, you must still set destinationTable when result size exceeds the allowed maximum response size.

    clustering_fields : list of str, optional, defaults to None
        Fields defining clustering for the table

    connection_properties : list of str, optional
        Connection properties which can modify the query behavior

    create_disposition : str, optional
        Specifies whether the job is allowed to create new tables. The following values are supported:
        CREATE_IF_NEEDED: If the table does not exist, BigQuery creates the table.
        CREATE_NEVER: The table must already exist. If it does not, a 'notFound' error is returned in the job result. The default value is CREATE_IF_NEEDED. Creation, truncation and append actions occur as one atomic update upon job completion.

    create_session : bool, optional
        If this property is true, the job creates a new session using a randomly generated sessionId. To continue using a created session with subsequent queries, pass the existing session identifier as a ConnectionProperty value. The session identifier is returned as part of the SessionInfo message within the query statistics.

        The new session's location will be set to Job.JobReference.location if it is present, otherwise it's set to the default location based on existing routing logic.

    default_dataset : google.cloud.bigquery.dataset.DatasetReference, optional
        The default dataset to use for unqualified table names in the query or None if not set.

        The default_dataset setter accepts:
        a Dataset, or
        a DatasetReference, or
        a str of the fully-qualified dataset ID in standard SQL format. The value must include a project ID and dataset ID separated by a period. Example: your-project.your_dataset.

    destination : google.cloud.bigquery.table.TableReference, optional
        Table where results are written or None if not set.

        The destination setter accepts:
        a Table, or
        a TableReference, or
        a str of the fully-qualified table ID in standard SQL format. The value must include a project ID, dataset ID, and table ID, each separated by a period. Example: your-project.your_dataset.your_table.

    dry_run : bool, optional

        If set to True, don't actually run this job. A valid query will return a mostly empty response with some processing statistics, while an invalid query will return the same error it would if it wasn't a dry run. Behavior of non-query jobs is undefined.

    labels : dict of [str, str], optional
        Labels for the job.

        This method always returns a dict. Once a job has been created on the server, its labels cannot be modified anymore.

    maximum_bytes_billed : int, optional
        Limits the bytes billed for this job. Queries that will have bytes billed beyond this limit will fail (without incurring a charge). If unspecified, this will be set to your project default.

    priority : google.cloud.bigquery.job.QueryPriority, optional
        Specifies a priority for the query. Possible values include INTERACTIVE and BATCH. The default value is INTERACTIVE.

    udf_resources : list of google.cloud.bigquery.query.UDFResource, optional
        User defined function resources (empty by default)

    use_query_cache : bool, optional
        Whether to look for the result in the query cache. The query cache is a best-effort cache that will be flushed whenever tables in the query are modified. Moreover, the query cache is only available when a query does not have a destination table specified. The default value is true.

    Returns
    -------
    google.cloud.bigquery.job.QueryJob instance
    """
    qjc = bigquery.QueryJobConfig()
    for key, value in kwargs.items():
        setattr(qjc, key, value)
    return qjc
