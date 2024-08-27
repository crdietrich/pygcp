"""Cloud Storage for Google Cloud Platform

Copyright (c) 2023 Colin Dietrich
MIT License, see LICENSE file for complete text.

TODO: be consistent with terms blob, object and file.
"""

import os
import pandas as pd

from google.cloud import storage


def id_to_uri(bucket_id):
    """Convert GCS bucket ID to URI

    Parameters
    ----------
    bucket_id : str
        Bucket ID

    Returns
    -------
    str
        Bucket URI
    """
    return f"gs://{bucket_id}"

def uri_to_id(uri):
    """Convert GCS URI to bucket ID

    Parameters
    ----------
    uri : str
        Bucket URI

    Returns
    -------
    str
        Bucket ID
    """
    return uri.split('//')[1]

def id_to_url(bucket_id):
    """Convert GCS bucket ID to URL

    Parameters
    ----------
    bucket_id : str
        Bucket ID

    Returns
    -------
    str
        URL for bucket
    """
    return f"https://console.cloud.google.com/storage/browser/{bucket_id}"

def url_to_id(url):
    """Convert GCS URL to bucket ID

    Parameters
    ----------
    url : str
        URL for bucket

    Returns
    -------
    str
        Bucket ID
    """
    return url.split('/')[-1]

def size_scaler(size, base=1024):
    """Scale the size of storage object depending on it's size in bytes.

    Parameters
    ----------
    size : int
        Size of object in bytes

    Returns
    -------
    size : float
        Size in units
    unit : str
        Unit of the size value; one of bytes, MB, KB, GB or TB.
    """
    if size < base:
        return size, 'bytes'
    elif size < base**2:
        return size / base, 'MB'
    elif size < base**3:
        return size / (base**2), 'KB'
    elif size < base**4:
        return size / (base**3), 'GB'
    elif size < base**5:
        return size / (base**4), 'TB'
    
def blob_get_metadata(blob, output='list'):
    """Get common blob information.

    Parameters
    ----------
    blob : TODO
        Storage object

    Returns
    -------
    dict
        name : str
            Name of object.
        size : float
            Size of object in size_unit units.
        size_unit : str
            Unit of the size value.
        size_bytes : int
            Size of object in bytes.
        type : str
            Content type.
        created : str
            Date of creation for the object.
    """
    size, size_unit = size_scaler(blob.size, 1024)
    info = [blob.name,
            round(size, 2),
            size_unit,
            blob.size,
            blob.content_type,
            str(blob.time_created)]
    if output == 'list':
        return info
    if output == 'dict':
        names = ['name', 'size', 'size_unit', 'size_bytes', 'type', 'created']
        return dict(zip(names, info))

def bucket_iam_members(gcp_credentials, bucket_name, verbose=False):
    """Get Storage bucket IAM members
    
    Parameters
    ----------
    gcp_credentials : google.auth.compute_engine.credentials.Credentials or google.oauth2.service_account.Credentials
        Instance of authenticated credentials
    bucket_name : str
        Unique bucket ID.
    verbose : bool
        Print debug statements. Default is False
        
    Returns
    -------
    list
        2 item string lists, [IAM role, IAM members with role] 
    """
    gcs_client = storage.Client(credentials=gcp_credentials)
    bucket = gcs_client.bucket(bucket_name)
    policy = bucket.get_iam_policy(requested_policy_version=3)

    iam_members = []
    for binding in policy.bindings:
        role = binding['role']
        members = binding['members']
        if verbose:
            print(f"Role: {role}, Members: {members}")
        iam_members.append([role, members])
    return iam_members

def blob_rename(gcp_credentials, bucket_name, 
                blob_name, blob_new_name, verbose=False):
    """Rename a blob in a storage bucket.
    
    Parameters
    ----------
    gcp_credentials : google.auth.compute_engine.credentials.Credentials or google.oauth2.service_account.Credentials
        Instance of authenticated credentials
    bucket_name : str
        Unique bucket ID. 
    blob_name : str
        Name of file in bucket to rename.
    blob_new_name : str
        New name for file.
    verbose : bool
        Print debug statements. Default is False.
    
    Returns
    -------
    None
    """
    
    gcs_client = storage.Client(credentials=gcp_credentials)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.rename_blob(blob, blob_new_name)
    if verbose:
        print(f"Blob {blob.name} has been renamed to {new_blob.name}")


def bucket_dataframe(gcp_credentials, bucket_name, criteria=None, verbose=False):
    """
    
    Parameters
    ----------
    gcp_credentials : google.auth.compute_engine.credentials.Credentials or google.oauth2.service_account.Credentials
        Instance of authenticated credentials
    bucket_name : str
        Unique bucket ID. 
    criteria : str
        Pattern to match in files to download. Default is None which selects all files.
    verbose : bool
        Print debug statements. Default is False.

    Returns
    -------
    Pandas DataFrame
        Columns: IAM role, IAM members 
    """
    blob_list = blob_select(gcp_credentials, bucket_name, criteria, verbose)
    return pd.DataFrame([blob_get_metadata(b, 'dict') for b in blob_list])


def blob_select(gcp_credentials, bucket_name, criteria, verbose=False):
    """
    
    Parameters
    ----------
    gcp_credentials : google.auth.compute_engine.credentials.Credentials or google.oauth2.service_account.Credentials
        Instance of authenticated credentials
    bucket_name : str
        Unique bucket ID. 
    criteria : str
        Pattern to match in files to download. Default is None which selects all files.
    verbose : bool
        Print debug statements. Default is False.

    Returns
    -------
    list, blobs found matching name criteria
    """
    
    gcs_client = storage.Client(credentials=gcp_credentials)
    # Note: Client.list_blobs requires at least package version 1.17.0.
    _blob_list = gcs_client.list_blobs(bucket_name)
    
    blob_list_found = []
    # Note: The call returns a response only when the iterator is consumed.
    for _blob in _blob_list:
        if verbose:
            print(_blob.name)
        if (criteria is not None) and (criteria in _blob.name):
            blob_list_found.append(_blob)
    return blob_list_found

def blob_download(gcp_credentials, bucket_name, criteria, destination=None,
                  rename=None, truncate=False, extension=None, verbose=False):
    """Download a single blob to the local machine.
    
    Parameters
    ----------
    gcp_credentials : google.auth.compute_engine.credentials.Credentials or google.oauth2.service_account.Credentials
        Instance of authenticated credentials
    bucket_name : str
        Unique bucket ID. 
    criteria : str
        Pattern to match in files to download. Default is None which selects all files.
    destination : str, optional.
        Local path to save blob(s) to. Defaults to current working directory.
    rename : str, optional.
        If set, the new name for the storage object when saved as a local file.
    truncate : bool
        Truncate the blob full filepath (delimited by /) to just the filename.
    extension : str, optional.
        Add a file extension to the local file being saved.
    verbose : bool
        Print debug statements. Default is False.

    Returns
    -------
    dict
        Metadata about downloads. Key for each matching blob
        is the original blob name. Values of each dict item are:
            name : str
                Name of blob in GCS.
            size : int
                Size in bytes.
            new_name : str
                New name for file.
    """
    
    _blob_list = blob_select(gcp_credentials=gcp_credentials,
                             bucket_name=bucket_name,
                             criteria=criteria)
    _metadata = {}
    _blob_list = list(enumerate(_blob_list))
    n_total = len(_blob_list)
    numbered = n_total > 1
    z_fill = len(str(n_total))
    
    for n, _blob in _blob_list:

        _m = {'name': _blob.name,
              'size': _blob.size,
              'new_name': None}
        f_name = _blob.name
        if truncate:
            f_name = _blob.name.split('/')[-1]
        elif rename is not None:
            f_name = rename
            if numbered:
                f_name += f'_{str(n).zfill(z_fill)}'
            if extension is not None:
                f_name += f'.{extension}'
            _m['new_name'] = f_name
        elif extension is not None:
            f_name += f'{_blob.name}.{extension}'

        _fp = destination + os.path.sep + f_name
        if destination is not None:
            _m['destination'] = destination
        if verbose:
            print(f'Saving to local: {_fp}')
        _blob.download_to_filename(_fp)
        _metadata[_blob.name] = _m
    return _metadata
