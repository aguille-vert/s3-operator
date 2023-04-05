from joblib import Parallel, delayed, parallel_backend, dump, load
import json
from io import BytesIO
import pandas as pd
import numpy as np


def get_page_iterator_from(s3_client,
                           bucket,
                           prefix=''):
  paginator = s3_client.get_paginator('list_objects')
  return paginator.paginate(Bucket=bucket,
                                    Prefix = prefix)

def get_page_iterator_keys_ts_from_(page):
    return  [(i['Key'],i['LastModified']) for i in page['Contents']]


def get_keys_ts_from_(s3_client,
                        bucket,
                        prefix='',
                        add_str = '', 
                        n_jobs=-1,
                        verbose=0):
    """
    Retrieve object keys from an Amazon S3 bucket with the specified prefix.
    Parameters:
        s3_client (boto3.client): An instance of the Amazon S3 client.
        bucket (str): The name of the S3 bucket.
        add_str (str): Additional string anywhere in the key string to filter object keys.
        prefix (str): The prefix used to filter object keys.
        verbose (int, optional): Controls verbosity of the output. Default is 0 (no output).
                                Set to 1 to print progress messages.
        n_jobs (int, optional): The number of parallel jobs to run. Default is -1 (all available cores).

    Returns:
        list: A list of object keys that match the specified prefix in the given S3 bucket.
    """

    
    page_iterator = get_page_iterator_from(s3_client,
                            bucket,
                            prefix=prefix)
    if verbose==1:
        print('started downloading keys')

    with parallel_backend('threading', n_jobs=n_jobs):
        keys_ts_list_of_lists = Parallel(verbose=verbose)(delayed(get_page_iterator_keys_ts_from_)(page) 
                                                        for page  in page_iterator)
    keys_ts_list =  [item for sublist in keys_ts_list_of_lists for item in sublist]
    if verbose==1:
        print(f'downloaded {len(keys_ts_list)} keys')

    if add_str != '':
      return [i for i in keys_ts_list if add_str in i[0]]
    else:
      return keys_ts_list

def pd_read_parquet(_s3_client,bucket,key,columns=None):
  try:
    obj = _s3_client.get_object(Bucket=bucket,Key=key)
    buffer = BytesIO(obj['Body'].read())
    if columns:
      return pd.read_parquet(buffer,
                            columns=columns)
    else:
      return pd.read_parquet(buffer)
  except:
    pass

def read_json_from_(s3_client,
                    bucket,
                    key):

    """
    Reads a JSON file from an Amazon S3 bucket and returns the parsed data as a Python object.

    Args:
        s3_client (boto3.client): A boto3 S3 client instance used to access the Amazon S3 service.
        bucket (str): The name of the S3 bucket containing the JSON file.
        key (str): The key (path) of the JSON file in the S3 bucket.

    Returns:
        dict or list or None: The JSON data as a Python object (usually a dictionary or a list) if the file
                              is read successfully, or None if an error occurs while reading the file.

    Example:
        s3_client = boto3.client('s3')
        bucket = 'my-bucket'
        key = 'path/to/myfile.json'
        json_data = read_json_from_(s3_client, bucket, key)
    """
    try:
        obj=s3_client.get_object(Bucket=bucket,
                                  Key=key)
        return json.loads(obj['Body'].read())
    except:
        return None

def get_json_data_from_(s3_client,
                        bucket,
                        prefix='',
                        n_jobs=-1,
                        verbose=1,
                        unpack_list=False):
    """
    Retrieves JSON data from multiple files in an Amazon S3 bucket and returns the parsed data as a list of Python objects.

    Args:
        s3_client (boto3.client): A boto3 S3 client instance used to access the Amazon S3 service.
        bucket (str): The name of the S3 bucket containing the JSON files.
        prefix (str, optional): The common prefix for the keys (paths) of the JSON files in the S3 bucket. Defaults to ''.
        n_jobs (int, optional): The number of concurrent jobs to run for reading JSON files. Defaults to -1 (all available CPUs).
        verbose (int, optional): Controls the verbosity of the function's output. Set to 1 for progress messages, 0 for silent operation. Defaults to 1.
        unpack_list (bool, optional): If True, the function will return a flat list of tuples containing the unpacked JSON data and its corresponding timestamp. If False, the function will return a list of tuples containing the parsed JSON data (without unpacking) and its corresponding timestamp. Defaults to False.

    Returns:
        list: A list of tuples containing the JSON data as Python objects (usually dictionaries or lists) and their corresponding timestamps.

    Example:
        s3_client = boto3.client('s3')
        bucket = 'my-bucket'
        prefix = 'path/to/json_files/'
        json_data = get_json_data_from_(s3_client, bucket, prefix)
    """
  

    keys_ts_list = get_keys_ts_from_(s3_client,
                        bucket,
                        prefix,
                        verbose=verbose,
                        n_jobs = n_jobs)
    
    ts_list = [i[1] for i in keys_ts_list]
    key_list = [i[0] for i in keys_ts_list]

    if verbose==1:
        print('downloading json_data')
    with parallel_backend('threading', n_jobs=n_jobs):
        json_data = Parallel(verbose=verbose)(delayed(read_json_from_)(
                                                    s3_client,
                                                    bucket,
                                                    key) for key in key_list)
    if unpack_list:
      if verbose==1:
        print(f'downloaded {len(json_data)} json_files')
      return [(item, ts) for ts, sublist in zip(ts_list, json_data) for item in sublist]

    else:
      return (json_data, ts_list)

def pd_save_parquet(_s3_client, df, bucket, key, schema=None):
    """
    Save a Pandas DataFrame as a parquet file to an S3 bucket.

    Args:
        _s3_client (boto3.client): A boto3 S3 client instance.
        df (pandas.DataFrame): The DataFrame to be saved as a parquet file.
        bucket (str): The name of the S3 bucket where the parquet file will be saved.
        key (str): The key (path) where the parquet file will be saved in the S3 bucket.
        schema (pyarrow.Schema, optional): The schema to use when saving the DataFrame. Defaults to None.

    Returns:
        None
    """
    buffer = BytesIO()
    if schema:
        df.to_parquet(buffer, schema=schema)
    df.to_parquet(buffer)
    _s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

