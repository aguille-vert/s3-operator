from joblib import Parallel, delayed, parallel_backend, dump, load
import json
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import csv



def get_folder_size(s3_client, 
                    bucket_name, 
                    prefix):

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    total_size = 0

    # Iterate over objects in the folder
    for obj in response['Contents']:
        total_size += obj['Size']

    return total_size

# Provide your bucket name and folder prefix
bucket_name = 'your-bucket-name'
folder_prefix = 'your-folder-prefix/'

folder_size = get_folder_size(bucket_name, folder_prefix)

print("Folder size:", folder_size, "bytes")
print("Folder size (in KB):", folder_size / 1024, "KB")
print("Folder size (in MB):", folder_size / (1024 * 1024), "MB")
print("Folder size (in GB):", folder_size / (1024 * 1024 * 1024), "GB")



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
                        additional_str = '', 
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

    if additional_str != '':
      return [i for i in keys_ts_list if additional_str in i[0]]
    else:
      return keys_ts_list

def get_latest_keys_from_(s3_client,
                          bucket, 
                          prefix, 
                          time_interval=1, 
                          time_unit='hour', 
                          additional_str=''):
  """
  Get the latest keys from an S3 bucket within a specified time interval.

  Args:
      bucket (str): The name of the S3 bucket.
      prefix (str): The prefix used to filter the S3 bucket objects.
      time_interval (int, optional): The time interval for filtering keys. Default is 1.
      time_unit (str, optional): The time unit of the time_interval. Can be 'second', 'hour', or 'day'. Default is 'hour'.
      additional_str (str, optional): An additional string to filter the keys. Default is ''.

  Returns:
      tuple: A tuple containing two elements:
          - last_ts_hour (str): The latest timestamp hour as a string in the format "YYYY-MM-DD-HH".
          - latest_keys (list): A list of keys within the specified time_interval.

  Raises:
      AssertionError: If the `additional_str` is not of type 'str'.
      AssertionError: If the `time_unit` is not 'second', 'hour', or 'day'.
  """


  add_str_expr="Error: 'additional_str' should be of type 'str', got"
  assert isinstance(additional_str, str), f"{add_str_expr} '{type(additional_str)}'"
  assert time_unit in ['second',
                        'hour',
                        'day'], f"time unit should be second,hour or day"

  obj = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
  pat = re.compile(additional_str, re.I)
  keys = [(i['Key'], i['LastModified']) for i in obj['Contents'] if pat.search(i['Key'])]

  if keys:

    keys.sort(key=lambda x: x[1], reverse=True)

    ts_latest = keys[0][1]
    time_units = {'second': 'seconds', 'hour': 'hours', 'day': 'days'}
    ts_earliest = ts_latest - timedelta(**{time_units[time_unit]: time_interval})

    latest_keys = [key[0] for key in keys if ts_earliest <= key[1] <= ts_latest]
    last_ts_hour = ts_latest.strftime("%Y-%m-%d-%H")

    return last_ts_hour, latest_keys
  else:
    print("no keys")
    return None, None

def pd_read_parquet(_s3_client,bucket,key,columns=None):

    """
    Reads a Parquet file from an S3 bucket and returns a pandas DataFrame.

    This function reads a Parquet file stored in an S3 bucket and converts it into a pandas DataFrame.
    If the `columns` parameter is provided, only the specified columns will be read from the Parquet file.

    Args:
        _s3_client (boto3.client): A boto3 S3 client instance.
        bucket (str): The name of the S3 bucket containing the Parquet file.
        key (str): The key (path) of the Parquet file in the S3 bucket.
        columns (list, optional): A list of column names to read from the Parquet file. If not provided, all columns will be read.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the Parquet file, or None if an exception occurs.

    Examples:
        >>> s3_client = boto3.client('s3')
        >>> bucket = 'my-bucket'
        >>> key = 'path/to/parquet_file.parquet'
        >>> df = pd_read_parquet(s3_client, bucket, key)
        >>> df.head()
    """


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

def upload_csv_file_to_bucket(s3_client, 
                                data,
                                bucket,
                                key,
                                headers=None):


    buffer = StringIO()
    
    writer = csv.writer(buffer, delimiter=',')
    
    writer.writerow(headers)
    writer.writerows(data)
    s3_client.put_object(Bucket = bucket, Key = key, Body = buffer.getvalue())
