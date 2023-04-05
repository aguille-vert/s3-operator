# S3 JSON Data Retrieval Utilities

This repository contains three utility functions for working with JSON data stored in Amazon S3 buckets. These functions allow you to easily retrieve JSON data from multiple files within an S3 bucket and process them in Python.

## Functions

### 1. `get_keys_ts_from_(s3_client, bucket, prefix='', verbose=0, n_jobs=-1)`

This function retrieves a list of keys (paths) and their corresponding last modified timestamps from an S3 bucket, filtered by a specified prefix.

### 2. `read_json_from_(s3_client, bucket, key)`

This function reads a JSON file from an S3 bucket and returns the parsed data as a Python object (usually a dictionary or a list).

### 3. `get_json_data_from_(s3_client, bucket, prefix='', n_jobs=-1, verbose=1, unpack_list=False)`

This function retrieves JSON data from multiple files in an S3 bucket and returns the parsed data as a list of Python objects.

## Usage

To use these functions, you'll first need to install the `boto3` library and set up an S3 client:

```python
import boto3

s3_client = boto3.client('s3')
bucket = 'my-bucket'
prefix = 'path/to/json_files/
```


Then, you can call the functions as needed:

```python
keys_ts_list = get_keys_ts_from_(s3_client, bucket, prefix)
json_data = read_json_from_(s3_client, bucket, key)
json_data_list = get_json_data_from_(s3_client, bucket, prefix)
```
