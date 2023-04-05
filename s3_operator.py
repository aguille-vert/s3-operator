from joblib import Parallel, delayed, parallel_backend, dump, load 


def get_page_iterator_from(s3_client,
                           bucket,
                           prefix=''):
  paginator = s3_client.get_paginator('list_objects')
  return paginator.paginate(Bucket=bucket,
                                    Prefix = prefix)

def get_page_iterator_keys_ts_from_(page):
    return  [(i['Key'],i['LastModified']) for i in page['Contents']]


def get_keys_from_(s3_client,
                   bucket,
                   add_str = '', 
                   prefix='',
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

    return [i for i in keys_ts_list if add_str in i[0]]

