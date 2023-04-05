# Fast interaction with AWS S3 buckets

boto3 client is used to interact with s3 buckets  
joblib Parallel is used to for acceleration

## Functions:

get_keys_from_(s3_client,  
                  bucket,  
                  prefix,  
                   verbose=0,  
                   n_jobs = -1)  
                   
    Retrieve object keys from an Amazon S3 bucket with the specified prefix.  
  
    Parameters:
        s3_client (boto3.client): An instance of the Amazon S3 client.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix used to filter object keys.
        verbose (int, optional): Controls verbosity of the output. Default is 0 (no output).
                                Set to 1 to print progress messages.
        n_jobs (int, optional): The number of parallel jobs to run. Default is -1 (all available cores).

    Returns:
        list: A list of object keys that match the specified prefix in the given S3 bucket.
