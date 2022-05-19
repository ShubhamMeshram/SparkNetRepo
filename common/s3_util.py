from urllib.parse import urlparse

import boto3


def split_path_bucket_key(path):
    """Split a full path into a bucket and key for s3 writes.

    Does what it says.

    Parameters:
        path (str): full s3 path
    Returns:
        bucket (str): bucket portion of s3 path
        key (str): key portion of s3 path
    """
    parsed = urlparse(path)
    bucket = parsed.netloc
    key = parsed.path[1:]
    return (bucket, key)


def uploadlocaltoS3(file_type, config):
    """
    Create trg file on cluster and upload on s3
    """
    s3 = boto3.client("s3")
    if file_type == "user":
        bucket, key = split_path_bucket_key(
            config["paths"]["usr_api_raw"]["path"]
        )
        path = config["params"]["usr_json_response"]
        with open(path, "rb") as data:
            s3.upload_fileobj(data, bucket, key)

    elif file_type == "msg":
        bucket, key = split_path_bucket_key(
            config["paths"]["msg_api_raw"]["path"]
        )
        path = config["params"]["msg_json_response"]
        with open(path, "rb") as data:
            s3.upload_fileobj(data, bucket, key)
    else:
        print("Incorrect file type arg")
