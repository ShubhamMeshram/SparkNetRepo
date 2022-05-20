import ast
import base64

import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name):
    """
    Fetches secret from AWS Secrets Manager

    Args:
        secret_name (str) - Name of the secret on AWS Secrets Manager

    Returns:
        actual_secret (str/byte) - The retrieved secret
    """
    secret_name = secret_name
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e.response["Error"]["Code"])
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "AccessDeniedException":
            # You dont have enough permissions
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            # print(get_secret_value_response)
            secret = get_secret_value_response["SecretString"]
            # print(secret)
            # print(type(secret))
            actual_secret = ast.literal_eval(secret).get(secret_name)
            if secret_name == "sparknet-crpyt-key":
                actual_secret = str.encode(actual_secret)
            return actual_secret
        else:
            print("inside inner else")
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )
            return decoded_binary_secret
