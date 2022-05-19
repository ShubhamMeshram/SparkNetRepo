import cryptography
from cryptography.fernet import Fernet
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType

from common.secrets_mgr import get_secret


def ConvertStringToTimeStamp(spark_df, ts_col):
    spark_df = spark_df.withColumn(
        "temp_ts_col", spark_df[ts_col].cast(TimestampType())
    )
    spark_df = spark_df.drop(ts_col)
    spark_df = spark_df.withColumnRenamed("temp_ts_col", ts_col)
    return spark_df


def encrypt_message(message):
    """
    Encrypts a message
    """

    key = get_secret("sparknet-crpyt-key")
    encoded_message = message.encode()
    f = Fernet(key)
    encrypted_message = f.encrypt(encoded_message)

    return encrypted_message


def decrypt_message(encrypted_message):
    """
    Decrypts an encrypted message
    """
    key = get_secret("sparknet-crpyt-key")

    f = Fernet(key)
    decrypted_message = f.decrypt(encrypted_message)

    return decrypted_message.decode()


encrypt_message_udf = udf(lambda x: encrypt_message(x), StringType())
decrypt_message_udf = udf(lambda x: decrypt_message(x), StringType())
