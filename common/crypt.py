import cryptography
from cryptography.fernet import Fernet
from pyspark.sql.functions import col, lit, udf
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


def encryption_fn(spark_df, col_tuple):
    encrypt_message_udf = udf(lambda x: encrypt_message(x), StringType())
    for column in col_tuple:
        spark_df = spark_df.withColumn(
            column + "_en", encrypt_message_udf(col(column))
        )
        spark_df = spark_df.drop(column)
    return spark_df


def decryption_fn(spark_df, col_tuple):
    decrypt_message_udf = udf(lambda x: decrypt_message(x), StringType())
    for name in spark_df.schema.names:
        spark_df = spark_df.withColumnRenamed(name, name.replace("_en", ""))
    temp_list = []
    col_list = list(col_tuple)
    for i in col_list:
        i = i.replace("_en", "")
        temp_list.append(i)
    col_tuple = tuple(temp_list)
    for column in col_tuple:
        spark_df = spark_df.withColumn(
            column, decrypt_message_udf(col(column))
        )
    return spark_df


def MaskApproach2Method(spark_df, mask_col_list):
    for mask_col in mask_col_list:
        spark_df = spark_df.withColumn(mask_col, lit("***Masked***")).show(
            500, False
        )
        return spark_df
