import cryptography
from cryptography.fernet import Fernet
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType


def load_key():
    """
    Load the previously generated key
    """
    return open("secret.key", "rb").read()


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
    # key = load_key()
    # print(key)

    key = b"SgC_PuQOhFINn8XkKhnWMOKtWTSl8RnUXchTbeCz1XS="

    encoded_message = message.encode()
    f = Fernet(key)
    encrypted_message = f.encrypt(encoded_message)

    # print(encrypted_message)
    return encrypted_message


def decrypt_message(encrypted_message):
    """
    Decrypts an encrypted message
    """
    # key = load_key()
    key = b"SgC_PuQOhFINn8XkKhnWMOKtWTSl8RnUXchTbeCz1XS="

    f = Fernet(key)
    decrypted_message = f.decrypt(encrypted_message)

    # print(decrypted_message.decode())
    return decrypted_message
