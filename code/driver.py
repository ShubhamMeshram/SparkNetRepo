from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import yaml
from common.api_util import *
from common.crypt import *
from common.job_manager import JobManager
from common.s3_util import *
from common.spark_util import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, explode_outer, split


def ReadConfigFile(yaml_file):
    with open("conf/spark_net.yaml") as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config


def main_fn(job):
    job.config = job.add_dates_to_paths(job.config)
    usr_url = job.config["params"]["usr_api_endpoint"]
    msg_url = job.config["params"]["msg_api_endpoint"]
    health_flag, usr_request, error_msg = PingAPI(usr_url)
    health_flag, msg_request, error_msg = PingAPI(msg_url)

    GenerateJSON(job.config["params"]["usr_json_response"], usr_request)
    GenerateJSON(job.config["params"]["msg_json_response"], msg_request)
    uploadlocaltoS3("user", job.config)
    uploadlocaltoS3("msg", job.config)

    usr_df = (
        job.spark.read.format("json")
        .option("multiLine", "true")
        .load(job.config["paths"]["usr_api_raw"]["path"])
    )

    msg_df = (
        job.spark.read.format("json")
        .option("multiLine", "true")
        .load(job.config["paths"]["msg_api_raw"]["path"])
    )

    usr_df = usr_df.withColumnRenamed("id", "userId")
    msg_df = msg_df.withColumnRenamed("id", "msgId")

    usr_df = job.ConvertStringToTimeStamp(usr_df, "createdAt")
    usr_df = job.ConvertStringToTimeStamp(usr_df, "updatedAt")
    msg_df = job.ConvertStringToTimeStamp(msg_df, "createdAt")

    usr_df = job.GetLatestSlimDataset("userId", "updatedAt", usr_df)

    # create user_sub and user_attr table
    user_attr_df = usr_df.select("userId", "profile.*")
    user_sub_df = usr_df.select(
        "userId", explode_outer("subscription")
    ).select("userId", "col.*")
    user_sub_df_slim = job.GetLatestSlimDataset(
        "userId", "startDate", user_sub_df
    )
    usr_df = usr_df.drop("profile")
    usr_df = usr_df.drop("subscription")
    # adding feature column -domain
    usr_df = usr_df.withColumn(
        "email_domain",
        split(split(col("email"), "@").getItem(1), ".com").getItem(0),
    )

    # temp table for qurying
    msg_df.registerTempTable("msg_df")
    usr_df.registerTempTable("usr_df")
    user_attr_df.registerTempTable("user_attr_df")
    user_sub_df.registerTempTable("user_sub_df")
    user_sub_df_slim.registerTempTable("user_sub_df_slim")

    # write all 4 tables to S3
    job.write(usr_df, "user", job.config)
    job.write(user_attr_df, "user_attributes", job.config)
    job.write(user_sub_df, "user_subscription", job.config)
    job.write(msg_df, "msg", job.config)
    GenerateAnalyticsOutput(job, job.config)
    return usr_df


def encryption_fn(usr_df, col_tuple):
    for column in col_tuple:
        usr_df = usr_df.withColumn(
            column + "_en", encrypt_message_udf(col(column))
        )
    temp_df = usr_df.drop(*col_tuple)
    return temp_df


def decryption_fn(usr_df, col_tuple):
    for name in usr_df.schema.names:
        usr_df = usr_df.withColumnRenamed(name, name.replace("_en", ""))
    temp_list = []
    col_list = list(col_tuple)
    for i in col_list:
        i = i.replace("_en", "")
        temp_list.append(i)
    col_tuple = tuple(col_list)
    print(col_tuple)
    # print(usr_df.printSchema())
    for column in col_tuple:
        print(column)
        usr_df = usr_df.withColumn(column, decrypt_message_udf(col(column)))
    temp_df = usr_df.drop(*col_tuple)
    return temp_df


job = JobManager("SparkNetApp", config_path="conf/spark_net.yaml")
usr_df = main_fn(job)
usr_df_en = encryption_fn(usr_df, ("firstName", "email"))
print(usr_df_en.printSchema())
usr_df_de = decryption_fn(usr_df_en, ("firstName_en", "email_en"))


usr_df.select("firstName", "email").show(500, False)
usr_df_en.select("firstName_en", "email_en").show(500, False)
usr_df_de.select("firstName_de", "email_de").show(500, False)

# job.sc.stop()
