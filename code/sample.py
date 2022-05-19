import json

# from pyspark.sql.functions import explode_outer
# from pyspark.sql.functions import split
# from pyspark.sql.functions import col
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import requests
import yaml
from common.api_util import *
from common.job_manager import JobManager
from common.s3_util import *
from common.spark_util import *
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import col, explode_outer, split
from pyspark.sql.types import DateType, TimestampType


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
    # uploadlocaltoS3("user", job.config)
    # uploadlocaltoS3("msg", job.config)

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

    # create user_sub and user_attr table
    user_attr_df = usr_df.select("userId", "profile.*")  # .show(500,False)
    user_sub_df = usr_df.select(
        "userId", explode_outer("subscription")
    ).select(
        "userId", "col.*"
    )  # .show(500,False)

    w = Window.partitionBy("userId")
    user_sub_df_slim = (
        user_sub_df.withColumn("maxB", f.max("startDate").over(w))
        .where(f.col("startDate") == f.col("maxB"))
        .drop("maxB")
    )

    usr_df = usr_df.drop("profile")
    usr_df = usr_df.drop("subscription")
    # adding feature column -domain
    usr_df = usr_df.withColumn(
        "domain", split(split(col("email"), "@").getItem(1), ".com").getItem(0)
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


job = JobManager("dna_mbr_mfi", config_path="conf/spark_net.yaml")
print(job)
for analytics_query in job.config["analytics_queries"].keys():
    print(analytics_query)

main_fn(job)
