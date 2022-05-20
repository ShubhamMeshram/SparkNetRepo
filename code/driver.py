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
    # adding feature column - email_domain
    usr_df = usr_df.withColumn(
        "email_domain",
        split(split(col("email"), "@").getItem(1), ".com").getItem(0),
    )

    job.WriteToRecentAndArchive(usr_df, "user", job.config)
    job.WriteToRecentAndArchive(user_attr_df, "user_attributes", job.config)
    job.WriteToRecentAndArchive(user_sub_df, "user_subscription", job.config)
    job.WriteToRecentAndArchive(msg_df, "msg", job.config)
    """
    # adding timestamp columns
    usr_df = job.add_date_info(usr_df)
    user_attr_df = job.add_date_info(user_attr_df)
    user_sub_df = job.add_date_info(user_sub_df)
    msg_df = job.add_date_info(msg_df)

    # write to archive S3 location
    job.write(usr_df, "user_recent", job.config)
    job.write(user_attr_df, "user_attributes_recent", job.config)
    job.write(user_sub_df, "user_subscription_recent", job.config)
    job.write(msg_df, "msg_recent", job.config)

    # write to recent S3 location
    job.write(usr_df.drop("year", "month", "day"), "user_archive", job.config)
    job.write(
        user_attr_df.drop("year", "month", "day"),
        "user_attributes_archive",
        job.config,
    )
    job.write(
        user_sub_df.drop("year", "month", "day"),
        "user_subscription_archive",
        job.config,
    )
    job.write(msg_df.drop("year", "month", "day"), "msg_archive", job.config)
    """
    msg_df.createOrReplaceTempView("msg_df")
    usr_df.createOrReplaceTempView("usr_df")
    user_sub_df.createOrReplaceTempView("user_sub_df")
    user_sub_df_slim.createOrReplaceTempView("user_sub_df_slim")
    GenerateAnalyticsOutput(job, job.config)

    # clean up for better performance
    user_sub_df.unpersist()
    user_attr_df.unpersist()
    job.spark.catalog.dropTempView("msg_df")
    job.spark.catalog.dropTempView("usr_df")
    job.spark.catalog.dropTempView("user_sub_df")
    job.spark.catalog.dropTempView("user_sub_df_slim")

    return usr_df


job = JobManager("SparkNetApp", config_path="conf/spark_net.yaml")
usr_df = main_fn(job)
usr_df = usr_df.persist()
usr_df_en = encryption_fn(usr_df, ("firstName",))
usr_df_en = job.add_date_info(usr_df_en)
job.write(usr_df_en, "usr_df_en_archive", job.config)
job.write(
    usr_df_en.drop("year", "month", "day"), "usr_df_en_archive", job.config
)
usr_df.unpersist()
job.sc.stop()
print("Done")
