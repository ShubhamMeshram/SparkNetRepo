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


def process_user(job):
    """
    Method which performs data engineering on User
    API response and creates other tables based
    on certain logic which are uploaded to S3.

    Args:
        job (JobManager)   - initialized Jobmanager
    Returns:
        df (spark df)      - returns user dataframe to perform data protection
    """
    job.config = job.add_dates_to_paths(job.config)
    usr_url = job.config["params"]["usr_api_endpoint"]
    u_health_flag, usr_request, u_error_msg = PingAPI(usr_url)
    if u_health_flag == "Healthy":
        GenerateJSON(job.config["params"]["usr_json_response"], usr_request)
        uploadlocaltoS3("user", job.config)

        usr_df = (
            job.spark.read.format("json")
            .option("multiLine", "true")
            .load(job.config["paths"]["usr_api_raw"]["path"])
        )

        usr_df = usr_df.withColumnRenamed("id", "userId")

        usr_df = job.ConvertStringToTimeStamp(usr_df, "createdAt")
        usr_df = job.ConvertStringToTimeStamp(usr_df, "updatedAt")
        usr_df = job.ConvertStringToTimeStamp(usr_df, "birthDate")

        usr_df = job.ApplySCDOneMethod("userId", "updatedAt", usr_df)

        # create user_sub and user_attr table
        user_attr_df = usr_df.select("userId", "profile.*")
        user_sub_df = usr_df.select(
            "userId", explode_outer("subscription")
        ).select("userId", "col.*")
        user_sub_df_slim = job.ApplySCDOneMethod(
            "userId", "startDate", user_sub_df
        )
        usr_df = usr_df.drop("profile")
        usr_df = usr_df.drop("subscription")
        # adding feature column - email_domain
        usr_df = usr_df.withColumn(
            "email_domain",
            split(split(col("email"), "@").getItem(1), ".com").getItem(0),
        )

        # upload the generated dataframes to S3
        job.WriteToRecentAndArchive(usr_df, "user", job.config)
        job.WriteToRecentAndArchive(
            user_attr_df, "user_attributes", job.config
        )
        job.WriteToRecentAndArchive(
            user_sub_df, "user_subscription", job.config
        )

        usr_df.createOrReplaceTempView("usr_df")
        user_sub_df.createOrReplaceTempView("user_sub_df")
        user_sub_df_slim.createOrReplaceTempView("user_sub_df_slim")
        user_attr_df.createOrReplaceTempView("user_attr_df")

        # clean up for better performance
        # user_sub_df.unpersist()
        # user_attr_df.unpersist()
        # job.spark.catalog.dropTempView("usr_df")
        # job.spark.catalog.dropTempView("user_sub_df")
        # job.spark.catalog.dropTempView("user_sub_df_slim")
        return usr_df
    else:
        print(f"User API returning unhealthy response: {u_error_msg}")


def process_msg(job):
    """
    Method which performs data engineering on Message
    API response and creates other tables based
    on certain logic which are uploaded to S3.

    Args:
        job (JobManager)   - initialized Jobmanager object
    Returns:
        df (spark df)      - returns user dataframe to perform data protection
    """
    job.config = job.add_dates_to_paths(job.config)
    msg_url = job.config["params"]["msg_api_endpoint"]
    m_health_flag, msg_request, m_error_msg = PingAPI(msg_url)
    if m_health_flag == "Healthy":
        GenerateJSON(job.config["params"]["msg_json_response"], msg_request)
        uploadlocaltoS3("msg", job.config)
        msg_df = (
            job.spark.read.format("json")
            .option("multiLine", "true")
            .load(job.config["paths"]["msg_api_raw"]["path"])
        )
        msg_df = msg_df.withColumnRenamed("id", "msgId")
        msg_df = job.ConvertStringToTimeStamp(msg_df, "createdAt")
        job.WriteToRecentAndArchive(msg_df, "msg", job.config)

        msg_df.createOrReplaceTempView("msg_df")
        # job.spark.catalog.dropTempView("msg_df")
        return msg_df
    else:
        print(f"Message API returning unhealthy response: {m_error_msg}")


job = JobManager("SparkNetApp", config_path="conf/spark_net.yaml")
usr_df = process_user(job)
msg_df = process_msg(job)
GenerateAnalyticsOutput(job, job.config)
GenerateDQOutput(job, job.config)
usr_df_en = encryption_fn(usr_df, ("firstName", "lastName", "address"))
msg_df_en = encryption_fn(msg_df, ("message",))

job.WriteToRecentAndArchive(usr_df_en, "user_en", job.config)
job.WriteToRecentAndArchive(msg_df_en, "msg_en", job.config)

job.sc.stop()
print("Done")
