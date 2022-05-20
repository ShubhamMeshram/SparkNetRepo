import numpy as np
import pandas as pd

from common.job_manager import JobManager


def qry_output(job, sql_qry):
    """
    Creates pandas df based on the query from the config file

    Args:
        job (JobManager)   - initialized Jobmanager object
        sql_qry (str)      - SQL query to be run on the dataset
    Returns:
        df(pandas df)      - Pandas df with output of query
    """
    df = job.spark.sql(sql_qry).toPandas().reset_index(drop=True)
    return df


def GenerateAnalyticsOutput(job, config):
    """
    Creates pandas df appended side by side based
    on the analytics queries from the config file. Writes the same to S3

    Args:
        job (JobManager)   - initialized Jobmanager object
        config (dict)    - config file which has all config params
    Returns:
    """
    appended_data = []
    for analytics_query in config["analytics_queries"].keys():
        sql_qry = job.config["analytics_queries"][analytics_query]["qry"]
        df_temp = qry_output(job, sql_qry)
        appended_data.append(df_temp)
    appended_data = pd.concat(appended_data, axis=1).replace(
        np.nan, "", regex=True
    )
    job.write(
        job.spark.createDataFrame(appended_data).coalesce(1),
        "analytics_op_recent",
        job.config,
    )


def GenerateDQOutput(job, config):
    """
    Creates pandas df appended side by side based
    on the DQ queries from the config file. Writes the same to S3

    Args:
        job (JobManager)   - initialized Jobmanager object
        config (dict)    - config file which has all config params
    Returns:
    """
    appended_data = []
    for analytics_query in config["dq_check_queries"].keys():
        sql_qry = job.config["dq_check_queries"][analytics_query]["qry"]
        df_temp = qry_output(job, sql_qry)
        appended_data.append(df_temp)
    appended_data = pd.concat(appended_data, axis=1).replace(
        np.nan, "", regex=True
    )
    job.write(
        job.spark.createDataFrame(appended_data).coalesce(1),
        "dq_op_recent",
        job.config,
    )
