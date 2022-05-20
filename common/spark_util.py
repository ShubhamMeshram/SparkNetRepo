import numpy as np
import pandas as pd

from common.job_manager import JobManager


def qry_output(job, analytics_qry_hdr):
    """
    Creates pandas df of values and dates based on query given in the config

    Args:
        job (JobManager)   - initialized Jobmanager object for DNA of interest
        query_base (str)   - SQL clause with dq check to be analyzed
    Returns:
        df (pandas dataframe)   - Pandas df with history of dq values
    """
    sql_qry = job.config["analytics_queries"][analytics_qry_hdr]["qry"]
    df = job.spark.sql(sql_qry).toPandas().reset_index(drop=True)
    return df


def GenerateAnalyticsOutput(job, config):
    appended_data = []
    for analytics_query in config["analytics_queries"].keys():
        df_temp = qry_output(job, analytics_query)
        appended_data.append(df_temp)
    appended_data = pd.concat(appended_data, axis=1).replace(
        np.nan, "", regex=True
    )
    job.write(
        job.spark.createDataFrame(appended_data).coalesce(1),
        "analytics_op_recent",
        job.config,
    )
