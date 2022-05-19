import numpy as np
import pandas as pd

from common.job_manager import JobManager


def write(self, df, table_name, config, mode="overwrite"):
    print(f"Starting write operation for {table_name} dataset")
    path = config["paths"][table_name]["path"]
    fmt = config["paths"][table_name]["format"]
    if fmt == "parquet":
        df.write.option("fs.s3a.committer.name", "partitioned").option(
            "fs.s3a.committer.staging.conflict-mode", "replace"
        ).option("fs.s3a.fast.upload.buffer", "bytebuffer").option(
            mode=mode
        ).parquet(
            path
        )

    elif fmt == "csv":
        df.write.csv(path, header=True, sep=",", mode=mode)
    else:
        print("Incorrect file format, kindly check the config file")


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
        "analytics_op",
        job.config,
    )
