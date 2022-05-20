import datetime
import os

import pyspark.sql.functions as sqlf
import pyspark.sql.functions as f
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import TimestampType

from common.secrets_mgr import get_secret


class JobManager(object):
    """
    This is a class to be used in all of the module to interact with
    SPARK and to perform various operations
    """

    def __init__(self, app_name, config_path=None, log_level="WARN"):
        """
        Set up spark session, spark context and the class member variables.

        Args:
            app_name (str) - name of the SPARK application to be started
            config_path (str) - path pointing to the config file
                containing paths and parameters
            log_level (str) - logging level to be used - INFO, WARN, DEBUG
        """
        if config_path:
            with open(config_path) as file:
                config_data = yaml.load(file, Loader=yaml.FullLoader)

            self.config = config_data
        else:
            self.config = {"paths": {}}

        self.config_path = config_path

        self.app_name = app_name
        self.sc = SparkContext.getOrCreate()
        log4j_logger = self.sc._jvm.org.apache.log4j
        self.logger = log4j_logger.LogManager.getLogger(self.app_name)

        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.spark.conf.set("fs.s3a.access.key", get_secret("admin-ak"))
        self.spark.conf.set("fs.s3a.secret.key", get_secret("admin-sak"))

        self.sc.setLogLevel(log_level)
        print(f"Started Spark application {self.app_name}")

    def GetLatestSlimDataset(self, partitionByCol, ColforSlimming, spark_df):
        """
        Fetches the latest records (if duplicated/updated)

        Args:
            partitionByCol (str) - column based on which the paritioning
                                   needs to happen to get the latest record.
            ColforSlimming (str) - column on which the window is applied
            spark_df (spark df)  - dataframe on which the slimming happens

        Returns:
            spark_df (spark df)  - dataframe which is reduced to ensure latest
                                   records are present for the partitionByCol
                                   based on the recency of ColforSlimming
        """
        w = Window.partitionBy(partitionByCol)
        temp_df = (
            spark_df.withColumn("temp_col", f.max(ColforSlimming).over(w))
            .where(f.col(ColforSlimming) == f.col("temp_col"))
            .drop("temp_col")
        )
        return temp_df

    def write(self, df, table_name, config, mode="overwrite"):
        """
        Write a table into file

        Args:
            df (SPARK DataFrame) - dataframe to be written
            table_name (str) - name of the table to be written
            config (dict)    - config file which has all config params
            mode (str) - writting mode to be passed to write function

        Returns:
        """
        print(f"Starting write operation for {table_name} dataset\n")
        path = config["paths"][table_name]["path"]
        fmt = config["paths"][table_name]["format"]
        if fmt == "parquet":
            df.write.option("fs.s3a.committer.name", "partitioned").option(
                "fs.s3a.committer.magic.enabled", "false"
            ).option(
                "fs.s3a.committer.staging.conflict-mode", "append"
            ).option(
                "fs.s3a.committer.staging.unique-filenames", "true"
            ).option(
                "fs.s3a.committer.staging.abort.pending.uploads", "true"
            ).option(
                "fs.s3a.fast.upload.buffer", "bytebuffer"
            ).parquet(
                path, mode=mode
            )
        elif fmt == "csv":
            df.write.csv(path, header=True, sep=",", mode=mode)
        else:
            print("Incorrect file format, kindly check the config file")

    def get_run_date_str(self, fmt="%Y%m%d"):
        """
        Return the current run date either supplied from
        the config or obtained from self.run_date

        Args:
            fmt (str) - format of datetime to be returns

        Returns:
            (str) - current run date
        """
        run_date = datetime.datetime.today()
        date_fmt = run_date.strftime(fmt)

        return date_fmt

    def add_date_info(self, df):
        """
        Adds the insert_date, DAY, MONTH and YEAR columns to a dataframe

        Args:
            df (Dataframe) - datafarame that the columns are to be added onto

        Returns:
            (Dataframe) - dataframe with additional columns
        """
        date_str = self.get_run_date_str("%Y%m%d")

        df_out = (
            df.withColumn("insert_date", sqlf.lit(date_str))
            .withColumn("day", sqlf.lit(date_str[6:8]))
            .withColumn("month", sqlf.lit(date_str[4:6]))
            .withColumn("year", sqlf.lit(date_str[0:4]))
        )

        return df_out

    def add_dates_to_paths(self, config):
        """
        Modifies the paths inside of config file replacing the
        year and month placeholders with actual dates and months
        obtained from the current datetime
        """

        date_str = self.get_run_date_str("%Y%m%d")

        format_dict = {
            "str_day": date_str[6:8],
            "str_month": date_str[4:6],
            "str_year": date_str[0:4],
        }
        for table_name, data_source in self.config["paths"].items():
            self.config["paths"][table_name]["path"] = data_source[
                "path"
            ].format(**format_dict)
        return config

    def ConvertStringToTimeStamp(self, spark_df, ts_col):
        """
        Casts the string representation of time into timestamp data type

        Args:
            spark_df (Dataframe) - datafarame that contains string timestamp
            ts_col (str)         - columns which needs to be converted
        Returns:
            (Dataframe) - dataframe with correct timestamp data type
        """
        spark_df = spark_df.withColumn(
            "temp_ts_col", spark_df[ts_col].cast(TimestampType())
        )
        spark_df = spark_df.drop(ts_col)
        spark_df = spark_df.withColumnRenamed("temp_ts_col", ts_col)
        return spark_df

    def WriteToRecentAndArchive(self, spark_df, path_name, config):
        """
        Upload dataset to recent and archive S3 locations.
        This method adds housekeeping columns to the archive dataset
        and removes them from the recent dataset and then uploads to
        respective S3 locations fetched from the config file

        Args:
            spark_df (Dataframe) - datafarame that needs to be uploaded
            path_name (str)      - path to which recent and archive keyword
                                   is appended and then parsed in config
            config (dict)        - config file which has all config params

        Returns:
        """
        spark_df = self.add_date_info(spark_df)
        self.write(spark_df, path_name + "_recent", config)  # write to recent
        self.write(
            spark_df.drop("year", "month", "day"),
            path_name + "_archive",
            config,
        )  # write to archive
        print(
            f"Completed writing to recent and archive S3 location for *{path_name}* dataset\n"
        )
