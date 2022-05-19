import datetime
import os

import pyspark.sql.functions as sqlf
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession

#    job = JobManager("dna_mbr_mfi", config_path=args.config)


class JobManager(object):
    """
    This is a class to be used in all of the module to interact with
    SPARK and to perform read and writes.
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
        # super().__init__(config_path)
        if config_path:
            with open(config_path) as file:
                config_data = yaml.load(file, Loader=yaml.FullLoader)

            self.config = config_data
        else:
            self.config = {"paths": {}}

        #self.run_date = datetime.datetime.today()
        self.config_path = config_path

        self.app_name = app_name
        self.sc = SparkContext.getOrCreate()
        log4j_logger = self.sc._jvm.org.apache.log4j
        self.logger = log4j_logger.LogManager.getLogger(self.app_name)

        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()

        self.sc.setLogLevel(log_level)
