from multiprocessing import JoinableQueue

import yaml
from common.SetupSpark import JobManager


def ReadConfigFile(yaml_file):
    with open("conf/spark_net.yaml") as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config


job = JobManager("dna_mbr_mfi", config_path="conf/spark_net.yaml")
print(job)
for analytics_query in job.config["analytics_queries"].keys():
    print(analytics_query)
