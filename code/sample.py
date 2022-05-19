import yaml
from common.api_util import *
from common.job_manager import JobManager
from common.s3_util import *


def ReadConfigFile(yaml_file):
    with open("conf/spark_net.yaml") as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config


def main_fn(job):
    usr_url = job.config["params"]["usr_api_endpoint"]
    msg_url = job.config["params"]["msg_api_endpoint"]
    health_flag, usr_request, error_msg = PingAPI(usr_url)
    health_flag, msg_request, error_msg = PingAPI(msg_url)

    GenerateJSON(job.config["params"]["usr_json_response"], usr_request)
    GenerateJSON(job.config["params"]["msg_json_response"], msg_request)
    uploadlocaltoS3("user", job.config)
    uploadlocaltoS3("msg", job.config)


job = JobManager("dna_mbr_mfi", config_path="conf/spark_net.yaml")
print(job)
for analytics_query in job.config["analytics_queries"].keys():
    print(analytics_query)

main_fn(job)
