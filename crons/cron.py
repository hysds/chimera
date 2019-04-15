import os
import logging
import json
from datetime import datetime
from hysds_commons.job_utils import submit_mozart_job
from commons import query_util

#this python code needs to query ES with job_id
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger("hysds")

BASE_PATH = os.path.dirname(__file__)

SCIFLO_QUEUE = ""
job_type = ""
version = ""

'''
This is an example cron job that submits HySDS jobs
'''


def find_input():
    """
    Find latest file in ES
    :return: id of file
    """

    return


def submit_sciflo(params, id):
    rule = {
        "rule_name": "submit_PGE",
        "queue": SCIFLO_QUEUE,
        "priority": '5',
        "kwargs": '{}'
    }

    print('submitting jobs with params:')
    print(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
    mozart_job_id = submit_mozart_job({}, rule, hysdsio={"id": "internal-temporary-wiring", "params": params,
                                                         "job-specification": "job-type:version"},
                                      job_name='job_%s-%s_%s' % ('PGE_NAME', "version", id),
                                      enable_dedup=False)

    LOGGER.info("Job ID: " + mozart_job_id)
    print("Job ID: " + mozart_job_id)
    return

def construct_params(prod_url):
    params = [
        # {
        #     "name": "product_paths",
        #     "from": "value",
        #     "value": prod_url
        # },
        # {
        #     "name": "purpose",
        #     "from": "value",
        #     "value": "PGE_NAME"
        # }
        ]
    print json.dumps(params)
    return params

def submit_job():
    product_url = []
    latest_input = find_input()
    for url in latest_input["_source"]["urls"]:
        if url.startswith("s3://"):
            product_url.append(url)
    params = construct_params(product_url)
    submit_sciflo(params, id=latest_input["_id"])
    return


if __name__ == '__main__':
    submit_job()
