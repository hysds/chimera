import json
import os
from smap_sciflo import post_processor

if __name__ == '__main__':
    """
    This is for testing of Production PGE Post Processing
    Comment out from hysds.celery import app in post_processor and query_util
    Setup 2 SSH tunnels:
    ssh -i [PEM file] -L 9200:localhost:9200 [username]@[MOZART_IP]
    ssh -i [PEM file] -L 9300:localhost:9200 [username]@[GRQ_IP]

    In post_processor, overwrite ES URLs with following:
    JOBS_ES_URL = "http://127.0.0.1:9200"
    GRQ_ES_URL = "http://127.0.0.1:9300"

    In commons/query_util.py, overwrite GRQ URL with:
    ES_URL = "http://127.0.0.1:9300"

    Update the following sample files:
    test-files/sf_context.json should be the sciflo context of an actual workflow run
    test-files/sample_job_submission_result.json with the result of a job submission corresponding
    If not testing for an L0B run please update:
    pge_type: Type of PGE
    pge_config_file: path to PGE's config file
    to the sciflo context above

    run this script
    """

    # Testing L0B post processing
    os.path.dirname(os.path.realpath(__file__))
    job_result = json.loads(open(os.path.dirname(os.path.realpath(__file__))+"/test-files/sample_job_submission_result.json").read())
    sf_context = os.path.dirname(os.path.realpath(__file__))+"/test-files/sf_context.json"
    pge_type = "L0A_Radiometer"
    level_up_dir = os.path.dirname(os.path.realpath(__file__))
    pge_config_file = os.path.abspath(os.path.join(os.path.realpath(__file__),"../..","configs/examples/PGE_L0A_RADIOMETER.json"))
    post_processor.create_context(sf_context, job_result, pge_type, pge_config_file, test_mode = True)